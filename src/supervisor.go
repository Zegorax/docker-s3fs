package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	startGrace     = 10 * time.Second
	maxRestartWait = 30 * time.Second
	maxMountLayers = 64

	// How often the supervisor checks that the kernel has not aborted the FUSE
	// connection of a running s3fs, and how long one check may take before it
	// is treated as "busy" rather than "aborted".
	probeInterval = 5 * time.Second
	probeTimeout  = 3 * time.Second
)

// s3fsProc supervises one foreground s3fs process. If s3fs dies unexpectedly
// the reason is logged and a fresh s3fs is mounted on top of the dead mount.
//
// Why on top: a running container does not see the plugin's mount directly.
// Its view of the volume is a slave copy of the original s3fs mount, so the
// only mount events that reach it are mounts made on top of that mount. A new
// mount beside it (after detaching the dead one) never reaches the container,
// which then keeps failing with "transport endpoint is not connected".
type s3fsProc struct {
	mountPoint string
	args       []string // s3fs arguments, including the mount point
	env        []string

	launchMu sync.Mutex // held by launch and stopS3fs: a stop never overlaps a (re)mount
	mu       sync.Mutex
	cmd      *exec.Cmd
	started  time.Time
	stopped  bool
	refs     int           // Mount requests not yet matched by an Unmount (guarded by procsMu)
	exited   chan struct{} // closed when the supervisor goroutine returns
}

var errStopped = errors.New("unmount requested")

var (
	procsMu sync.Mutex
	procs   = map[string]*s3fsProc{}
)

// isMounted reports whether mountPoint is listed in the mount table.
func isMounted(mountPoint string) bool {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) > 4 && fields[4] == mountPoint {
			return true
		}
	}
	return false
}

func isENOTCONN(err error) bool {
	if pe, ok := err.(*os.PathError); ok {
		err = pe.Err
	}
	return err == syscall.ENOTCONN
}

// kernelMountOptions translates the s3fs "-o" options that the kernel (and not
// s3fs) handles into mount flags and FUSE mount data, the way libfuse would.
func kernelMountOptions(args []string) (uintptr, string) {
	flags := uintptr(syscall.MS_NOSUID | syscall.MS_NODEV)
	data := ""
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "-o" {
			continue
		}
		for _, o := range strings.Split(args[i+1], ",") {
			switch {
			case o == "ro":
				flags |= syscall.MS_RDONLY
			case o == "suid":
				flags &^= syscall.MS_NOSUID
			case o == "dev":
				flags &^= syscall.MS_NODEV
			case o == "noexec":
				flags |= syscall.MS_NOEXEC
			case o == "allow_other", o == "default_permissions", strings.HasPrefix(o, "max_read="):
				data += "," + o
			}
		}
	}
	return flags, data
}

// launch mounts a new FUSE connection on the mount point (on top of whatever
// is there, dead or not) and starts s3fs serving it.
//
// The plugin does the mount syscall itself and hands the /dev/fuse fd to s3fs
// as /dev/fd/3, because s3fs and libfuse stat() a mount point given by path,
// which fails with ENOTCONN on a dead mount (see s3fs-devfd.patch).
func (s *s3fsProc) launch() (chan error, error) {
	s.launchMu.Lock()
	defer s.launchMu.Unlock()
	if s.isStopped() {
		return nil, errStopped
	}
	if err := os.MkdirAll(s.mountPoint, 0755); err != nil {
		// a dead mount is fine: we mount on top of it
		if _, serr := os.Lstat(s.mountPoint); !isENOTCONN(serr) {
			return nil, err
		}
	}

	fuse, err := os.OpenFile("/dev/fuse", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	flags, extra := kernelMountOptions(s.args)
	data := fmt.Sprintf("fd=%d,rootmode=40000,user_id=0,group_id=0%s", fuse.Fd(), extra)
	if err := syscall.Mount("s3fs", s.mountPoint, "fuse.s3fs", flags, data); err != nil {
		fuse.Close()
		return nil, fmt.Errorf("mounting fuse on %s: %w", s.mountPoint, err)
	}
	// From here on, a failure must remove the mount we just added (the top one).
	undo := func() { syscall.Unmount(s.mountPoint, syscall.MNT_DETACH) }

	args := make([]string, 0, len(s.args)+1)
	for _, a := range s.args {
		if a == s.mountPoint {
			a = "/dev/fd/3" // first entry of cmd.ExtraFiles
		}
		args = append(args, a)
	}
	cmd := exec.Command("s3fs", append(args, "-f")...)
	cmd.Env = s.env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{fuse}
	err = cmd.Start()
	// s3fs must hold the only fd to the connection: then the kernel aborts it
	// as soon as s3fs dies, instead of leaving requests hanging.
	fuse.Close()
	if err != nil {
		undo()
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	// The kernel queues the container's requests until s3fs answers them, so
	// once s3fs is running the mount is usable. Only an early exit is a
	// failure: the first answer can take tens of seconds on a slow backend or
	// when a busy container floods the fresh mount, and killing s3fs for that
	// would only restart the cycle.
	start := time.Now()
	ready := make(chan error, 1)
	go func() { _, err := os.Stat(s.mountPoint); ready <- err }()
	select {
	case err := <-done:
		undo()
		return nil, fmt.Errorf("s3fs exited during mount: %s", describeExit(err))
	case err := <-ready:
		if err != nil {
			// usually s3fs exiting at startup (bad options, credentials, disk space)
			select {
			case exitErr := <-done:
				undo()
				return nil, fmt.Errorf("s3fs exited during mount: %s", describeExit(exitErr))
			case <-time.After(2 * time.Second):
				log.Printf("first request on %s failed: %s", s.mountPoint, err)
			}
		}
	case <-time.After(startGrace):
		log.Printf("s3fs for %s has not answered after %s; leaving it running", s.mountPoint, startGrace)
		go func() {
			err := <-ready
			log.Printf("s3fs for %s answered after %s (err: %v)", s.mountPoint, time.Since(start).Round(time.Second), err)
		}()
	}
	s.mu.Lock()
	s.cmd = cmd
	s.started = time.Now()
	s.mu.Unlock()
	return done, nil
}

func describeExit(err error) string {
	if err == nil {
		return "exit status 0"
	}
	if ee, ok := err.(*exec.ExitError); ok {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return fmt.Sprintf("killed by signal %s (core dumped: %v)", ws.Signal(), ws.CoreDump())
		}
	}
	return err.Error()
}

func (s *s3fsProc) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// connectionAborted reports whether the kernel has aborted the FUSE
// connection behind the mount point. That happens for example when a request
// exceeds the kernel's FUSE request timeout (fs.fuse.*_request_timeout): every
// access then fails with ENOTCONN, but s3fs itself can keep running for
// minutes (it notices only when it next reads from /dev/fuse), so waiting for
// it to exit would leave the containers broken all that time.
//
// statfs is always sent to s3fs, which answers it without contacting S3. A
// probe that does not answer in time means s3fs is busy, not aborted; it is
// kept and collected on a later call, so at most one probe is in flight.
func (s *s3fsProc) connectionAborted(probe *chan error) bool {
	if *probe == nil {
		ch := make(chan error, 1)
		*probe = ch
		go func() {
			var st syscall.Statfs_t
			ch <- syscall.Statfs(s.mountPoint, &st)
		}()
	}
	select {
	case err := <-*probe:
		*probe = nil
		return err == syscall.ENOTCONN || err == syscall.ECONNABORTED
	case <-time.After(probeTimeout):
		return false
	}
}

func (s *s3fsProc) supervise(done chan error) {
	defer close(s.exited)
	wait := time.Second
	tick := time.NewTicker(probeInterval)
	defer tick.Stop()
	for {
		var err error
		var probe chan error
		aborted := false
	running:
		for {
			select {
			case err = <-done:
				break running
			case <-tick.C:
				if aborted || s.isStopped() || !s.connectionAborted(&probe) {
					continue
				}
				aborted = true
				s.mu.Lock()
				log.Printf("FUSE connection of %s was aborted by the kernel while s3fs (pid %d) is still running; restarting s3fs",
					s.mountPoint, s.cmd.Process.Pid)
				s.cmd.Process.Kill()
				s.mu.Unlock()
			}
		}
		if s.isStopped() {
			return
		}
		s.mu.Lock()
		uptime := time.Since(s.started).Round(time.Second)
		s.mu.Unlock()
		cause := "no unmount was requested"
		if aborted {
			cause = "killed by the supervisor after its FUSE connection was aborted"
		}
		log.Printf("s3fs for %s died unexpectedly after %s: %s (%s; still in mount table: %v); remounting on top",
			s.mountPoint, uptime, describeExit(err), cause, isMounted(s.mountPoint))
		for {
			time.Sleep(wait)
			if s.isStopped() {
				return
			}
			newDone, err := s.launch()
			if err == errStopped {
				return
			}
			if err == nil {
				done, wait = newDone, time.Second
				log.Printf("s3fs for %s restarted", s.mountPoint)
				break
			}
			log.Printf("restart of s3fs for %s failed: %s", s.mountPoint, err)
			if wait *= 2; wait > maxRestartWait {
				wait = maxRestartWait
			}
		}
	}
}

// startS3fs mounts and begins supervising s3fs for mountPoint.
//
// Docker can send several Mount requests for the same ID while a container
// runs (docker cp, for example, mounts the container's volumes again). They
// share the existing mount: mounting another s3fs on top and tearing all
// layers down at the matching Unmount would leave the running container on an
// s3fs the plugin no longer tracks, which could then never be recovered.
func startS3fs(mountPoint string, args, env []string) error {
	procsMu.Lock()
	if s := procs[mountPoint]; s != nil {
		s.refs++
		n := s.refs
		procsMu.Unlock()
		log.Printf("%s is already mounted; sharing it (%d users)", mountPoint, n)
		return nil
	}
	procsMu.Unlock()

	s := &s3fsProc{mountPoint: mountPoint, args: args, env: env, exited: make(chan struct{}), refs: 1}
	done, err := s.launch()
	if err != nil {
		return err
	}
	procsMu.Lock()
	procs[mountPoint] = s
	procsMu.Unlock()
	go s.supervise(done)
	return nil
}

// unmountAll removes every mount stacked on mountPoint (the live one and any
// dead ones below it left by earlier crashes).
func unmountAll(mountPoint string) error {
	for i := 0; i < maxMountLayers && isMounted(mountPoint); i++ {
		if err := syscall.Unmount(mountPoint, 0); err != nil && err != syscall.EINVAL && err != syscall.ENOENT {
			// busy or dead mount: fall back to a lazy detach
			if err := syscall.Unmount(mountPoint, syscall.MNT_DETACH); err != nil && err != syscall.EINVAL && err != syscall.ENOENT {
				return err
			}
		}
	}
	return nil
}

// stopS3fs unmounts mountPoint and stops its supervisor. It tolerates mounts
// that are already gone or dead.
func stopS3fs(mountPoint string) error {
	procsMu.Lock()
	s := procs[mountPoint]
	if s != nil && s.refs > 1 {
		s.refs--
		n := s.refs
		procsMu.Unlock()
		log.Printf("%s is still used by %d mount request(s); keeping it", mountPoint, n)
		return nil
	}
	procsMu.Unlock()

	if s != nil {
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()
		// wait for an in-flight remount to finish, so it cannot add a layer
		// after unmountAll has run
		s.launchMu.Lock()
	}
	err := unmountAll(mountPoint)
	if s != nil {
		s.launchMu.Unlock()
	}
	if err != nil {
		if s != nil { // still mounted, keep supervising
			s.mu.Lock()
			s.stopped = false
			s.mu.Unlock()
		}
		return err
	}

	if s != nil {
		select {
		case <-s.exited:
		case <-time.After(10 * time.Second):
			s.mu.Lock()
			if s.cmd != nil && s.cmd.Process != nil {
				s.cmd.Process.Kill()
			}
			s.mu.Unlock()
		}
		procsMu.Lock()
		delete(procs, mountPoint)
		procsMu.Unlock()
	}
	if err := os.Remove(mountPoint); err != nil && !os.IsNotExist(err) {
		log.Printf("could not remove %s: %s", mountPoint, err)
	}
	return nil
}
