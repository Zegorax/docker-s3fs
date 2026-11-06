package main

import (
	"bufio"
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
	mountTimeout   = 30 * time.Second
	maxRestartWait = 30 * time.Second
)

// s3fsProc supervises one foreground s3fs process. If s3fs dies unexpectedly
// the reason is logged, the dead mount is detached and s3fs is started again.
type s3fsProc struct {
	mountPoint string
	args       []string
	env        []string

	mu      sync.Mutex
	cmd     *exec.Cmd
	stopped bool
	exited  chan struct{} // closed when the supervisor goroutine returns
}

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

// cleanStale detaches a leftover mount whose FUSE daemon is gone
// ("transport endpoint is not connected").
func cleanStale(mountPoint string) {
	if _, err := os.Stat(mountPoint); !isENOTCONN(err) {
		return
	}
	log.Printf("detaching stale mount %s", mountPoint)
	syscall.Unmount(mountPoint, syscall.MNT_DETACH)
}

func isENOTCONN(err error) bool {
	if pe, ok := err.(*os.PathError); ok {
		err = pe.Err
	}
	return err == syscall.ENOTCONN
}

// launch starts s3fs in the foreground and waits until the mount is live.
func (s *s3fsProc) launch() (chan error, error) {
	if err := os.MkdirAll(s.mountPoint, 0755); err != nil {
		return nil, err
	}
	cleanStale(s.mountPoint)

	cmd := exec.Command("s3fs", append(append([]string{}, s.args...), "-f")...)
	cmd.Env = s.env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	deadline := time.After(mountTimeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			return nil, fmt.Errorf("s3fs exited during mount: %s", describeExit(err))
		case <-deadline:
			cmd.Process.Kill()
			<-done
			return nil, fmt.Errorf("timed out waiting for s3fs to mount %s", s.mountPoint)
		case <-tick.C:
			if isMounted(s.mountPoint) {
				s.mu.Lock()
				s.cmd = cmd
				s.mu.Unlock()
				return done, nil
			}
		}
	}
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

func (s *s3fsProc) supervise(done chan error) {
	defer close(s.exited)
	wait := time.Second
	for {
		err := <-done
		if s.isStopped() {
			return
		}
		log.Printf("s3fs for %s died unexpectedly: %s; restarting", s.mountPoint, describeExit(err))
		for {
			time.Sleep(wait)
			if s.isStopped() {
				return
			}
			newDone, err := s.launch()
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
func startS3fs(mountPoint string, args, env []string) error {
	s := &s3fsProc{mountPoint: mountPoint, args: args, env: env, exited: make(chan struct{})}
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

// stopS3fs unmounts mountPoint and stops its supervisor. It tolerates mounts
// that are already gone or dead.
func stopS3fs(mountPoint string) error {
	procsMu.Lock()
	s := procs[mountPoint]
	procsMu.Unlock()

	if s != nil {
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()
	}

	err := syscall.Unmount(mountPoint, 0)
	if err != nil && err != syscall.EINVAL && err != syscall.ENOENT {
		// busy or dead mount: fall back to a lazy detach
		err = syscall.Unmount(mountPoint, syscall.MNT_DETACH)
	}
	if err != nil && err != syscall.EINVAL && err != syscall.ENOENT {
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
