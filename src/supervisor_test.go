package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Integration test. Needs root, /dev/fuse, an s3fs built with
// s3fs-devfd.patch (run it inside the plugin image) and an S3 endpoint:
//
//	S3FS_TEST_ENDPOINT  e.g. http://127.0.0.1:8333
//	S3FS_TEST_BUCKET    bucket holding hello.txt containing "hi" (default testbkt)
//	S3FS_TEST_KEY / S3FS_TEST_SECRET  credentials (default a / b)
func TestSupervisorSurvivesKill(t *testing.T) {
	ep := os.Getenv("S3FS_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("S3FS_TEST_ENDPOINT not set")
	}
	bucket := envOr("S3FS_TEST_BUCKET", "testbkt")

	// Like the plugin's propagated mount: a shared parent for the mount point.
	base := t.TempDir()
	must(t, syscall.Mount(base, base, "", syscall.MS_BIND, ""))
	must(t, syscall.Mount("", base, "", syscall.MS_SHARED, ""))
	defer syscall.Unmount(base, syscall.MNT_DETACH)

	mp := filepath.Join(base, "mnt")
	env := append(os.Environ(), "AWS_ACCESS_KEY_ID="+envOr("S3FS_TEST_KEY", "a"), "AWS_SECRET_ACCESS_KEY="+envOr("S3FS_TEST_SECRET", "b"))
	args := []string{mp, "-o", "allow_other,use_path_request_style,ensure_diskfree=0,url=" + ep + ",bucket=" + bucket}
	if err := startS3fs(mp, args, env); err != nil {
		t.Fatal(err)
	}

	// Like a container's view of the volume: a slave bind copy of the mount.
	view := filepath.Join(t.TempDir(), "container-data")
	must(t, os.Mkdir(view, 0755))
	must(t, syscall.Mount(mp, view, "", syscall.MS_BIND, ""))
	must(t, syscall.Mount("", view, "", syscall.MS_SLAVE, ""))

	read := func(dir string) string {
		b, err := os.ReadFile(dir + "/hello.txt")
		if err != nil {
			return "ERR:" + err.Error()
		}
		return string(b)
	}
	if got := read(view); got != "hi" {
		t.Fatalf("initial read through container view: %q", got)
	}
	t.Logf("initial read through container view: %q", read(view))

	for round := 1; round <= 3; round++ {
		pid := s3fsPid(t, mp)
		must(t, syscall.Kill(pid, syscall.SIGKILL))
		t.Logf("round %d: killed s3fs pid %d", round, pid)
		time.Sleep(300 * time.Millisecond)
		t.Logf("round %d: container view right after kill: %s", round, read(view))

		deadline := time.Now().Add(20 * time.Second)
		for read(view) != "hi" {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: container view did not recover: %s", round, read(view))
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Logf("round %d: container view recovered: %q via s3fs pid %d", round, read(view), s3fsPid(t, mp))
	}

	// A second Mount request for the same ID (docker cp does this) and its
	// Unmount must leave the running container's mount alone and recoverable.
	must(t, startS3fs(mp, args, env))
	must(t, stopS3fs(mp))
	if got := read(view); got != "hi" || !isMounted(mp) {
		t.Fatalf("after a second mount/unmount pair: view %q, mounted %v", got, isMounted(mp))
	}
	must(t, syscall.Kill(s3fsPid(t, mp), syscall.SIGKILL))
	deadline := time.Now().Add(20 * time.Second)
	for read(view) != "hi" {
		if time.Now().After(deadline) {
			t.Fatalf("container view did not recover after a mount/unmount pair: %s", read(view))
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Log("second mount/unmount pair for the same ID: container view kept working and still recovers")

	// The kernel aborts the FUSE connection (e.g. a request exceeded
	// fs.fuse.*_request_timeout) while s3fs keeps running: it is paused here so
	// it cannot notice and exit. The supervisor must detect the abort itself.
	if _, err := os.Stat("/sys/fs/fuse/connections"); err == nil {
		syscall.Mount("fusectl", "/sys/fs/fuse/connections", "fusectl", 0, "")
	}
	pid := s3fsPid(t, mp)
	must(t, syscall.Kill(pid, syscall.SIGSTOP))
	must(t, os.WriteFile("/sys/fs/fuse/connections/"+fuseConn(t, mp)+"/abort", []byte("1"), 0200))
	t.Logf("aborted the FUSE connection while s3fs pid %d is still alive; view: %s", pid, read(view))
	deadline = time.Now().Add(30 * time.Second)
	for read(view) != "hi" {
		if time.Now().After(deadline) {
			t.Fatalf("container view did not recover after a connection abort: %s", read(view))
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("connection abort with s3fs alive: container view recovered via s3fs pid %d", s3fsPid(t, mp))

	// Writes through the recovered view must work too.
	must(t, os.WriteFile(view+"/after-recovery.txt", []byte("ok"), 0644))
	if b, err := os.ReadFile(mp + "/after-recovery.txt"); err != nil || string(b) != "ok" {
		t.Fatalf("write through recovered view: %q %v", b, err)
	}
	os.Remove(view + "/after-recovery.txt")

	// Docker unmounts the container's copy before asking the plugin to unmount.
	must(t, unmountLayers(view))
	if err := stopS3fs(mp); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if isMounted(mp) {
		t.Fatal("still mounted after stop")
	}
	if _, err := os.Stat(mp); !os.IsNotExist(err) {
		t.Fatalf("mount dir not removed: %v", err)
	}
	time.Sleep(3 * time.Second) // the supervisor must not bring it back
	if isMounted(mp) {
		t.Fatal("supervisor restarted s3fs after stop")
	}
	if n := countS3fs(); n != 0 {
		t.Fatalf("%d s3fs processes left after stop", n)
	}
	t.Log("stopped cleanly: no mount, no directory, no s3fs process left")
}

func unmountLayers(dir string) error {
	for i := 0; i < maxMountLayers && isMounted(dir); i++ {
		if err := syscall.Unmount(dir, syscall.MNT_DETACH); err != nil {
			return err
		}
	}
	return nil
}

func s3fsPid(t *testing.T, mp string) int {
	procsMu.Lock()
	defer procsMu.Unlock()
	s := procs[mp]
	if s == nil {
		t.Fatal("no supervisor for " + mp)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cmd.Process.Pid
}

func countS3fs() int {
	n := 0
	dirs, _ := filepath.Glob("/proc/[0-9]*/cmdline")
	for _, f := range dirs {
		b, err := os.ReadFile(f)
		if err != nil || len(b) == 0 {
			continue
		}
		if filepath.Base(strings.SplitN(string(b), "\x00", 2)[0]) == "s3fs" {
			n++
		}
	}
	return n
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// fuseConn returns the FUSE connection number (device minor) of the top mount on mp.
func fuseConn(t *testing.T, mp string) string {
	b, err := os.ReadFile("/proc/self/mountinfo")
	must(t, err)
	conn := ""
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 4 && f[4] == mp {
			conn = strings.SplitN(f[2], ":", 2)[1] // last one listed is the top
		}
	}
	if conn == "" {
		t.Fatal("no mount on " + mp)
	}
	return conn
}
