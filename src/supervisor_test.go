package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Integration test: needs s3fs, /dev/fuse, root and an S3 endpoint
// (e.g. moto) in S3FS_TEST_ENDPOINT with bucket "testbkt" holding hello.txt.
func TestSupervisorSurvivesKill(t *testing.T) {
	ep := os.Getenv("S3FS_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("S3FS_TEST_ENDPOINT not set")
	}
	mp := filepath.Join(t.TempDir(), "mnt")
	env := append(os.Environ(), "AWS_ACCESS_KEY_ID=a", "AWS_SECRET_ACCESS_KEY=b")
	args := []string{mp, "-o", "bucket=testbkt,url=" + ep + ",use_path_request_style"}

	if err := startS3fs(mp, args, env); err != nil {
		t.Fatal(err)
	}
	read := func() string {
		b, err := os.ReadFile(mp + "/hello.txt")
		if err != nil {
			return "ERR:" + err.Error()
		}
		return string(b)
	}
	if got := read(); got != "hi" {
		t.Fatalf("initial read: %q", got)
	}
	t.Logf("initial read ok: %q", read())

	procsMu.Lock()
	pid := procs[mp].cmd.Process.Pid
	procsMu.Unlock()
	syscall.Kill(pid, syscall.SIGKILL)
	t.Logf("killed s3fs pid %d with SIGKILL", pid)
	time.Sleep(200 * time.Millisecond)
	_, err := os.Stat(mp)
	t.Logf("stat right after kill: %v", err)

	deadline := time.Now().Add(20 * time.Second)
	for read() != "hi" {
		if time.Now().After(deadline) {
			t.Fatalf("did not recover: %s", read())
		}
		time.Sleep(200 * time.Millisecond)
	}
	procsMu.Lock()
	newpid := procs[mp].cmd.Process.Pid
	procsMu.Unlock()
	t.Logf("recovered: read %q via new s3fs pid %d", read(), newpid)
	if newpid == pid {
		t.Fatal("pid unchanged")
	}

	// kill again and stop while dead: unmount must still succeed
	syscall.Kill(newpid, syscall.SIGKILL)
	time.Sleep(50 * time.Millisecond)
	if err := stopS3fs(mp); err != nil {
		t.Fatalf("stop on dead mount: %v", err)
	}
	if isMounted(mp) {
		t.Fatal("still mounted")
	}
	if _, err := os.Stat(mp); !os.IsNotExist(err) {
		t.Fatalf("mount dir not removed: %v", err)
	}
	time.Sleep(3 * time.Second) // supervisor must not resurrect it
	if isMounted(mp) {
		t.Fatal("supervisor restarted s3fs after stop")
	}
	out, _ := exec.Command("pgrep", "-fc", "s3fs.*"+strings.TrimPrefix(mp, "/")).Output()
	t.Logf("s3fs processes for mount after stop: %s", strings.TrimSpace(string(out)))
}
