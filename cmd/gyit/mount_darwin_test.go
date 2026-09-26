package main

import "testing"

func TestNativeMountKeepsOptionsAndPathsSeparate(t *testing.T) {
	args := nativeMountArgs("/tmp/mount with spaces", 4096)
	if len(args) != 7 || args[0] != "-F" || args[2] != "gyit" || args[5] != "https://github.com" || args[6] != "/tmp/mount with spaces" {
		t.Fatalf("args %q", args)
	}
}
