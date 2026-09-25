//go:build linux || darwin

package controlcli

import (
	"golang.org/x/sys/unix"
	"io"
	"os"
)

func terminalOutput(out io.Writer) bool {
	file, ok := out.(*os.File)
	if !ok {
		return false
	}
	_, err := unix.IoctlGetWinsize(int(file.Fd()), unix.TIOCGWINSZ)
	return err == nil
}
