//go:build !linux && !darwin

package controlcli

import "io"

func terminalOutput(io.Writer) bool { return false }
