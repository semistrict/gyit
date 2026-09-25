package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

// Each child runs an independent Local instance. The pipe releases all writers
// after process creation, exercising file locks rather than a shared Go mutex.
func TestLocalCASProcesses(t *testing.T) {
	if os.Getenv("GAT_CAS_CHILD") == "1" {
		s, err := NewLocal(os.Getenv("GAT_CAS_ROOT"))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("ready")
		if _, err := io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		err = s.Put(context.Background(), "HEAD", bytes.Repeat([]byte(os.Getenv("GAT_CAS_VALUE")), 128<<10), os.Getenv("GAT_CAS_TOKEN"))
		if errors.Is(err, ErrConflict) {
			fmt.Println("conflict")
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("published")
		return
	}
	for _, existing := range []bool{false, true} {
		t.Run(strconv.FormatBool(existing), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			root := t.TempDir()
			s, err := NewLocal(root)
			if err != nil {
				t.Fatal(err)
			}
			token := "*"
			if existing {
				if err := s.Put(ctx, "HEAD", []byte("before"), "*"); err != nil {
					t.Fatal(err)
				}
				_, token, err = s.Get(ctx, "HEAD", 0, -1)
				if err != nil {
					t.Fatal(err)
				}
			}
			type child struct {
				cmd    *exec.Cmd
				input  io.WriteCloser
				output *bytes.Buffer
			}
			var children []child
			for i := range 8 {
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLocalCASProcesses$")
				cmd.Env = append(os.Environ(), "GAT_CAS_CHILD=1", "GAT_CAS_ROOT="+root, "GAT_CAS_TOKEN="+token, "GAT_CAS_VALUE="+strconv.Itoa(i))
				input, err := cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				output := new(bytes.Buffer)
				cmd.Stdout, cmd.Stderr = output, output
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				children = append(children, child{cmd, input, output})
			}
			for _, child := range children {
				child.input.Write([]byte{1})
				child.input.Close()
			}
			successes := 0
			for _, child := range children {
				if err := child.cmd.Wait(); err != nil {
					t.Fatalf("child: %v: %s", err, child.output)
				}
				if bytes.Contains(child.output.Bytes(), []byte("published")) {
					successes++
				}
			}
			if successes != 1 {
				t.Fatalf("%d independent processes published", successes)
			}
			data, _, err := s.Get(ctx, "HEAD", 0, -1)
			if err != nil || len(data) != 128<<10 {
				t.Fatalf("incomplete publication: %d bytes: %v", len(data), err)
			}
			if !bytes.Equal(data, bytes.Repeat(data[:1], len(data))) {
				t.Fatal("mixed publication bytes")
			}
		})
	}
}
