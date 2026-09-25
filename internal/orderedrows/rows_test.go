package orderedrows

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
)

func newTestSpool(t *testing.T) *Spool {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestSpoolPreservesRowsOwnsInputsAndRemovesFile(t *testing.T) {
	s := newTestSpool(t)
	k, v := []byte("a"), []byte("first")
	if err := s.Add(t.Context(), k, v); err != nil {
		t.Fatal(err)
	}
	k[0] = 'z'
	v[0] = 'X'
	if err := s.Add(t.Context(), []byte("c"), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Next(t.Context()); err == nil {
		t.Fatal("unsealed spool was readable")
	}
	if err := s.Seal(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Seal(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(t.Context(), []byte("d"), nil); err == nil {
		t.Fatal("sealed spool accepted row")
	}
	if got := readAll(t, s.Next); !reflect.DeepEqual(got, [][2]string{{"a", "first"}, {"c", ""}}) {
		t.Fatal(got)
	}
	if got := s.Metrics(); got != (Metrics{2, 2, 5, 23}) {
		t.Fatal(got)
	}
	path := s.path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file remains: %v", err)
	}
	if _, _, err := s.Next(t.Context()); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}

func TestSpoolRejectsInvalidRowsWithoutSealingPartialResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		key, value []byte
	}{
		{"duplicate", []byte("b"), nil}, {"decreasing", []byte("a"), nil},
		{"empty", nil, nil}, {"key_limit", make([]byte, MaxKey+1), nil},
		{"value_limit", []byte("c"), make([]byte, MaxValue+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSpool(t)
			if err := s.Add(t.Context(), []byte("b"), nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Add(t.Context(), tc.key, tc.value); err == nil {
				t.Fatal("invalid row accepted")
			}
			if err := s.Seal(t.Context()); err == nil {
				t.Fatal("partially failed stream sealed")
			}
		})
	}
}

func TestSpoolEmptyCancellationAndWriteFailure(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s := newTestSpool(t)
		if err := s.Seal(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := readAll(t, s.Next); len(got) != 0 {
			t.Fatal(got)
		}
	})
	for _, op := range []string{"add", "seal", "read"} {
		t.Run(op, func(t *testing.T) {
			s := newTestSpool(t)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var err error
			switch op {
			case "add":
				err = s.Add(ctx, []byte("a"), nil)
			case "seal":
				err = s.Seal(ctx)
			case "read":
				if err = s.Seal(t.Context()); err != nil {
					t.Fatal(err)
				}
				_, _, err = s.Next(ctx)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
	t.Run("write_failure", func(t *testing.T) {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err = s.file.Close(); err != nil {
			t.Fatal(err)
		}
		if err = s.Add(t.Context(), []byte("a"), []byte("b")); err != nil {
			t.Fatal(err)
		}
		if err = s.Seal(t.Context()); err == nil {
			t.Fatal("failed flush accepted")
		}
		_ = s.Close()
		if _, err = os.Stat(s.path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	})
}

func TestSpoolRejectsTruncatedAndOversizedRecords(t *testing.T) {
	var valid [8]byte
	binary.LittleEndian.PutUint32(valid[:4], 1)
	binary.LittleEndian.PutUint32(valid[4:], 2)
	var oversized [8]byte
	binary.LittleEndian.PutUint32(oversized[:4], MaxKey+1)
	for _, raw := range [][]byte{{1, 2}, valid[:], append(append([]byte(nil), valid[:]...), 'a'), oversized[:]} {
		s := newTestSpool(t)
		if _, err := s.file.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := s.Seal(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Next(t.Context()); err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("invalid data accepted: %v", err)
		}
	}
}

func cursor(rows ...[2]string) Cursor {
	n := 0
	key := make([]byte, MaxKey)
	value := make([]byte, MaxValue)
	return func(ctx context.Context) ([]byte, []byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if n == len(rows) {
			return nil, nil, io.EOF
		}
		r := rows[n]
		n++
		nk, nv := copy(key, r[0]), copy(value, r[1])
		return key[:nk], value[:nv], nil
	}
}

func readAll(t *testing.T, next Cursor) [][2]string {
	t.Helper()
	var rows [][2]string
	for {
		k, v, err := next(t.Context())
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, [2]string{string(k), string(v)})
	}
}

func TestMergePreservesOrderWithBorrowedInputs(t *testing.T) {
	for _, tc := range []struct{ left, right, want [][2]string }{
		{},
		{left: [][2]string{{"a", "1"}}, want: [][2]string{{"a", "1"}}},
		{right: [][2]string{{"a", "1"}}, want: [][2]string{{"a", "1"}}},
		{left: [][2]string{{"a", "1"}, {"d", "4"}}, right: [][2]string{{"b", "2"}, {"c", "3"}, {"e", "5"}}, want: [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}}},
	} {
		if got := readAll(t, Merge(cursor(tc.left...), cursor(tc.right...))); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("got %v, want %v", got, tc.want)
		}
	}
}

func TestMergeRejectsInvalidInputsAndPropagatesFailures(t *testing.T) {
	boom := errors.New("source failure")
	for name, next := range map[string]Cursor{
		"duplicate_routes": Merge(cursor([2]string{"a", "1"}), cursor([2]string{"a", "2"})),
		"duplicate_input":  Merge(cursor([2]string{"a", "1"}, [2]string{"a", "2"}), cursor()),
		"descending_input": Merge(cursor([2]string{"b", "1"}, [2]string{"a", "2"}), cursor()),
		"source_error":     Merge(func(context.Context) ([]byte, []byte, error) { return nil, nil, boom }, cursor()),
	} {
		t.Run(name, func(t *testing.T) {
			var err error
			for i := 0; i < 3; i++ {
				_, _, err = next(t.Context())
				if err != nil {
					break
				}
			}
			if err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("invalid stream accepted: %v", err)
			}
			if name == "source_error" && !errors.Is(err, boom) {
				t.Fatal(err)
			}
			if _, _, again := next(t.Context()); again != err {
				t.Fatalf("error not retained: %v, %v", err, again)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := Merge(cursor(), cursor())(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
