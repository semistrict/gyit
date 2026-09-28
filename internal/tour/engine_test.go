package tour

import (
	"context"
	"strings"
	"testing"
)

func TestRealReaderFixture(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	run := func(cmd string) Result {
		t.Helper()
		r := e.Run(context.Background(), cmd, 0, func(v Event) {
			b, ok := e.objects[v.Key]
			if !ok || v.Offset < 0 || v.Offset+int64(v.Bytes) > int64(len(b)) {
				t.Errorf("invalid request: %+v", v)
			}
		})
		if r.Error != "" {
			t.Fatalf("%s: %s", cmd, r.Error)
		}
		return r
	}
	listing := run("ls -al")
	for _, name := range []string{"README.md", "src/", "docs/", "data/", "LICENSE"} {
		if !strings.Contains(listing.Output, name) {
			t.Fatalf("listing missing %s: %s", name, listing.Output)
		}
	}
	run("cd data")
	if got := strings.Count(run("ls").Output, "\n"); got != 24 {
		t.Fatalf("data entries: %d", got)
	}
	run("cd /")
	cold := run("gyit log -n 10 README.md")
	if cold.Output != string(e.expected["log-readme.txt"]) {
		t.Fatalf("log mismatch:\n%s\nwant:\n%s", cold.Output, e.expected["log-readme.txt"])
	}
	warm := run("gyit log -n 10 README.md")
	if warm.Output != cold.Output || cold.Requests == 0 || warm.Requests != 0 {
		t.Fatalf("cold/warm mismatch: %+v / %+v", cold, warm)
	}
	if got := run("cat README.md").Output; got != string(e.expected["readme.txt"]) {
		t.Fatalf("cat mismatch: %q", got)
	}
	run("cache clear")
	again := run("gyit log -n 10 README.md")
	if again.Output != cold.Output || again.Requests == 0 {
		t.Fatal("cleared cache did not reread history")
	}
	run("gyit checkout v1")
	if run("cat README.md").Output == string(e.expected["readme.txt"]) {
		t.Fatal("tag did not change contents")
	}
	run("gyit checkout main")
	if run("cat README.md").Output != string(e.expected["readme.txt"]) {
		t.Fatal("main did not restore contents")
	}
	run("cd src")
	if run("pwd").Output != "/src\n" {
		t.Fatal("cwd mismatch")
	}
	run("cd ../../")
	if run("pwd").Output != "/\n" {
		t.Fatal("escaped root")
	}
	for _, cmd := range []string{"cat missing", "cd README.md", "cat src", "rm README.md", "gyit log -n -1"} {
		if e.Run(context.Background(), cmd, 0, nil).Error == "" {
			t.Fatalf("accepted %q", cmd)
		}
	}
}
