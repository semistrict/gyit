//go:build !js

// Generates the presentation fixture through the real repository importer.
package main

import (
	"archive/zip"
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	output := flag.String("output", "internal/tour/testdata/repository.zip", "generated fixture archive")
	flag.Parse()
	root, err := os.MkdirTemp("", "gyit-tour-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	source := filepath.Join(root, "source")
	if err = os.Mkdir(source, 0700); err != nil {
		return err
	}
	git := func(args ...string) (string, error) {
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Example Maintainer", "GIT_AUTHOR_EMAIL=maintainer@example.test", "GIT_COMMITTER_NAME=Example Maintainer", "GIT_COMMITTER_EMAIL=maintainer@example.test", "GIT_AUTHOR_DATE=2026-01-01T12:00:00Z", "GIT_COMMITTER_DATE=2026-01-01T12:00:00Z")
		b, e := cmd.CombinedOutput()
		if e != nil {
			return "", fmt.Errorf("git %v: %w: %s", args, e, b)
		}
		return string(b), nil
	}
	write := func(name, body string) error {
		p := filepath.Join(source, name)
		if e := os.MkdirAll(filepath.Dir(p), 0755); e != nil {
			return e
		}
		return os.WriteFile(p, []byte(body), 0644)
	}
	if _, err = git("init", "-qb", "main"); err != nil {
		return err
	}
	for name, body := range map[string]string{"README.md": "# peach orchard\n\nA tiny repository for exploring real object-store reads.\n", "src/main.go": "package main\n\nfunc main() { println(\"hello, orchard\") }\n", "docs/architecture.md": "# Architecture\n\nImmutable data. Small local cache. Atomic publication.\n", "LICENSE": "Example fixture data. Released under CC0-1.0.\n"} {
		if err = write(name, body); err != nil {
			return err
		}
	}
	for i := 0; i < 24; i++ {
		if err = write(fmt.Sprintf("data/row-%02d.txt", i), fmt.Sprintf("orchard row %02d\n", i)); err != nil {
			return err
		}
	}
	for i := 0; i < 12; i++ {
		if i%4 == 0 {
			if err = write("README.md", fmt.Sprintf("# peach orchard\n\nA tiny repository for exploring real object-store reads.\n\nHarvest edition %d.\n", 1+i/4)); err != nil {
				return err
			}
		}
		if err = write("src/main.go", fmt.Sprintf("package main\n\nfunc main() { println(\"harvest %d\") }\n", i)); err != nil {
			return err
		}
		if _, err = git("add", "."); err != nil {
			return err
		}
		if _, err = git("commit", "-qm", fmt.Sprintf("Harvest %02d", i)); err != nil {
			return err
		}
		if i == 3 {
			if _, err = git("tag", "v1"); err != nil {
				return err
			}
		}
	}
	storageRoot := filepath.Join(root, "objects")
	backend, err := store.NewLocal(storageRoot)
	if err != nil {
		return err
	}
	if _, err = repo.Import(context.Background(), backend, repo.ImportOptions{Repo: source}); err != nil {
		return err
	}
	// Prepare the tagged snapshot too, so changing versions demonstrates reuse.
	p, err := repo.NewProgressive(context.Background(), backend, nil, root)
	if err != nil {
		return err
	}
	latest, err := git("rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if err = p.PrepareSnapshot(context.Background(), strings.TrimSpace(latest)); err != nil {
		return err
	}
	old, err := git("rev-parse", "v1")
	if err != nil {
		return err
	}
	if err = p.PrepareSnapshot(context.Background(), strings.TrimSpace(old)); err != nil {
		return err
	}
	expected, err := git("log", "-n10", "--format=medium", "--no-decorate", "--", "README.md")
	if err != nil {
		return err
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return err
	}
	readme, err := os.ReadFile(filepath.Join(source, "README.md"))
	if err != nil {
		return err
	}
	files := map[string][]byte{"expected/log-readme.txt": []byte(expected), "expected/readme.txt": readme, "expected/head.txt": []byte(strings.TrimSpace(head))}
	err = filepath.WalkDir(storageRoot, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() || strings.HasSuffix(path, ".lock") {
			return nil
		}
		rel, e := filepath.Rel(storageRoot, path)
		if e != nil {
			return e
		}
		b, e := os.ReadFile(path)
		files["objects/"+filepath.ToSlash(rel)] = b
		return e
	})
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		return err
	}
	f, err := os.Create(*output)
	if err != nil {
		return err
	}
	defer f.Close()
	z := zip.NewWriter(f)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		h := &zip.FileHeader{Name: name, Method: zip.Deflate}
		h.SetModTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		w, e := z.CreateHeader(h)
		if e != nil {
			return e
		}
		if _, e = w.Write(files[name]); e != nil {
			return e
		}
	}
	if err = z.Close(); err != nil {
		return err
	}
	fmt.Printf("Generated %s (%d entries)\n", *output, len(files))
	return f.Close()
}
