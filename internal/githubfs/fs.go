// Package githubfs exposes GitHub repositories after background snapshot setup.
package githubfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/singleflight"

	"gyit/internal/control"
	"gyit/internal/repo"
	"gyit/internal/store"

	"golang.org/x/sys/unix"
)

type Options struct {
	// StoreRoot optionally locates durable progressive repositories in object storage.
	// DataDir still owns local acquisition/process state; CacheDir remains disposable.
	StoreRoot string
	// DataDir is durable repository storage, never subject to cache eviction.
	DataDir string
	// CacheDir contains disposable decoded data shared by every repository.
	CacheDir string
	// CacheBytes limits the combined cache size, not each repository or revision.
	CacheBytes int64
	Token      string
	// RemoteBase defaults to GitHub; file URLs support repeatable local tests.
	RemoteBase string
	// APIBase overrides the GitHub API for local integration tests.
	APIBase string
}

type Target struct{ Owner, Repository, Revision string }

func (t Target) Key() string { return t.Owner + "/" + t.Repository + "@" + t.Revision }
func validName(s string) bool {
	if s == "" || s == "." || s == ".." || len(s) > 100 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func parse(path string) ([]string, Target, error) {
	var t Target
	if path == "" {
		return nil, t, nil
	}
	if !fs.ValidPath(path) || path == "." || strings.ContainsRune(path, 0) {
		return nil, t, syscall.EINVAL
	}
	p := strings.Split(path, "/")
	if !validName(p[0]) || strings.ContainsAny(p[0], "._") {
		return nil, t, syscall.ENOENT
	}
	t.Owner = strings.ToLower(p[0])
	if len(p) == 1 {
		return p, t, nil
	}
	name, rev, has := strings.Cut(p[1], "@")
	if !validName(name) {
		return nil, t, syscall.ENOENT
	}
	t.Repository = strings.ToLower(name)
	if has {
		decoded, err := url.PathUnescape(rev)
		if err != nil || decoded == "" || len(decoded) > 1024 || strings.ContainsAny(decoded, "\x00\r\n") || strings.HasPrefix(decoded, "-") {
			return nil, t, syscall.EINVAL
		}
		t.Revision = decoded
	}
	return p, t, nil
}

type job struct {
	update      chan struct{}
	progressive *repo.Progressive
	control     *control.Server
	endpoint    string
	mu          sync.RWMutex
	target      Target
	display     string
	notice      string
	startedAt   time.Time
	finishedAt  time.Time
	snapshot    *repo.Snapshot
	done        chan struct{}
	noticeAfter time.Time
	generation  uint64
}

// The kernel can retain NOTICE's size across opens even when its data is read
// without caching. Keep the virtual file's length stable as progress changes.
const noticeSize = 512

func clippedNoticeText(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	s = s[:limit-3]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s + "..."
}

func (j *job) status() (*repo.Snapshot, string, uint64) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.snapshot != nil {
		return j.snapshot, "", j.generation
	}
	end := time.Now()
	if !j.finishedAt.IsZero() {
		end = j.finishedAt
	}
	seconds := max(0, int64(end.Sub(j.startedAt)/time.Second))
	header := "gyit: preparing github.com/" + clippedNoticeText(j.display, 160) + "\n\n"
	progress := j.notice
	footer := "\n\nThe complete repository will replace this NOTICE when ready."
	if strings.HasSuffix(progress, "\nTouch NOTICE to retry.") {
		progress = strings.TrimSuffix(progress, "\nTouch NOTICE to retry.")
		footer = "\n\nTouch NOTICE to retry."
	}
	footer = fmt.Sprintf("\n\nElapsed: %02d:%02d:%02d%s", seconds/3600, seconds/60%60, seconds%60, footer)
	progress = clippedNoticeText(progress, noticeSize-1-len(header)-len(footer))
	notice := header + progress + footer
	notice += strings.Repeat(" ", noticeSize-1-len(notice)) + "\n"
	return nil, notice, j.generation
}
func (j *job) progress(s string) {
	j.mu.Lock()
	j.notice = s
	j.mu.Unlock()
}

type FS struct {
	changeMu          sync.Mutex
	changeID          uint64
	changes           map[uint64]func(string)
	directoryVersions map[string]uint64
	progressiveRepos  map[string]*progressiveRepository
	preparedOnly      bool
	controlDir        string
	opts              Options
	ctx               context.Context
	cancel            context.CancelFunc
	mu                sync.Mutex
	jobs              map[string]*job
	owners            map[string]bool
	closed            bool
	workers           chan struct{}
	wg                sync.WaitGroup
	cache             *store.DiskCache
	lock              *os.File
	listings          map[string]listing
	repositoryChecks  map[string]listing
	listingFlight     singleflight.Group
}

func New(o Options) (*FS, error) {
	if o.DataDir == "" || o.CacheDir == "" {
		return nil, fmt.Errorf("repository data and cache directories are required")
	}
	if o.RemoteBase == "" {
		o.RemoteBase = "https://github.com"
	}
	u, err := url.Parse(o.RemoteBase)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "file") {
		return nil, fmt.Errorf("invalid repository remote base")
	}
	if strings.ContainsAny(o.Token, "\r\n") {
		return nil, fmt.Errorf("invalid GitHub token")
	}
	if err = os.MkdirAll(o.DataDir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(o.DataDir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("repository directory already in use: %w", err)
	}
	cache, err := store.NewDiskCache(nil, o.CacheDir, "github-snapshots-v1", o.CacheBytes)
	if err != nil {
		lock.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &FS{opts: o, ctx: ctx, cancel: cancel, jobs: make(map[string]*job), owners: make(map[string]bool), workers: make(chan struct{}, 2), cache: cache, lock: lock, listings: make(map[string]listing)}, nil
}
func (f *FS) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	f.cancel()
	f.mu.Unlock()
	f.wg.Wait()
	for _, j := range f.jobs {
		if j.control != nil {
			j.control.Close()
		}
	}
	for _, p := range f.progressiveRepos {
		if p.backend != nil {
			if c, ok := p.backend.(io.Closer); ok {
				_ = c.Close()
			}
		}
	}
	if f.controlDir != "" {
		_ = os.RemoveAll(f.controlDir)
	}
	err := f.cache.Close()
	f.lock.Close()
	return err
}

// A quick import completes within the calling read. Slower imports expose
// progress after one shared deadline, without delaying every subsequent read.
func (f *FS) ensure(ctx context.Context, t Target, display string) (*job, error) {
	if err := f.publicRepository(ctx, t); err != nil {
		return nil, err
	}
	j, err := f.ensureJob(t, display)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	done, deadline := j.done, j.noticeAfter
	f.mu.Unlock()
	if snapshot, _, _ := j.status(); snapshot != nil {
		return j, nil
	}
	timer := time.NewTimer(max(0, time.Until(deadline)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return j, nil
	case <-done:
		if snapshot, _, _ := j.status(); snapshot != nil {
			return j, nil
		}
		// Failures also wait for the notice deadline before exposing a placeholder.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return j, nil
		}
	}
}

func (f *FS) ensureJob(t Target, display string) (*job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, os.ErrClosed
	}
	if j := f.jobs[t.Key()]; j != nil {
		return j, nil
	}
	if len(f.jobs) >= 4096 {
		return nil, syscall.ENOSPC
	}
	j := &job{target: t, display: display, done: make(chan struct{}), update: make(chan struct{}, 1), generation: 1}
	j.progress("Queued for background setup.")
	f.jobs[t.Key()] = j
	f.owners[t.Owner] = true
	f.changed("")
	f.changed(t.Owner)
	f.start(j)
	return j, nil
}

// start requires f.mu; retries reuse the job and never duplicate active setup.
func (f *FS) start(j *job) {
	t := j.target
	j.noticeAfter = time.Now().Add(10 * time.Second)
	j.mu.Lock()
	j.startedAt = time.Now()
	j.finishedAt = time.Time{}
	j.mu.Unlock()
	done := j.done
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		defer func() {
			j.mu.Lock()
			j.finishedAt = time.Now()
			j.mu.Unlock()
			close(done)
		}()
		select {
		case f.workers <- struct{}{}:
			defer func() { <-f.workers }()
		case <-f.ctx.Done():
			j.progress("Setup canceled.")
			return
		}
		p, s, err := f.prepareProgressive(f.ctx, t, func(message string) { j.progress(f.redact(message)) })
		if err != nil {
			j.progress("Setup failed: " + f.redact(err.Error()) + "\nTouch NOTICE to retry.")
			return
		}
		j.mu.Lock()
		j.progressive, j.snapshot = p.reader, s
		j.generation++
		j.mu.Unlock()
		f.changed(j.display)
		f.startProgressiveBackground(p, s, j)
	}()
}

// Retry accepts only the synthetic NOTICE, never a repository-owned file.
func (f *FS) Retry(path string) error {
	p, t, err := parse(path)
	if err != nil {
		return err
	}
	if len(p) != 3 || p[2] != "NOTICE" {
		return syscall.EROFS
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	j := f.jobs[t.Key()]
	if j == nil {
		return syscall.ENOENT
	}
	s, _, _ := j.status()
	if s != nil {
		return syscall.EROFS
	}
	select {
	case <-j.done:
		j.done = make(chan struct{})
		j.progress("Queued for background setup.")
		f.start(j)
	default:
	}
	return nil
}

func directory(name string) repo.Entry { return repo.Entry{Name: name, Mode: 0040000} }
func (f *FS) Lookup(ctx context.Context, path string) (repo.Entry, error) {
	if err := ctx.Err(); err != nil {
		return repo.Entry{}, err
	}
	p, t, err := parse(path)
	if err != nil {
		return repo.Entry{}, err
	}
	if len(p) == 0 {
		return directory(""), nil
	}
	if len(p) == 1 {
		f.mu.Lock()
		if len(f.owners) >= 4096 && !f.owners[t.Owner] {
			f.mu.Unlock()
			return repo.Entry{}, syscall.ENOSPC
		}
		added := !f.owners[t.Owner]
		f.owners[t.Owner] = true
		f.mu.Unlock()
		if added {
			f.changed("")
		}
		return directory(p[0]), nil
	}
	// Finder and IDEs stat every child of an owner listing. Merely inspecting
	// a repository directory must not import it; reading it or a child does.
	if len(p) == 2 {
		if err := f.publicRepository(ctx, t); err != nil {
			return repo.Entry{}, err
		}
		return directory(p[1]), nil
	}
	j, err := f.ensure(ctx, t, strings.Join(p[:2], "/"))
	if err != nil {
		return repo.Entry{}, err
	}
	s, notice, _ := j.status()
	relative := strings.Join(p[2:], "/")
	if s == nil {
		if relative == "NOTICE" {
			return repo.Entry{Name: "NOTICE", Mode: 0100444, Size: int64(len(notice))}, nil
		}
		return repo.Entry{}, syscall.ENOENT
	}

	return s.Resolve(ctx, relative)
}
func (f *FS) ReadDir(ctx context.Context, path, after string, limit int) ([]repo.Entry, error) {
	if limit < 1 || limit > 128 {
		return nil, syscall.EINVAL
	}
	p, t, err := parse(path)
	if err != nil {
		return nil, err
	}
	var entries []repo.Entry
	if len(p) < 2 {
		if len(p) == 1 {
			names, err := f.listRepositories(ctx, t.Owner)
			if err != nil {
				return nil, err
			}
			for _, name := range names {
				entries = append(entries, directory(name))
			}
		}
		f.mu.Lock()
		if len(p) == 0 {
			for owner := range f.owners {
				entries = append(entries, directory(owner))
			}
		} else {
			for _, j := range f.jobs {
				if j.target.Owner == t.Owner {
					if f.opts.APIBase != "" || f.opts.RemoteBase == "https://github.com" {
						snapshot, _, _ := j.status()
						if snapshot == nil && j.target.Revision == "" {
							continue
						}
					}
					_, name, _ := strings.Cut(j.display, "/")
					entries = append(entries, directory(name))
				}
			}
		}
		f.mu.Unlock()
	} else {
		j, err := f.ensure(ctx, t, strings.Join(p[:2], "/"))
		if err != nil {
			return nil, err
		}
		s, notice, _ := j.status()
		if s == nil {
			if len(p) > 2 {
				return nil, syscall.ENOTDIR
			}
			entries = []repo.Entry{{Name: "NOTICE", Mode: 0100444, Size: int64(len(notice))}}
		} else {

			e, err := s.Resolve(ctx, strings.Join(p[2:], "/"))
			if err != nil {
				return nil, err
			}
			if e.Mode == 0160000 {
				return nil, nil
			}
			if e.Mode != 0040000 {
				return nil, syscall.ENOTDIR
			}
			entries, err := s.ReadDirectory(ctx, e, after, limit)
			if err != nil {
				return nil, err
			}

			return entries, nil
		}
	}
	// Deduplicate visited aliases against the remote listing.
	seen := make(map[string]bool)
	unique := entries[:0]
	for _, e := range entries {
		if !seen[e.Name] {
			seen[e.Name] = true
			unique = append(unique, e)
		}
	}
	entries = unique
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	i := sort.Search(len(entries), func(i int) bool { return entries[i].Name > after })
	return entries[i:min(i+limit, len(entries))], nil
}
func (f *FS) Read(ctx context.Context, path string, b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, syscall.EINVAL
	}
	p, t, err := parse(path)
	if err != nil {
		return 0, err
	}
	if len(p) < 3 {
		return 0, syscall.EISDIR
	}
	j, err := f.ensure(ctx, t, strings.Join(p[:2], "/"))
	if err != nil {
		return 0, err
	}
	s, notice, _ := j.status()
	relative := strings.Join(p[2:], "/")
	if s == nil {
		if relative != "NOTICE" {
			return 0, syscall.ENOENT
		}
		if off >= int64(len(notice)) {
			return 0, nil
		}
		return copy(b, notice[off:]), nil
	}

	e, err := s.Resolve(ctx, relative)
	if err != nil {
		return 0, err
	}
	if e.Mode == 0040000 || e.Mode == 0160000 {
		return 0, syscall.EISDIR
	}
	n, err := s.ReadAt(ctx, e.OID, b, off)
	if err == io.EOF {
		err = nil
	}
	return n, err
}

// Generation changes only when the complete snapshot becomes visible.
func (f *FS) Generation(path string) uint64 {
	p, t, err := parse(path)
	if err != nil || len(p) < 2 {
		return 1
	}
	f.mu.Lock()
	j := f.jobs[t.Key()]
	f.mu.Unlock()
	if j == nil {
		return 1
	}
	_, _, g := j.status()
	return g
}
func storeID(t Target, sha string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(t.Owner+"/"+t.Repository+"/"+sha)))
}
