package githubfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"syscall"
	"time"
)

type listing struct {
	names   []string
	err     error
	expires time.Time
}
type remoteRepository struct {
	Name       string
	Private    bool
	Visibility string
	Owner      struct{ Login string }
}

// A definitive answer, including "no such public repository", is as stable as
// a listing. Only transient failures such as rate limits are retried soon.
func listingTTL(err error) time.Duration {
	if err == nil || errors.Is(err, syscall.ENOENT) {
		return 5 * time.Minute
	}
	return 10 * time.Second
}

// A repository name is a directory only when GitHub identifies it as public.
// Existing jobs remain available if a listing expires while a mount is active.
func (f *FS) publicRepository(ctx context.Context, t Target) error {
	if f.preparedOnly {
		f.mu.Lock()
		known := f.jobs[t.Key()] != nil
		f.mu.Unlock()
		if known {
			return nil
		}
		return syscall.ENOENT
	}
	if f.opts.APIBase == "" && f.opts.RemoteBase != "https://github.com" {
		return nil
	}
	f.mu.Lock()
	known := f.jobs[t.Key()] != nil
	f.mu.Unlock()
	if known {
		return nil
	}
	f.mu.Lock()
	cached, listed := f.listings[t.Owner]
	f.mu.Unlock()
	if listed && cached.err == nil && time.Now().Before(cached.expires) {
		for _, name := range cached.names {
			if strings.EqualFold(name, t.Repository) {
				return nil
			}
		}
		return syscall.ENOENT
	}
	// Direct traversal must not paginate an entire organization before fetching
	// one repository. Readdir still obtains the complete public listing.
	return f.check(ctx, "repo/"+strings.ToLower(t.Owner+"/"+t.Repository), func(ctx context.Context) error {
		response, err := f.repositoryRequest(ctx, "/repos/"+url.PathEscape(t.Owner)+"/"+url.PathEscape(t.Repository))
		if err != nil {
			return err
		}
		defer response.Body.Close()
		var r remoteRepository
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&r); err != nil {
			return err
		}
		if r.Private || (r.Visibility != "" && r.Visibility != "public") || !strings.EqualFold(r.Owner.Login, t.Owner) || !strings.EqualFold(r.Name, t.Repository) {
			return syscall.ENOENT
		}
		return nil
	})
}

// An owner name is a directory only when GitHub has that user or organization.
// The kernel looks names up before mkdir and Finder probes names; neither may
// make a name appear. Owners with known repositories need no request.
func (f *FS) publicOwner(ctx context.Context, owner string) error {
	f.mu.Lock()
	known := f.owners[owner]
	cached, listed := f.listings[owner]
	f.mu.Unlock()
	if known || listed && cached.err == nil && time.Now().Before(cached.expires) {
		return nil
	}
	if f.preparedOnly {
		return syscall.ENOENT
	}
	if f.opts.APIBase == "" && f.opts.RemoteBase != "https://github.com" {
		return nil
	}
	return f.check(ctx, "owner/"+owner, func(ctx context.Context) error {
		response, err := f.repositoryRequest(ctx, "/users/"+url.PathEscape(owner))
		if err != nil {
			return err
		}
		defer response.Body.Close()
		var r struct{ Login string }
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&r); err != nil {
			return err
		}
		if !strings.EqualFold(r.Login, owner) {
			return syscall.ENOENT
		}
		return nil
	})
}

type check struct {
	err     error
	expires time.Time
}

// check answers an existence question from a bounded, expiring cache. One
// request per key is in flight; it belongs to the filesystem, so a caller that
// stops waiting does not cancel it for others.
func (f *FS) check(ctx context.Context, key string, request func(context.Context) error) error {
	f.mu.Lock()
	cached, ok := f.checks[key]
	f.mu.Unlock()
	if ok && time.Now().Before(cached.expires) {
		return cached.err
	}
	result := f.listingFlight.DoChan("check/"+key, func() (any, error) {
		call, cancel := context.WithTimeout(f.ctx, 30*time.Second)
		defer cancel()
		err := request(call)
		f.mu.Lock()
		if f.checks == nil {
			f.checks = make(map[string]check)
		}
		if len(f.checks) >= 1024 {
			for k := range f.checks {
				delete(f.checks, k)
				break
			}
		}
		f.checks[key] = check{err: err, expires: time.Now().Add(listingTTL(err))}
		f.mu.Unlock()
		return nil, err
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-result:
		return r.Err
	}
}

// Directory metadata is short-lived and bounded independently of file content.
// Listing never imports a repository or starts a preparation job.
func (f *FS) listRepositories(ctx context.Context, owner string) ([]string, error) {
	if f.preparedOnly {
		f.mu.Lock()
		defer f.mu.Unlock()
		var names []string
		for _, j := range f.jobs {
			if j.target.Owner == owner {
				names = append(names, j.target.Repository)
			}
		}
		sort.Strings(names)
		return names, nil
	}
	if f.opts.APIBase == "" && f.opts.RemoteBase != "https://github.com" {
		return nil, nil
	}
	f.mu.Lock()
	cached, ok := f.listings[owner]
	f.mu.Unlock()
	if ok && time.Now().Before(cached.expires) {
		return cached.names, cached.err
	}
	result := f.listingFlight.DoChan(owner, func() (any, error) {
		call, cancel := context.WithTimeout(f.ctx, 30*time.Second)
		defer cancel()
		names, err := f.fetchRepositories(call, owner)
		ttl := listingTTL(err)
		f.mu.Lock()
		if len(f.listings) >= 64 {
			for k := range f.listings {
				delete(f.listings, k)
				break
			}
		}
		f.listings[owner] = listing{names: names, err: err, expires: time.Now().Add(ttl)}
		f.mu.Unlock()
		f.changed(owner)
		// A cached kernel listing must be dropped when the API listing expires,
		// otherwise no readdir would reach this refresh path again.
		time.AfterFunc(ttl, func() {
			if f.ctx.Err() == nil {
				f.changed(owner)
			}
		})
		return names, err
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-result:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.([]string), nil
	}
}
func (f *FS) fetchRepositories(ctx context.Context, owner string) ([]string, error) {
	repos, status, err := f.repositoryPages(ctx, "/orgs/"+url.PathEscape(owner)+"/repos?type=public")
	if status == http.StatusNotFound {
		repos, _, err = f.repositoryPages(ctx, "/users/"+url.PathEscape(owner)+"/repos?type=owner")
	}
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for _, r := range repos {
		if !r.Private && (r.Visibility == "" || r.Visibility == "public") && strings.EqualFold(r.Owner.Login, owner) && validName(r.Name) {
			names[r.Name] = true
		}
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}
func (f *FS) repositoryRequest(ctx context.Context, path string) (*http.Response, error) {
	base := f.opts.APIBase
	if base == "" {
		base = "https://api.github.com"
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "gyit")
	if f.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+f.opts.Token)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		response.Body.Close()
		code := syscall.EIO
		if response.StatusCode == 404 {
			code = syscall.ENOENT
		}
		if response.StatusCode == 401 || response.StatusCode == 403 {
			code = syscall.EACCES
		}
		return response, fmt.Errorf("GitHub repository lookup HTTP %d: %w", response.StatusCode, code)
	}
	return response, nil
}

func (f *FS) repositoryPages(ctx context.Context, path string) ([]remoteRepository, int, error) {
	var all []remoteRepository
	for page := 1; ; page++ {
		response, err := f.repositoryRequest(ctx, fmt.Sprintf("%s&per_page=100&page=%d", path, page))
		if err != nil {
			if response != nil {
				return nil, response.StatusCode, err
			}
			return nil, 0, err
		}
		var batch []remoteRepository
		err = json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&batch)
		response.Body.Close()
		if err != nil {
			return nil, 200, err
		}
		all = append(all, batch...)
		if len(batch) < 100 {
			return all, 200, nil
		}
		if len(all) > 200000 {
			return nil, 200, fmt.Errorf("GitHub repository listing too large: %w", syscall.EOVERFLOW)
		}
	}
}
