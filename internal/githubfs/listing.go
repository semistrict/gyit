package githubfs

import (
	"context"
	"encoding/json"
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
	Name  string
	Owner struct{ Login string }
}

// Directory metadata is short-lived and bounded independently of file content.
// Listing never imports a repository or starts a preparation job.
func (f *FS) listRepositories(ctx context.Context, owner string) ([]string, error) {
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
		ttl := 5 * time.Minute
		if err != nil {
			ttl = 10 * time.Second
		}
		f.mu.Lock()
		if len(f.listings) >= 64 {
			for k := range f.listings {
				delete(f.listings, k)
				break
			}
		}
		f.listings[owner] = listing{names: names, err: err, expires: time.Now().Add(ttl)}
		f.mu.Unlock()
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
		if strings.EqualFold(r.Owner.Login, owner) && validName(r.Name) {
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
func (f *FS) repositoryPages(ctx context.Context, path string) ([]remoteRepository, int, error) {
	base := f.opts.APIBase
	if base == "" {
		base = "https://api.github.com"
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var all []remoteRepository
	for page := 1; ; page++ {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s%s&per_page=100&page=%d", strings.TrimRight(base, "/"), path, page), nil)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		req.Header.Set("User-Agent", "gyit")
		if f.opts.Token != "" {
			req.Header.Set("Authorization", "Bearer "+f.opts.Token)
		}
		response, err := client.Do(req)
		if err != nil {
			return nil, 0, err
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
			return nil, response.StatusCode, fmt.Errorf("GitHub repository listing HTTP %d: %w", response.StatusCode, code)
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
