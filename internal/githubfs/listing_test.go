package githubfs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOwnerListingPages(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Error("public listing unexpectedly authenticated")
		}
		var names []string
		switch r.URL.Path {
		case "/orgs/acme/repos":
			if r.URL.Query().Get("page") == "1" {
				for i := range 100 {
					names = append(names, fmt.Sprintf("repo-%03d", i))
				}
			} else {
				names = []string{"last-page"}
			}
		default:
			t.Errorf("unexpected API %s", r.URL)
			http.NotFound(w, r)
			return
		}
		var out []remoteRepository
		for _, name := range names {
			repo := remoteRepository{Name: name}
			repo.Owner.Login = "acme"
			out = append(out, repo)
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()
	opts, _, _ := fixture(t)
	opts.APIBase = server.URL
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var names []string
	after := ""
	for {
		entries, err := f.ReadDir(t.Context(), "acme", after, 30)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			names = append(names, e.Name)
		}
		after = entries[len(entries)-1].Name
	}
	if len(names) != 101 || !strings.Contains(strings.Join(names, " "), "last-page") {
		t.Fatalf("incomplete listing: %v", names)
	}
	if calls.Load() != 2 {
		t.Fatalf("pagination repeated requests: %d", calls.Load())
	}
	for _, name := range names {
		if _, err := f.Lookup(t.Context(), "acme/"+name); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.jobs) != 0 {
		t.Fatal("listing imported repositories")
	}
}
func TestListingFailureIsNotCachedAsEmptySuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer server.Close()
	opts, _, _ := fixture(t)
	opts.APIBase = server.URL
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for range 2 {
		if _, err := f.ReadDir(t.Context(), "acme", "", 128); err == nil {
			t.Fatal("listing failure hidden")
		}
	}
}
