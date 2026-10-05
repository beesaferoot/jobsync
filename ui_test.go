package jobsync_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beesaferoot/jobsync"
	"github.com/beesaferoot/jobsync/driver/memory"
)

func TestDashboardServesUI(t *testing.T) {
	srv := httptest.NewServer(jobsync.Dashboard(memory.New(), jobsync.DashboardConfig{
		BasePath: "/jobs",
		Auth:     jobsync.AllowAll,
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/jobs/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	// The page must know where it was mounted, or every API call 404s behind a
	// proxy that rewrites paths.
	if !strings.Contains(string(body), `window.JOBSYNC_BASE = "/jobs"`) {
		t.Error("the mount point was not injected into the page")
	}
	// No CDN: the dashboard has to work on a laptop with no internet and in an
	// air-gapped deployment.
	for _, forbidden := range []string{"https://", "http://cdn", "unpkg", "jsdelivr"} {
		if strings.Contains(string(body), forbidden) {
			t.Errorf("page references an external resource (%q); it must be self-contained", forbidden)
		}
	}
}

// The base path is injected into a <script> block, so a mount point containing
// markup must not be able to close it.
func TestDashboardBasePathCannotBreakOut(t *testing.T) {
	srv := httptest.NewServer(jobsync.Dashboard(memory.New(), jobsync.DashboardConfig{
		BasePath: `/x</script><script>alert(1)</script>`,
		Auth:     jobsync.AllowAll,
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + `/x</script><script>alert(1)</script>/`)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if strings.Contains(string(body), "<script>alert(1)</script>") {
		t.Error("a base path containing markup escaped its script block")
	}
}

func TestDashboardAuthDefaultsToLoopback(t *testing.T) {
	// Auth left nil: the default must not be "open".
	h := jobsync.Dashboard(memory.New(), jobsync.DashboardConfig{BasePath: ""})

	// A proxied request — loopback RemoteAddr but a forwarding header — is the
	// case that makes a naive loopback check world-readable.
	req := httptest.NewRequest("GET", "/api/overview", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("proxied request got %d, want 403", w.Code)
	}

	req = httptest.NewRequest("GET", "/api/overview", nil)
	req.RemoteAddr = "203.0.113.9:1234"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("remote request got %d, want 403", w.Code)
	}

	req = httptest.NewRequest("GET", "/api/overview", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("local request got %d, want 200", w.Code)
	}
}

func TestBasicAuth(t *testing.T) {
	h := jobsync.Dashboard(memory.New(), jobsync.DashboardConfig{
		Auth: jobsync.BasicAuth("ops", "s3cret"),
	})

	req := httptest.NewRequest("GET", "/api/overview", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("no credentials got %d, want 401", w.Code)
	}
	// Without the challenge header the browser never prompts.
	if w.Header().Get("WWW-Authenticate") == "" {
		t.Error("401 without a WWW-Authenticate challenge")
	}

	req = httptest.NewRequest("GET", "/api/overview", nil)
	req.SetBasicAuth("ops", "wrong")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("wrong password got %d, want 401", w.Code)
	}

	req = httptest.NewRequest("GET", "/api/overview", nil)
	req.SetBasicAuth("ops", "s3cret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("correct credentials got %d, want 200", w.Code)
	}
}
