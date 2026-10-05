package storagetest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beesaferoot/jobsync"
)

// RunAPI exercises the dashboard's JSON API against a driver. It is separate
// from Run because it tests the HTTP layer rather than the Storage contract, but
// it runs against every driver for the same reason: the API is what the UI
// depends on, and a shape that differs between drivers becomes permanent the
// moment a page reads it.
func RunAPI(t *testing.T, newStore New) {
	t.Run("Overview", func(t *testing.T) { testAPIOverview(t, newStore) })
	t.Run("ListAndFilter", func(t *testing.T) { testAPIList(t, newStore) })
	t.Run("JobDetail", func(t *testing.T) { testAPIJobDetail(t, newStore) })
	t.Run("RequeueAndDelete", func(t *testing.T) { testAPIMutations(t, newStore) })
	t.Run("Schedules", func(t *testing.T) { testAPISchedules(t, newStore) })
	t.Run("UnsupportedFilterIsNotSilent", func(t *testing.T) { testAPIUnsupported(t, newStore) })
	t.Run("RejectsBadInput", func(t *testing.T) { testAPIBadInput(t, newStore) })
	t.Run("PauseRoundTrip", func(t *testing.T) { testAPIPause(t, newStore) })
}

// Pausing has to be reachable, not merely storable. The dashboard showed a
// paused badge for months of development with no route that could ever set it.
func testAPIPause(t *testing.T, newStore New) {
	s := newStore(t)
	srv := dash(t, s)

	overview, _ := getJSON(t, srv, "/jobs/api/overview")
	caps := overview["capabilities"].(map[string]any)

	if caps["pause"] == true {
		j := job("a")
		j.Queue = "alpha"
		put(t, s, j)

		if _, status := postJSON(t, srv, "/jobs/api/queues/alpha/pause", `{}`); status != http.StatusOK {
			t.Fatalf("pause queue returned %d", status)
		}
		overview, _ = getJSON(t, srv, "/jobs/api/overview")
		if !queuePaused(overview, "alpha") {
			t.Error("queue paused but the overview still reports it live")
		}

		if _, status := postJSON(t, srv, "/jobs/api/queues/alpha/resume", `{}`); status != http.StatusOK {
			t.Fatalf("resume queue returned %d", status)
		}
		overview, _ = getJSON(t, srv, "/jobs/api/overview")
		if queuePaused(overview, "alpha") {
			t.Error("queue resumed but the overview still reports it paused")
		}
	}

	if caps["schedules"] != true {
		return
	}
	sch, ok := s.(jobsync.Schedules)
	if !ok {
		return
	}
	err := sch.SaveSchedule(context.Background(), jobsync.Schedule{
		ID: "nightly", Kind: "test", Queue: "default", Payload: []byte(`{}`),
		Cron: "0 2 * * *", Timezone: "UTC", NextRun: timeNow(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, status := postJSON(t, srv, "/jobs/api/schedules/nightly/pause", `{}`); status != http.StatusOK {
		t.Fatalf("pause schedule returned %d", status)
	}
	body, _ := getJSON(t, srv, "/jobs/api/schedules")
	if !schedulePaused(body, "nightly") {
		t.Error("schedule paused but the API still reports it live")
	}

	if _, status := postJSON(t, srv, "/jobs/api/schedules/nightly/resume", `{}`); status != http.StatusOK {
		t.Fatalf("resume schedule returned %d", status)
	}
	body, _ = getJSON(t, srv, "/jobs/api/schedules")
	if schedulePaused(body, "nightly") {
		t.Error("schedule resumed but the API still reports it paused")
	}
}

func queuePaused(body map[string]any, name string) bool {
	qs, _ := body["queues"].([]any)
	for _, q := range qs {
		if m, ok := q.(map[string]any); ok && m["name"] == name {
			return m["paused"] == true
		}
	}
	return false
}

func schedulePaused(body map[string]any, id string) bool {
	ss, _ := body["schedules"].([]any)
	for _, sc := range ss {
		if m, ok := sc.(map[string]any); ok && m["id"] == id {
			return m["paused"] == true
		}
	}
	return false
}

func timeNow() time.Time { return time.Now() }

func dash(t *testing.T, s jobsync.Storage) *httptest.Server {
	t.Helper()
	h := jobsync.Dashboard(s, jobsync.DashboardConfig{
		BasePath: "/jobs",
		Auth:     jobsync.AllowAll,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, srv *httptest.Server, path string) (map[string]any, int) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("GET %s returned non-JSON (%d): %s", path, resp.StatusCode, body)
	}
	return out, resp.StatusCode
}

func postJSON(t *testing.T, srv *httptest.Server, path, body string) (map[string]any, int) {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("POST %s returned non-JSON (%d): %s", path, resp.StatusCode, raw)
	}
	return out, resp.StatusCode
}

func testAPIOverview(t *testing.T, newStore New) {
	s := newStore(t)
	seed(t, s)
	srv := dash(t, s)

	body, status := getJSON(t, srv, "/jobs/api/overview")
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, body)
	}

	counts, ok := body["counts"].(map[string]any)
	if !ok {
		t.Fatalf("no counts object: %v", body)
	}
	// Every state key must be present even at zero. A UI that has to cope with a
	// missing key renders a blank tile instead of a zero, and blank reads as
	// "broken" during an incident.
	for _, want := range []string{"scheduled", "enqueued", "running", "retrying", "succeeded", "dead", "cancelled"} {
		if _, ok := counts[want]; !ok {
			t.Errorf("counts is missing %q: %v", want, counts)
		}
	}
	if counts["running"] != float64(1) {
		t.Errorf("counts.running = %v, want 1", counts["running"])
	}

	caps, ok := body["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("no capabilities object: %v", body)
	}
	for _, want := range []string{"search", "throughput", "schedules"} {
		if _, ok := caps[want]; !ok {
			t.Errorf("capabilities is missing %q: %v", want, caps)
		}
	}
	if _, ok := body["queues"]; !ok {
		t.Error("no queues in overview")
	}
	if _, ok := body["servers"]; !ok {
		t.Error("no servers in overview")
	}
}

func testAPIList(t *testing.T, newStore New) {
	s := newStore(t)
	seed(t, s)
	srv := dash(t, s)

	body, status := getJSON(t, srv, "/jobs/api/jobs?state=running")
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, body)
	}
	jobs, _ := body["jobs"].([]any)
	if len(jobs) != 1 {
		t.Fatalf("state=running returned %d jobs, want 1", len(jobs))
	}
	job := jobs[0].(map[string]any)
	if job["state"] != "running" {
		t.Errorf("job state = %v, want running", job["state"])
	}
	if job["id"] != "running" {
		t.Errorf("job id = %v, want running", job["id"])
	}
	// The list page renders hundreds of rows and never shows a payload; sending
	// it would be bytes nobody reads.
	if _, present := job["payload"]; present {
		t.Error("list response carries payloads")
	}
	if body["total"] == nil {
		t.Error("no total; the UI cannot render a pager")
	}

	body, _ = getJSON(t, srv, "/jobs/api/jobs?limit=2")
	jobs, _ = body["jobs"].([]any)
	if len(jobs) != 2 {
		t.Errorf("limit=2 returned %d jobs", len(jobs))
	}
}

func testAPIJobDetail(t *testing.T, newStore New) {
	s := newStore(t)
	seed(t, s)
	srv := dash(t, s)

	body, status := getJSON(t, srv, "/jobs/api/jobs/dead")
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, body)
	}
	job, ok := body["job"].(map[string]any)
	if !ok {
		t.Fatalf("no job object: %v", body)
	}
	if job["last_error"] != "boom" {
		t.Errorf("last_error = %v, want boom", job["last_error"])
	}
	// The detail page is the one place a payload is worth sending.
	if job["payload"] == nil {
		t.Error("detail response carries no payload")
	}
	if _, ok := body["transitions"].([]any); !ok {
		t.Errorf("no transitions array: %v", body)
	}

	_, status = getJSON(t, srv, "/jobs/api/jobs/no-such-job")
	if status != http.StatusNotFound {
		t.Errorf("unknown job returned %d, want 404", status)
	}
}

func testAPIMutations(t *testing.T, newStore New) {
	s := newStore(t)
	seed(t, s)
	srv := dash(t, s)

	body, status := postJSON(t, srv, "/jobs/api/jobs/requeue", `{"ids":["dead"]}`)
	if status != http.StatusOK {
		t.Fatalf("requeue status %d: %v", status, body)
	}
	list, _ := getJSON(t, srv, "/jobs/api/jobs?state=enqueued")
	if !containsJobID(list, "dead") {
		t.Errorf("requeued job is not enqueued: %v", list["jobs"])
	}

	body, status = postJSON(t, srv, "/jobs/api/jobs/delete", `{"ids":["succeeded"]}`)
	if status != http.StatusOK {
		t.Fatalf("delete status %d: %v", status, body)
	}
	list, _ = getJSON(t, srv, "/jobs/api/jobs?state=succeeded")
	if containsJobID(list, "succeeded") {
		t.Error("deleted job still listed")
	}
}

func testAPISchedules(t *testing.T, newStore New) {
	s := newStore(t)
	srv := dash(t, s)

	body, status := getJSON(t, srv, "/jobs/api/schedules")
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, body)
	}
	// Always an array, never null: a UI that must special-case null before it can
	// iterate is a UI that will forget to.
	if _, ok := body["schedules"].([]any); !ok {
		t.Errorf("schedules is %T, want an array", body["schedules"])
	}
}

// A driver that cannot serve a filter must say so with a distinguishable code.
// Returning everything would show an operator the wrong jobs during an incident,
// which is worse than an error.
func testAPIUnsupported(t *testing.T, newStore New) {
	s := newStore(t)
	seed(t, s)
	srv := dash(t, s)

	body, status := getJSON(t, srv, "/jobs/api/jobs?search=email")
	switch status {
	case http.StatusOK:
		if _, ok := body["jobs"]; !ok {
			t.Errorf("200 without a jobs array: %v", body)
		}
	case http.StatusBadRequest:
		if body["code"] != "unsupported_filter" {
			t.Errorf("400 without the unsupported_filter code: %v", body)
		}
		// And the UI must have been told in advance, so it never renders the box.
		overview, _ := getJSON(t, srv, "/jobs/api/overview")
		caps := overview["capabilities"].(map[string]any)
		if caps["search"] != false {
			t.Error("driver rejects search but overview advertises capabilities.search = true")
		}
	default:
		t.Errorf("search returned %d: %v", status, body)
	}
}

func testAPIBadInput(t *testing.T, newStore New) {
	srv := dash(t, newStore(t))

	for _, tc := range []struct{ name, body string }{
		{"not json", `{`},
		{"no ids", `{"ids":[]}`},
		{"wrong shape", `{"identifiers":["a"]}`},
	} {
		_, status := postJSON(t, srv, "/jobs/api/jobs/requeue", tc.body)
		if status != http.StatusBadRequest {
			t.Errorf("requeue with %s returned %d, want 400", tc.name, status)
		}
	}

	// Garbage in a query parameter falls back to the default rather than
	// erroring: a stale bookmark should still render the page.
	body, status := getJSON(t, srv, "/jobs/api/jobs?limit=abc&offset=-5")
	if status != http.StatusOK {
		t.Errorf("unparseable paging returned %d: %v", status, body)
	}
}

func containsJobID(body map[string]any, id string) bool {
	jobs, _ := body["jobs"].([]any)
	for _, j := range jobs {
		if m, ok := j.(map[string]any); ok && m["id"] == id {
			return true
		}
	}
	return false
}
