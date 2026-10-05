package jobsync

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The dashboard's JSON API. It is the whole contract between the UI and a
// driver, which is why it is built and tested against every driver before a line
// of UI exists: once a page reads a field, the shape of that field is permanent.
//
//	GET  {base}/api/overview            counts, queues, servers, throughput
//	GET  {base}/api/jobs?...            a filtered page of jobs
//	GET  {base}/api/jobs/{id}           one job and its transitions
//	POST {base}/api/jobs/requeue        {"ids": [...]}
//	POST {base}/api/jobs/delete         {"ids": [...]}
//	GET  {base}/api/schedules           recurring jobs
//
// Capability is reported, never faked. A driver that is not a Monitor answers
// 501 on every route rather than an empty page that looks like an idle system.

type apiServer struct {
	store Storage
	base  string
}

// jobJSON is the wire shape. Separate from Job because the two have different
// jobs to do: Job is what a driver stores, this is what a browser renders —
// durations as milliseconds, times as RFC 3339, payload as a string, and no
// lease or owner bookkeeping the UI has no use for.
type jobJSON struct {
	ID          string     `json:"id"`
	Kind        string     `json:"kind"`
	Queue       string     `json:"queue"`
	State       State      `json:"state"`
	Priority    int        `json:"priority"`
	Attempt     int        `json:"attempt"`
	MaxAttempts int        `json:"max_attempts"`
	Tags        []string   `json:"tags,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	Payload     string     `json:"payload,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	Owner       string     `json:"owner,omitempty"`
}

func toJobJSON(j *Job, withPayload bool) jobJSON {
	out := jobJSON{
		ID: j.ID, Kind: j.Kind, Queue: j.Queue, State: j.State,
		Priority: j.Priority, Attempt: j.Attempt, MaxAttempts: j.MaxAttempts,
		Tags: j.Tags, LastError: j.LastError,
		CreatedAt: j.CreatedAt, ScheduledAt: j.ScheduledAt, Owner: j.Owner,
	}
	if !j.FinishedAt.IsZero() {
		t := j.FinishedAt
		out.FinishedAt = &t
	}
	// The list page shows hundreds of rows and never renders a payload, so it is
	// sent only on the detail route. A 4KB payload times 100 rows is 400KB of
	// JSON nobody reads.
	if withPayload {
		out.Payload = string(j.Payload)
	}
	return out
}

func (a *apiServer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/overview", a.overview)
	mux.HandleFunc("GET /api/jobs", a.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", a.jobDetail)
	mux.HandleFunc("POST /api/jobs/requeue", a.requeue)
	mux.HandleFunc("POST /api/jobs/delete", a.deleteJobs)
	mux.HandleFunc("GET /api/schedules", a.schedules)
	mux.HandleFunc("POST /api/schedules/{id}/pause", a.pauseSchedule(true))
	mux.HandleFunc("POST /api/schedules/{id}/resume", a.pauseSchedule(false))
	mux.HandleFunc("POST /api/schedules/{id}/trigger", a.triggerSchedule)
	mux.HandleFunc("POST /api/queues/{name}/pause", a.pauseQueue(true))
	mux.HandleFunc("POST /api/queues/{name}/resume", a.pauseQueue(false))
	return mux
}

func (a *apiServer) monitor(w http.ResponseWriter) (Monitor, bool) {
	m, ok := a.store.(Monitor)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error": "this storage driver provides no introspection",
		})
		return nil, false
	}
	return m, true
}

func (a *apiServer) overview(w http.ResponseWriter, r *http.Request) {
	m, ok := a.monitor(w)
	if !ok {
		return
	}
	ctx := r.Context()
	// The footer reports how long this took. Hangfire does the same, and it earns
	// its space: a dashboard that has quietly gone from 8ms to 3s is telling you
	// something about the storage that no counter will.
	started := time.Now()

	counts, err := m.Counts(ctx)
	if err != nil {
		writeError(w, err)
		return
	}
	queues, err := m.QueueStats(ctx)
	if err != nil {
		writeError(w, err)
		return
	}
	servers, err := m.Servers(ctx)
	if err != nil {
		writeError(w, err)
		return
	}

	window := durationParam(r, "window", 6*time.Hour)
	bucket := durationParam(r, "bucket", 5*time.Minute)
	throughput, err := m.Throughput(ctx, time.Now().Add(-window), bucket)
	if err != nil {
		writeError(w, err)
		return
	}

	type queueJSON struct {
		Name           string `json:"name"`
		Enqueued       int64  `json:"enqueued"`
		Running        int64  `json:"running"`
		Paused         bool   `json:"paused"`
		OldestEnqueued int64  `json:"oldest_enqueued_ms"`
	}
	qs := make([]queueJSON, len(queues))
	for i, q := range queues {
		qs[i] = queueJSON{q.Name, q.Enqueued, q.Running, q.Paused, q.OldestEnqueued.Milliseconds()}
	}

	type serverJSON struct {
		ID          string    `json:"id"`
		Hostname    string    `json:"hostname"`
		Queues      []string  `json:"queues"`
		Concurrency int       `json:"concurrency"`
		StartedAt   time.Time `json:"started_at"`
		HeartbeatAt time.Time `json:"heartbeat_at"`
	}
	ss := make([]serverJSON, len(servers))
	for i, s := range servers {
		ss[i] = serverJSON{s.ID, s.Hostname, s.Queues, s.Concurrency, s.StartedAt, s.HeartbeatAt}
	}

	_, searchable := a.store.(Monitor)
	if searchable {
		// Probe rather than guess: a driver advertises its inability to search by
		// returning ErrUnsupportedFilter, and the UI must know before it renders
		// a search box that would silently do nothing.
		_, _, err := m.ListJobs(ctx, Filter{Search: "\x00probe", Limit: 1})
		searchable = !errors.Is(err, ErrUnsupportedFilter)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"storage":      storageName(a.store),
		"generated_ms": float64(time.Since(started).Microseconds()) / 1000,
		"counts": map[string]int64{
			"scheduled": counts.Scheduled, "enqueued": counts.Enqueued,
			"running": counts.Running, "retrying": counts.Retrying,
			"succeeded": counts.Succeeded, "dead": counts.Dead,
			"cancelled": counts.Cancelled,
		},
		"queues":     qs,
		"servers":    ss,
		"throughput": throughput,
		"capabilities": map[string]bool{
			"search":     searchable,
			"throughput": throughput != nil,
			"schedules":  isSchedules(a.store),
			"pause":      isQueueControl(a.store),
		},
	})
}

func (a *apiServer) listJobs(w http.ResponseWriter, r *http.Request) {
	m, ok := a.monitor(w)
	if !ok {
		return
	}

	q := r.URL.Query()
	f := Filter{
		Search: q.Get("search"),
		Offset: intParam(r, "offset", 0),
		Limit:  intParam(r, "limit", 50),
	}
	for _, s := range splitParam(q.Get("state")) {
		f.States = append(f.States, State(s))
	}
	f.Queues = splitParam(q.Get("queue"))
	f.Kinds = splitParam(q.Get("kind"))
	f.Tags = splitParam(q.Get("tag"))

	jobs, total, err := m.ListJobs(r.Context(), f)
	if errors.Is(err, ErrUnsupportedFilter) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "this storage driver cannot serve that filter",
			"code":  "unsupported_filter",
		})
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}

	out := make([]jobJSON, len(jobs))
	for i, j := range jobs {
		out[i] = toJobJSON(j, false)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"jobs": out, "total": total, "offset": f.Offset, "limit": f.Limit,
	})
}

func (a *apiServer) jobDetail(w http.ResponseWriter, r *http.Request) {
	m, ok := a.monitor(w)
	if !ok {
		return
	}

	job, transitions, err := m.JobHistory(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	type transitionJSON struct {
		State  State     `json:"state"`
		At     time.Time `json:"at"`
		Reason string    `json:"reason,omitempty"`
	}
	ts := make([]transitionJSON, len(transitions))
	for i, t := range transitions {
		ts[i] = transitionJSON{t.State, t.At, t.Reason}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"job": toJobJSON(job, true), "transitions": ts,
	})
}

func (a *apiServer) requeue(w http.ResponseWriter, r *http.Request) {
	a.mutate(w, r, func(m Monitor, ids []string) error {
		return m.Requeue(r.Context(), ids)
	})
}

func (a *apiServer) deleteJobs(w http.ResponseWriter, r *http.Request) {
	a.mutate(w, r, func(m Monitor, ids []string) error {
		return m.Delete(r.Context(), ids)
	})
}

func (a *apiServer) mutate(w http.ResponseWriter, r *http.Request, do func(Monitor, []string) error) {
	m, ok := a.monitor(w)
	if !ok {
		return
	}

	var body struct {
		IDs []string `json:"ids"`
	}
	// The one genuinely untrusted input in this file: a request body. Everything
	// downstream works on []string.
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected {\"ids\": [...]}"})
		return
	}
	if len(body.IDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no job ids given"})
		return
	}
	if err := do(m, body.IDs); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"affected": len(body.IDs)})
}

func (a *apiServer) schedules(w http.ResponseWriter, r *http.Request) {
	s, ok := a.store.(Schedules)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"schedules": []any{}})
		return
	}

	list, err := s.ListSchedules(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}

	type scheduleJSON struct {
		ID       string     `json:"id"`
		Kind     string     `json:"kind"`
		Queue    string     `json:"queue"`
		Cron     string     `json:"cron"`
		Timezone string     `json:"timezone"`
		LastRun  *time.Time `json:"last_run,omitempty"`
		NextRun  time.Time  `json:"next_run"`
		Paused   bool       `json:"paused"`
	}
	out := make([]scheduleJSON, len(list))
	for i, sc := range list {
		out[i] = scheduleJSON{
			ID: sc.ID, Kind: sc.Kind, Queue: sc.Queue, Cron: sc.Cron,
			Timezone: sc.Timezone, NextRun: sc.NextRun, Paused: sc.Paused,
		}
		if !sc.LastRun.IsZero() {
			t := sc.LastRun
			out[i].LastRun = &t
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": out})
}

// pauseSchedule stops or resumes a recurring job. Pausing rather than deleting
// is what an operator wants during an incident: the definition and its history
// survive, and resuming does not re-fire whatever was missed.
func (a *apiServer) pauseSchedule(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s, ok := a.store.(Schedules)
		if !ok {
			writeJSON(w, http.StatusNotImplemented, map[string]string{
				"error": "this storage driver stores no schedules",
			})
			return
		}
		if err := s.SetSchedulePaused(r.Context(), r.PathValue("id"), paused); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"paused": paused})
	}
}

func (a *apiServer) pauseQueue(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q, ok := a.store.(QueueControl)
		if !ok {
			writeJSON(w, http.StatusNotImplemented, map[string]string{
				"error": "this storage driver cannot pause queues",
			})
			return
		}
		name := r.PathValue("name")

		var err error
		if paused {
			err = q.PauseQueue(r.Context(), name)
		} else {
			err = q.ResumeQueue(r.Context(), name)
		}
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"paused": paused})
	}
}

// triggerSchedule runs a recurring job now, without disturbing its schedule.
//
// The job gets a fresh id rather than the scheduler's deterministic (schedule,
// tick) one. That is deliberate: the deterministic id exists to make a retried
// tick idempotent, but an operator pressing "Trigger" is asking for an EXTRA
// run, and reusing the id would silently do nothing if that tick had already
// fired.
func (a *apiServer) triggerSchedule(w http.ResponseWriter, r *http.Request) {
	s, ok := a.store.(Schedules)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error": "this storage driver stores no schedules",
		})
		return
	}
	ctx := r.Context()
	id := r.PathValue("id")

	list, err := s.ListSchedules(ctx)
	if err != nil {
		writeError(w, err)
		return
	}
	for _, sc := range list {
		if sc.ID != id {
			continue
		}
		now := time.Now()
		job := &Job{
			ID: newID(), Kind: sc.Kind, Queue: sc.Queue, Payload: sc.Payload,
			MaxAttempts: 10, State: StateEnqueued,
			CreatedAt: now, ScheduledAt: now.Add(-time.Second),
			Tags: []string{"schedule:" + sc.ID, "manual"},
		}
		if err := a.store.Enqueue(ctx, []*Job{job}); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"job": job.ID})
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "no schedule " + id})
}

// storageName renders "*postgres.Storage" as "postgres" for the footer.
func storageName(s Storage) string {
	name := fmt.Sprintf("%T", s)
	name = strings.TrimPrefix(name, "*")
	if i := strings.LastIndex(name, "."); i > 0 {
		name = name[:i]
	}
	return name
}

func isQueueControl(s Storage) bool {
	_, ok := s.(QueueControl)
	return ok
}

func isSchedules(s Storage) bool {
	_, ok := s.(Schedules)
	return ok
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// writeError returns the storage's message. The dashboard is an operator tool
// behind an Authorizer, and an operator debugging a stuck queue needs the real
// error, not "internal server error".
func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

func splitParam(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func intParam(r *http.Request, name string, def int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil || n < 0 {
		return def
	}
	return n
}

func durationParam(r *http.Request, name string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(r.URL.Query().Get(name))
	if err != nil || d <= 0 {
		return def
	}
	return d
}
