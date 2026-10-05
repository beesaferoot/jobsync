# jobsync

Background jobs for Go, with a Hangfire-style API, **pluggable storage**, and a
unified dashboard that looks the same whichever driver you run.

```go
type WelcomeArgs struct {
    UserID int    `json:"user_id"`
    Email  string `json:"email"`
}

// Declare once, at package level. Carries no dependencies, so anything
// that enqueues can reference it.
var SendWelcome = jobsync.Declare[WelcomeArgs]("email.welcome")
```

```go
// Enqueue side — needs only a Client.
SendWelcome.Enqueue(ctx, client, WelcomeArgs{UserID: 7, Email: "a@b.com"})

// Handler side — bound at wiring time, where dependencies exist.
SendWelcome.Handle(srv, func(ctx context.Context, a WelcomeArgs) error {
    return mailer.Send(ctx, a.Email)
})
```

No `Kind()` method on your args type, no worker struct, no separate registry
call. Dependencies come from closure capture, which is what Go has instead of an
IoC container.

## Install

```sh
go get github.com/beesaferoot/jobsync
```

Go 1.22+. Pick a driver: `driver/postgres`, `driver/mysql`, `driver/redis`, or
`driver/memory` for tests.

## A complete program

```go
package main

import (
    "context"
    "log"
    "net/http"

    "github.com/beesaferoot/jobsync"
    "github.com/beesaferoot/jobsync/driver/postgres"
)

type WelcomeArgs struct{ Email string `json:"email"` }

var SendWelcome = jobsync.Declare[WelcomeArgs]("email.welcome")

func main() {
    ctx := context.Background()

    // Open connects, migrates and is ready. No migration CLI step.
    store, err := postgres.Open(ctx, "postgres://user:pass@localhost/app?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    client, err := jobsync.NewClient(store)
    if err != nil {
        log.Fatal(err)
    }

    srv := jobsync.NewServer(store, jobsync.ServerConfig{
        Queues:      []string{"default"},
        Concurrency: 20,
    })
    SendWelcome.Handle(srv, func(ctx context.Context, a WelcomeArgs) error {
        return sendMail(ctx, a.Email)
    })
    go srv.Run(ctx)

    SendWelcome.Enqueue(ctx, client, WelcomeArgs{Email: "new@user.com"})

    http.Handle("/jobs/", jobsync.Dashboard(store, jobsync.DashboardConfig{
        BasePath: "/jobs",
    }))
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

## Enqueueing

```go
SendWelcome.Enqueue(ctx, client, args)                          // now
SendWelcome.Schedule(ctx, client, args, time.Now().Add(time.Hour))
SendWelcome.Cron(ctx, client, "nightly", "0 2 * * *", args)      // recurring
SendWelcome.EnqueueTx(ctx, client, tx, args)                     // inside your transaction
```

Options, all optional:

```go
SendWelcome.Enqueue(ctx, client, args,
    jobsync.Queue("mail"),
    jobsync.Priority(0),                     // lower runs first
    jobsync.MaxAttempts(5),
    jobsync.In(5*time.Minute),
    jobsync.Tags("billing"),
    jobsync.Unique("report:2026-10-05"),     // drop if one is already pending
)
```

**Transactional enqueue** (`EnqueueTx`) makes a job and the rows that justify it
commit together, so a job can never reference a row that rolled back. SQL drivers
only; it returns an error on Redis rather than silently writing outside your
transaction.

**Recurring jobs** need an explicit timezone. The default is UTC, and
`time.Local` is rejected: a schedule stores its zone *by name*, and `time.Local`
is named `"Local"` — which resolves to a different zone on every machine, so one
fleet would fire the same nightly job at several different hours.

## Dashboard

```go
mux.Handle("/jobs/", jobsync.Dashboard(store, jobsync.DashboardConfig{
    BasePath: "/jobs",
    Auth:     jobsync.BasicAuth(user, pass),
}))
```

One embedded HTML file — no CDN, no build step, works air-gapped. Overview with
a throughput graph, a filterable job list, job detail with state history, queues,
recurring jobs, and live servers.

**`Auth` defaults to `LoopbackOnly`**: nothing to configure on a laptop, and not
world-readable if it ships. Behind a reverse proxy you must set it, because
`LoopbackOnly` refuses any request carrying `X-Forwarded-For` — a proxy is
usually itself on loopback, so a naive loopback check is wide open. Use
`BasicAuth`, `AuthorizeFunc` to wrap existing session middleware, or `AllowAll`
to open it deliberately.

## Drivers

| | Postgres | MySQL | Redis | memory |
|---|---|---|---|---|
| Jobs, retries, scheduling | ✅ | ✅ | ✅ | ✅ |
| Recurring jobs (`Locker`, `Schedules`) | ✅ | ✅ | ✅ | ✅ |
| Dashboard (`Monitor`) | ✅ | ✅ | ✅ | ✅ |
| Queue pausing (`QueueControl`) | ✅ | ✅ | ✅ | ✅ |
| Transactional enqueue (`TxEnqueuer`) | ✅ | ✅ | — | — |
| Free-text search | ✅ | ✅ | — | ✅ |

**MySQL requires 8.0+ / MariaDB 10.6+** for `SELECT ... FOR UPDATE SKIP LOCKED`.
`Open` refuses older servers rather than failing on every poll.

**Redis** declines `Filter.Search` with `ErrUnsupportedFilter`; the dashboard
reads that and hides the search box rather than returning unfiltered results.
Priority is clamped to 0–9 — a sorted set score encodes one dimension, so
ordering by priority while filtering by due time needs a structure per level.

`memory` is not for production. It exists as the control for the conformance
suite, and it is useful in application tests where a real queue is more
infrastructure than the test needed.

## Semantics worth knowing

**At-least-once, lease-based.** A job claimed by a server that dies is returned
to its queue by any other server once the lease expires. Handlers should be
idempotent; there is no exactly-once and the docs will not pretend otherwise.

**Due-ness uses the storage's clock**, never the calling process's — it is the
only clock every server shares. A job enqueued with `ScheduledAt = time.Now()` on
a host running ahead of the database waits out the skew, then the next poll.

**A duplicate job ID is ignored, not overwritten.** This is what makes the
scheduler idempotent: a server crashing between enqueuing a tick and recording it
refires the same tick as a no-op.

**Retention.** SQL drivers keep terminal jobs for `Retention` (7 days default) —
run `store.Janitor(ctx, time.Hour)` to enforce it. Redis sets a TTL instead and
needs no janitor. Throughput history is bounded by whatever retention you run.

## Writing a driver

`Storage` is five methods: `Enqueue`, `Fetch`, `Extend`, `Finish`, `Reclaim`.
Everything else — `Locker`, `Schedules`, `Monitor`, `QueueControl`,
`TxEnqueuer`, `Heartbeat` — is an optional interface found by type assertion. A
driver implementing only `Storage` runs jobs correctly; it just lights up fewer
dashboard panels.

The contract speaks **jobs, queues, leases and schedules — never sets, hashes or
counters**. That is the deliberate break from Hangfire, whose storage API exposes
Redis data structures and therefore forces every SQL driver into emulation tables.

Prove a driver with the conformance suite:

```go
func TestConformance(t *testing.T)  { storagetest.Run(t, newStore) }
func TestDashboardAPI(t *testing.T) { storagetest.RunAPI(t, newStore) }
```

It pins the parts that are invisible until production: that `Fetch` is atomic
across processes, that a late `Finish` from a server whose lease expired is
ignored rather than applied, that a terminal job releases its unique key, and
that timestamps survive a non-UTC session.

## Development

```sh
docker compose up -d          # postgres, redis, mysql
go test -race -count=5 ./...  # -count catches the clock- and lease-boundary races
go run ./examples/dashboard   # demo app at http://127.0.0.1:8787/jobs/
```

`STORAGE=postgres|mysql|redis` picks the driver for the example.

## Status

Pre-1.0: the `Storage` contract is settled and covered by the conformance suite,
but it may still gain optional interfaces. Not yet implemented: continuations and
batches, and batched fetch (`Fetch` claims one job per round trip).

See `DESIGN.md` for the architecture and the reasoning behind the storage
contract.
