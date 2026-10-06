# jobsync

**Background jobs for Go that you can actually see.**

One library, one dashboard, and your choice of Postgres, MySQL or Redis — with
the same API and the same UI whichever you pick.

![The jobsync dashboard](docs/screenshots/overview.png)

```go
var SendWelcome = jobsync.Declare[WelcomeArgs]("email.welcome")

SendWelcome.Enqueue(ctx, client, WelcomeArgs{Email: "new@user.com"})
```

That's the whole enqueue path. The dashboard above is one line to mount.

---

## Why another job queue

Go has excellent job libraries — [River] on Postgres and [asynq] on Redis are
both worth your time, and if you are settled on one of those backends you should
look at them first.

jobsync is built around a storage contract rather than a database. The same API,
the same dashboard and the same guarantees run on Postgres, MySQL or Redis, and
you can move between them without rewriting your jobs.

| | jobsync |
|---|---|
| Storage | Postgres, MySQL, Redis, or your own driver |
| Dashboard | one `http.Handler` in your process, identical on every driver |
| Transactional enqueue | yes, on the engines that support it |
| Recurring jobs | cron with per-fleet locking, in the same library |

[River]: https://riverqueue.com
[asynq]: https://github.com/hibiken/asynq

**Running MySQL?** The MySQL driver is first-class here — same conformance
suite, same dashboard, same semantics as Postgres and Redis.

## Install

```sh
go get github.com/beesaferoot/jobsync
```

Go 1.22+. Pick a driver: `driver/postgres`, `driver/mysql`, `driver/redis`, or
`driver/memory` for tests.

## Thirty seconds

```go
store, _ := postgres.Open(ctx, os.Getenv("DATABASE_URL"))  // connects + migrates
client, _ := jobsync.NewClient(store)

srv := jobsync.NewServer(store, jobsync.ServerConfig{Concurrency: 20})
SendWelcome.Handle(srv, func(ctx context.Context, a WelcomeArgs) error {
    return mailer.Send(ctx, a.Email)
})
go srv.Run(ctx)

http.Handle("/jobs/", jobsync.Dashboard(store, jobsync.DashboardConfig{BasePath: "/jobs"}))
```

`Open` runs its own migrations, and the dashboard is a handler inside the
process you already deploy.

### The API, in full

```go
type WelcomeArgs struct {
    UserID int    `json:"user_id"`
    Email  string `json:"email"`
}

// Declared at package level. Carries no dependencies, so anything can enqueue it.
var SendWelcome = jobsync.Declare[WelcomeArgs]("email.welcome")

// The implementation is bound at wiring time, where dependencies exist.
SendWelcome.Handle(srv, func(ctx context.Context, a WelcomeArgs) error {
    return mailer.Send(ctx, a.Email)   // mailer captured here
})
```

`Declare` gives you a typed handle that any package can enqueue against.
`Handle` binds the implementation at wiring time, where your dependencies
already exist — so they arrive by closure capture, which is what Go offers in
place of a container.

<details>
<summary><b>A complete program</b></summary>

```go
package main

import (
    "context"
    "log"
    "net/http"

    "github.com/beesaferoot/jobsync"
    "github.com/beesaferoot/jobsync/driver/postgres"
)

type WelcomeArgs struct {
    Email string `json:"email"`
}

var SendWelcome = jobsync.Declare[WelcomeArgs]("email.welcome")

func main() {
    ctx := context.Background()

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

</details>

## The dashboard

Mount it on any `http.ServeMux` and you get the whole thing. It ships as a single
embedded HTML file, so it renders offline and stays out of your build pipeline.

**Find the job that broke.** Filter by state from the sidebar; the failure
message is in the table, so you don't have to open anything to see what went
wrong.

![Job list](docs/screenshots/jobs.png)

**Then open it.** Payload, attempt count, and a state timeline with `+Nms`
deltas — how long it waited, how long it ran, how long until the retry.

![Job detail](docs/screenshots/job-detail.png)

**Recurring jobs**, with next and last run, and a Trigger button for when
somebody asks whether the nightly report still works.

![Recurring jobs](docs/screenshots/recurring.png)

Also: queue pausing for when a downstream API is down and you'd rather not burn
ten thousand retries against it, and a live server list so you can see your
fleet during a deploy.

### Security, by default

`Auth` defaults to `LoopbackOnly`: nothing to configure on a laptop, and not
world-readable if it ships. Behind a reverse proxy you **must** set it, because
`LoopbackOnly` refuses any request carrying `X-Forwarded-For` — a proxy is
usually itself on loopback, so a naive loopback check is wide open.

```go
jobsync.Dashboard(store, jobsync.DashboardConfig{
    BasePath: "/jobs",
    Auth:     jobsync.BasicAuth(user, pass),
    // or: jobsync.AuthorizeFunc(yourSessionCheck, loginRedirect)
})
```

## Scheduling

```go
SendWelcome.Enqueue(ctx, client, args)                             // now
SendWelcome.Schedule(ctx, client, args, time.Now().Add(time.Hour)) // later
SendWelcome.Cron(ctx, client, "nightly", "0 2 * * *", args)        // recurring
SendWelcome.EnqueueTx(ctx, client, tx, args)                       // in your transaction
```

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

**`EnqueueTx`** makes a job and the rows that justify it commit together, so a
job can never reference a row that rolled back. SQL drivers only; it errors on
Redis rather than silently writing outside your transaction.

**Cron** runs once per fleet, not once per server — one server wins a lock per
tick. Recurring jobs need an explicit timezone; `time.Local` is rejected,
because a schedule stores its zone *by name* and `"Local"` means something
different on every machine.

## Drivers

| | Postgres | MySQL | Redis | memory |
|---|---|---|---|---|
| Jobs, retries, scheduling | ✅ | ✅ | ✅ | ✅ |
| Recurring jobs | ✅ | ✅ | ✅ | ✅ |
| Dashboard | ✅ | ✅ | ✅ | ✅ |
| Queue pausing | ✅ | ✅ | ✅ | ✅ |
| Transactional enqueue | ✅ | ✅ | — | — |
| Free-text search | ✅ | ✅ | — | ✅ |

**MySQL requires 8.0+ / MariaDB 10.6+** for `SELECT ... FOR UPDATE SKIP LOCKED`.
`Open` refuses older servers rather than failing on every poll.

**Redis** declines free-text search; the dashboard reads that capability and
hides the search box rather than returning unfiltered results. Priority is
clamped to 0–9.

`memory` is not for production. It is the control for the conformance suite, and
it is useful in application tests where a real queue is more infrastructure than
the test needed.

## Semantics worth knowing

**At-least-once, lease-based.** A job claimed by a server that dies returns to
its queue once the lease expires. Handlers should be idempotent; there is no
exactly-once and the docs will not pretend otherwise.

**Due-ness uses the storage's clock**, never the calling process's — it's the
only clock every server shares.

**A permanent failure skips its remaining attempts.** Return an error wrapping
`jobsync.ErrPermanent` and the job goes straight to dead. A payload that will not
decode is treated this way automatically — it would not decode on the tenth
attempt either, and the retries bury real failures under noise.

**A duplicate job ID is ignored, not overwritten.** This is what makes the
scheduler idempotent: a server crashing between enqueuing a tick and recording
it refires the same tick as a no-op.

**Retention.** SQL drivers keep terminal jobs for `Retention` (7 days default) —
run `store.Janitor(ctx, time.Hour)` to enforce it. Redis uses a TTL instead.

## Bring your own storage

`Storage` is five methods: `Enqueue`, `Fetch`, `Extend`, `Finish`, `Reclaim`.
Everything else — `Locker`, `Schedules`, `Monitor`, `QueueControl`,
`TxEnqueuer`, `Heartbeat` — is an optional interface found by type assertion. A
driver implementing only `Storage` runs jobs correctly; it just lights up fewer
dashboard panels.

The contract speaks **jobs, queues, leases and schedules — never sets, hashes or
counters**. That's the deliberate break from Hangfire, whose storage API exposes
Redis data structures and so forces every SQL driver into emulation tables.

Prove your driver with the suite the built-in ones are held to:

```go
func TestConformance(t *testing.T)  { storagetest.Run(t, newStore) }
func TestDashboardAPI(t *testing.T) { storagetest.RunAPI(t, newStore) }
```

It pins the things that are invisible until production: that `Fetch` is atomic
across processes, that a late `Finish` from a server whose lease expired is
ignored rather than applied, that a terminal job releases its unique key, and
that timestamps survive a non-UTC session.

## Try it

```sh
docker compose up -d          # postgres, redis, mysql
go run ./examples/dashboard   # http://127.0.0.1:8787/jobs/
```

`STORAGE=postgres|mysql|redis` switches the driver — the UI is identical, minus
whatever that driver declines to support.

```sh
go test -race -count=5 ./...  # -count catches the clock- and lease-boundary races
```

## Status

Pre-1.0. The `Storage` contract is settled and covered by the conformance suite,
but it may still gain optional interfaces. Not yet implemented: continuations
and batches, and batched fetch (`Fetch` claims one job per round trip).

See `DESIGN.md` for the architecture and the reasoning behind the storage
contract.

## License

MIT — see [LICENSE](LICENSE).
