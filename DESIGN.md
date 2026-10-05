# jobsync

A Hangfire-style background job system for Go with genuinely pluggable storage.

## Why

| | River | asynq | here |
|---|---|---|---|
| Storage | Postgres only | Redis only | Postgres, MySQL, Redis, + third-party |
| Transactional enqueue | yes | no | yes, where the engine allows it |
| Dashboard | per-project | Asynqmon, separate binary | one `http.Handler`, same UI on every driver |
| Declaring a job | args type + `Kind()` + worker struct + `AddWorker` | handler func + string task type + mux | one `Declare`, one `Handle` |

## The storage contract

`Storage` is five methods: `Enqueue`, `Fetch`, `Extend`, `Finish`, `Reclaim`.

Everything else is an optional interface found by type assertion — `Locker`,
`Schedules`, `TxEnqueuer`, `Monitor`. A driver implementing only `Storage` runs
jobs correctly; it just lights up fewer dashboard panels.

**The contract speaks jobs, queues, leases and schedules — never sets, hashes,
lists or counters.** This is the deliberate break from Hangfire, whose
`IStorageConnection` exposes Redis data structures and therefore forces every
SQL storage into a pile of emulation tables (`Hash`, `Set`, `List`, `Counter`).
That is why writing a Hangfire storage is a month of work and why the SQL ones
are slow. Here each driver answers domain questions however its engine is good
at answering them.

### Delivery semantics

At-least-once, lease-based. There is no exactly-once, and the docs will say so
rather than implying otherwise:

- `Fetch` atomically claims and sets a lease. Two servers never get the same job.
- The server heartbeats via `Extend` while the handler runs.
- A crashed server's jobs are returned to the queue by any server's `Reclaim`.
- `Finish` from a server whose lease already expired is **ignored, not an error**
  — the job may be running elsewhere, and a late result must not clobber it.

The last one is the rule new drivers get wrong, so it is a conformance test.

## Per-driver notes

**Postgres.** `SELECT ... FOR UPDATE SKIP LOCKED` for `Fetch`, a partial index on
`(queue, priority, scheduled_at) WHERE state IN ('enqueued','scheduled')`.
`TxEnqueuer` is natural. The one to build first — it is the reference driver.

**MySQL.** Supported floor is **MySQL 8.0 / MariaDB 10.6**, for `SKIP LOCKED`.
No claim-by-`UPDATE ... LIMIT` fallback for 5.7: it would roughly double the
driver and is slower under contention. This belongs in the README as a
supported-versions line, and `Fetch` should fail at startup with a clear message
on an older server rather than silently thrashing. MySQL is the gap nothing in
the Go ecosystem fills today, so it is the differentiator worth getting right.

**Redis.** `Fetch` is a Lua script: pop from the queue list, write the lease into
a sorted set scored by expiry, in one round trip. `Reclaim` is
`ZRANGEBYSCORE leases -inf now`. No `TxEnqueuer`; `EnqueueTx` fails loudly rather
than silently writing outside the caller's transaction. Job history for the
dashboard needs an explicit retention policy (expiring keys) since there is no
cheap `DELETE ... WHERE finished_at < ?`.

## Dashboard

One line to mount, same UI on every driver, because it talks only to `Monitor`:

```go
mux.Handle("/jobs/", jobsync.Dashboard(store, jobsync.DashboardConfig{
    BasePath: "/jobs",
    Auth:     jobsync.BasicAuth(user, pass),
}))
```

Assets embedded with `go:embed`; no separate binary, no CDN, no node build in
the consumer's pipeline.

`Auth` defaults to `LoopbackOnly`: nothing to configure on a laptop, and a
deployed dashboard is not silently world-readable. Hangfire's best-known footgun
is an accidentally public dashboard, and requiring explicit config would have
been answered by everyone pasting `AllowAll` from a blog post.

One subtlety the default must handle: behind a reverse proxy, `RemoteAddr` is
usually the proxy, and the proxy is usually on loopback — so a naive loopback
check is wide open to the internet. `LoopbackOnly` therefore refuses any request
carrying `X-Forwarded-For`, `X-Real-IP` or `Forwarded`. Running behind a proxy
means declaring a real `Authorizer`:

- `BasicAuth(user, pass)` — constant-time over SHA-256 digests, sends the
  `WWW-Authenticate` challenge so browsers prompt.
- `AuthorizeFunc(check, onDeny)` — adapts existing session/SSO middleware.
- `AllowAll` — named explicitly, so an open dashboard is always a decision
  somebody made, and is greppable in review.

## Open decisions

1. **Continuations and batches.** Hangfire's `ContinueJobWith` needs a parent ref
   and a completion check in the contract. Deferred to v2 so v1's contract stays
   at five methods; adding it later is additive.
2. **No global default client.** Hangfire's `BackgroundJob.Enqueue(...)` is a
   static call over a mutable global. Here the client is passed explicitly. Easy
   to add later, impossible to remove.
3. **pgx.** `TxEnqueuer` takes a `database/sql`-shaped `Tx`. Native pgx users go
   through `pgx/stdlib`, or the Postgres driver exposes `EnqueueTxPgx`.
4. **Name.** Settled: module and package `jobsync`. ("Hangfire" is a
   trademarked commercial product.)

## Build order

1. Postgres driver + conformance suite green. ← proves the contract
2. Redis driver. ← proves the contract is not Postgres-shaped
3. Dashboard against both.
4. MySQL driver. ← should be boring by now; if it isn't, the contract is wrong
5. Cron (`Locker` + `Schedules`), real cron parser in `cron.go`.
