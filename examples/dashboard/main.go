// Command dashboard is a runnable jobsync example: a worker producing traffic,
// with the dashboard mounted over it.
//
//	docker compose up -d redis
//	go run ./examples/dashboard
//	open http://127.0.0.1:8787/jobs/
//
// Point STORAGE at postgres or mysql to see the same dashboard over a different
// driver — the UI is identical, minus whatever that driver declines to support.
// Redis is the default because it is the one that declines something (Search),
// so the capability handling is visible without changing anything.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/beesaferoot/jobsync"
	"github.com/beesaferoot/jobsync/driver/mysql"
	"github.com/beesaferoot/jobsync/driver/postgres"
	jobsyncredis "github.com/beesaferoot/jobsync/driver/redis"
)

type MailArgs struct {
	To string `json:"to"`
}

type InvoiceArgs struct {
	Invoice  int `json:"invoice"`
	Customer int `json:"customer"`
}

var (
	SendMail    = jobsync.Declare[MailArgs]("email.send")
	SendInvoice = jobsync.Declare[InvoiceArgs]("billing.invoice")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	store, err := open(ctx, env("STORAGE", "redis"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	client, err := jobsync.NewClient(store)
	if err != nil {
		log.Fatal(err)
	}

	srv := jobsync.NewServer(store, jobsync.ServerConfig{
		Queues:           []string{"default", "mail"},
		Concurrency:      4,
		PollInterval:     200 * time.Millisecond,
		ScheduleInterval: time.Second,
		// Seconds rather than the default exponential minutes, so a failing job
		// walks through Retrying to Dead while you watch.
		Backoff: func(attempt int) time.Duration { return time.Duration(attempt) * 2 * time.Second },
	})
	SendMail.Handle(srv, func(ctx context.Context, a MailArgs) error {
		time.Sleep(time.Duration(rand.IntN(500)) * time.Millisecond)
		if rand.IntN(5) == 0 {
			return errors.New("smtp: connection refused")
		}
		return nil
	})
	// The two failure modes worth seeing on the Failures tab: email fails
	// transiently and is retried until it runs out of attempts; billing fails
	// permanently, so retrying is skipped. Its message carries per-job IDs,
	// which the dashboard masks to group the failures as one cause.
	SendInvoice.Handle(srv, func(ctx context.Context, a InvoiceArgs) error {
		time.Sleep(time.Duration(rand.IntN(300)) * time.Millisecond)
		if a.Customer%7 == 0 {
			return fmt.Errorf("billing: invoice %d: customer %d has no payment method: %w",
				a.Invoice, a.Customer, jobsync.ErrPermanent)
		}
		return nil
	})
	go srv.Run(ctx)

	if err := SendMail.Cron(ctx, client, "digest", "*/5 * * * *", MailArgs{To: "digest@example.com"}); err != nil {
		log.Fatal(err)
	}
	go produce(ctx, client)

	// This is the whole dashboard setup. Auth is unset, so it serves loopback
	// only — nothing to configure on a laptop, and not world-readable if this
	// ever ran somewhere real.
	mux := http.NewServeMux()
	mux.Handle("/jobs/", jobsync.Dashboard(store, jobsync.DashboardConfig{
		BasePath: "/jobs",
	}))

	log.Println("dashboard: http://127.0.0.1:8787/jobs/")
	server := &http.Server{Addr: "127.0.0.1:8787", Handler: mux}
	go func() {
		<-ctx.Done()
		server.Close()
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// produce keeps the dashboard interesting: a trickle of work, some of it
// deliberately slow to arrive so the Scheduled tile is not empty, and some of it
// failing so Retrying, Dead and the Failures tab are not either.
func produce(ctx context.Context, c *jobsync.Client) {
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}

		queue := []string{"default", "mail"}[i%2]
		SendMail.Enqueue(ctx, c, MailArgs{To: fmt.Sprintf("user%d@example.com", i)},
			jobsync.Queue(queue), jobsync.Tags("demo"), jobsync.MaxAttempts(3))

		if i%3 == 0 {
			SendInvoice.Enqueue(ctx, c, InvoiceArgs{Invoice: 10000 + i, Customer: rand.IntN(500)},
				jobsync.Tags("demo"))
		}

		if i%10 == 0 {
			SendMail.Schedule(ctx, c, MailArgs{To: "later@example.com"},
				time.Now().Add(time.Duration(i%7+1)*time.Hour))
		}
	}
}

// open connects to the demo's own storage.
//
// Deliberately NOT the TEST_* variables the conformance suite uses, and Redis
// database 5 rather than 0. This example runs a real server that polls the
// default queue, so sharing a namespace with the suite means the demo claims the
// suite's jobs and the suite's FlushAll wipes the demo — which looks exactly
// like a driver bug and is not one.
func open(ctx context.Context, kind string) (jobsync.Storage, error) {
	switch kind {
	case "postgres":
		return postgres.Open(ctx, env("DEMO_POSTGRES_URL",
			"postgres://jobsync:jobsync@127.0.0.1:5433/jobsync_demo?sslmode=disable"))
	case "mysql":
		return mysql.Open(ctx, env("DEMO_MYSQL_DSN",
			"root:jobsync@tcp(127.0.0.1:3307)/jobsync?parseTime=true&loc=UTC"))
	case "redis":
		return jobsyncredis.Open(env("DEMO_REDIS_URL", "redis://127.0.0.1:6380/5"))
	}
	return nil, fmt.Errorf("unknown STORAGE %q: want postgres, mysql or redis", kind)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
