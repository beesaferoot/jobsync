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

var SendMail = jobsync.Declare[MailArgs]("email.send")

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
	})
	SendMail.Handle(srv, func(ctx context.Context, a MailArgs) error {
		time.Sleep(time.Duration(rand.IntN(500)) * time.Millisecond)
		if rand.IntN(5) == 0 {
			return errors.New("smtp: connection refused")
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
// deliberately slow to arrive so the Scheduled and Retrying tiles are not empty.
func produce(ctx context.Context, c *jobsync.Client) {
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}

		queue := []string{"default", "mail"}[i%2]
		SendMail.Enqueue(ctx, c, MailArgs{To: fmt.Sprintf("user%d@example.com", i)},
			jobsync.Queue(queue), jobsync.Tags("demo"))

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
