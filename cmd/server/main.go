package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BenMeehan/paytm-wallet/internal/api"
	"github.com/BenMeehan/paytm-wallet/internal/logs"
	"github.com/BenMeehan/paytm-wallet/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	var st *store.Store
	var err error
	for attempt := 1; ; attempt++ {
		st, err = store.New(ctx, dsn)
		if err == nil {
			break
		}
		if attempt >= 12 {
			panic("connect store: " + err.Error())
		}
		time.Sleep(5 * time.Second)
	}
	defer st.Close()

	lg := logs.New(os.Getenv("LOGTAIL_SOURCE_TOKEN"), os.Getenv("LOGTAIL_URL"))
	defer lg.Close()

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           api.New(st, lg).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		lg.Event("boot", "server_started", "wallet service listening",
			map[string]any{"port": port})
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic("listen: " + err.Error())
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}
