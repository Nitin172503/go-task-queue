package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/nitinstp23/go-task-queue/internal/api"
	"github.com/nitinstp23/go-task-queue/internal/broker"
	"github.com/nitinstp23/go-task-queue/internal/worker"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" { addr = ":8080" }

	b := broker.NewMemoryBroker()
	pool := worker.NewPool(b, 5, 200*time.Millisecond)
	h := api.NewHandler(b, pool)

	r := mux.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			log.Printf("[http] %s %s", req.Method, req.URL.Path)
			next.ServeHTTP(w, req)
		})
	})
	h.RegisterRoutes(r)

	srv := &http.Server{Addr: addr, Handler: r, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	go pool.Start(ctx)

	go func() {
		log.Printf("[server] listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[server] %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[server] shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
	cancel()
}
