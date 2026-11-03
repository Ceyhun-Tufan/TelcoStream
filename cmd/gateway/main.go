package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"telco-stream/internal/api"
	"telco-stream/internal/kafka"
	"telco-stream/internal/ratelimit"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.Println("Starting Ingestion Gateway Service...")

	// Kafka
	kafkaBrokers := []string{"localhost:9092"}
	kafkaTopic := "cdr-events"
	producer := kafka.NewProducer(kafkaBrokers, kafkaTopic)
	defer producer.Close()

	// Redis
	//
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	luaScriptBytes, err := os.ReadFile("scripts/rate_limiter.lua")
	if err != nil {
		log.Fatalf("failed to read rate_limiter.lua: %v", err)
	}

	script := redis.NewScript(string(luaScriptBytes))

	// bir saniyede en fazla 1 istek
	limiter := ratelimit.NewLimiter(rdb, script, 5, 1*time.Second)

	// Route ve Middleware
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)    // Her isteği loglar
	r.Use(middleware.Recoverer) // Panic durumunda sunucunun çökmesini engeller

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"UP"}`))
	})

	cdrHandler := api.NewCDRHandler(producer)
	r.With(api.RateLimitMiddleware(limiter)).Post("/api/v1/cdr", cdrHandler.IngestCDR)

	// http server
	server := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// gracefully shutdown icin goroutine icerisinde baslatiyoruz tum sistemi
	go func() {
		log.Printf("Gateway HTTP server listening on port %s\n", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	//  Graceful Shutdown dinleme
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit // Sinyal gelene kadar burada bloklanır
	log.Println("Shutdown signal received, shutting down gracefully...")

	// Kapanma için 5 saniyelik muhlet
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Gateway service stopped safely.")
}
