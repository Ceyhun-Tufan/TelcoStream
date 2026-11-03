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

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	log.Println("Starting Ingestion Gateway Service...")

	// 1. Kafka Producer'ı başlat
	kafkaBrokers := []string{"localhost:9092"}
	kafkaTopic := "cdr-events"
	producer := kafka.NewProducer(kafkaBrokers, kafkaTopic)
	defer producer.Close()

	// 2. Chi Router ve Middleware'ler
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)    // Her isteği loglar
	r.Use(middleware.Recoverer) // Panic durumunda sunucunun çökmesini engeller

	// 3. Rotalar
	cdrHandler := api.NewCDRHandler(producer)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"UP"}`))
	})
	r.Post("/api/v1/cdr", cdrHandler.IngestCDR)

	// 4. HTTP Sunucusunu Yapılandır
	server := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 5. Sunucuyu ayrı bir Goroutine'de başlat
	go func() {
		log.Printf("Gateway HTTP server listening on port %s\n", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// 6. Graceful Shutdown (SIGINT, SIGTERM dinleme)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit // Sinyal gelene kadar burada bloklanır
	log.Println("Shutdown signal received, shutting down gracefully...")

	// Kapanma için 5 saniyelik bir mühlet ver
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Gateway service stopped safely.")
}
