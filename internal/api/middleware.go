package api

import (
	"net/http"

	"telco-stream/internal/ratelimit"
)

// RateLimitMiddleware gelen istekleri abone veya IP bazında sınırlar.
func RateLimitMiddleware(limiter *ratelimit.Limiter) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// once telefon numarasi sonra ip,
			// ip isi daha sonra kaldirilabilir ne kadar mantikli emin degilim
			identifier := r.Header.Get("X-Subscriber-ID")
			if identifier == "" {
				identifier = r.RemoteAddr
			}

			// 2. Redis Lua motoruna sor
			allowed, err := limiter.Allow(r.Context(), identifier)
			if err != nil {
				// rediste bir sorun varsa loglayip kapatiyoruz
				http.Error(w, "rate limiter error: "+err.Error(), http.StatusInternalServerError)
				return
			}

			// limit kontrol
			if !allowed {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":"rate limit exceeded, please slow down"}`))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
