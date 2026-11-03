// Package node contains shared HTTP helper utilities used by both master and slave.
package node

import (
	"encoding/json"
	"net/http"

	"distributed-db/models"
)

// writeJSON writes any value as a JSON response with status 200.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeSuccess sends a successful response envelope.
func writeSuccess(w http.ResponseWriter, message string, data interface{}) {
	writeJSON(w, http.StatusOK, models.Response{Success: true, Message: message, Data: data})
}

// writeError sends a failure response envelope with HTTP 400.
func writeError(w http.ResponseWriter, message string) {
	writeJSON(w, http.StatusBadRequest, models.Response{Success: false, Message: message})
}

// WithCORS wraps an http.Handler to add CORS headers.
func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

