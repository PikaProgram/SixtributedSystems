package main

import (
	"net/http"
)

// auditToken guards /internal/audit/* with a bearer token (Lampiran A).
func auditToken(expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" || r.Header.Get("Authorization") != "Bearer "+expected {
			w.Header().Set("WWW-Authenticate", "Bearer")
			lampiranError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		next.ServeHTTP(w, r)
	})
}
