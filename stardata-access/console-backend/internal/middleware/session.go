package middleware

import (
	"context"
	"log/slog"
	"net/http"

	"stardata-console-backend/internal/auth"
)

type contextKey string

const SessionKey contextKey = "session"

// RequireSession is HTTP middleware that validates the session cookie.
// Valid sessions are stored in context; invalid/missing sessions return 401.
func RequireSession(store *auth.ValkeyStore, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cook, err := r.Cookie("__Host-stardata_session")
			if err != nil {
				http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
				return
			}
			sess, err := store.Get(r.Context(), cook.Value)
			if err != nil {
				log.Error("session lookup error", "err", err)
				http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
				return
			}
			if sess == nil {
				http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), SessionKey, sess)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// SessionFromContext retrieves the session from context (set by RequireSession).
func SessionFromContext(ctx context.Context) *auth.Session {
	s, _ := ctx.Value(SessionKey).(*auth.Session)
	return s
}
