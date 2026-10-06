package jobsync

import (
	"crypto/sha256"
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
)

// DashboardConfig configures the dashboard handler.
type DashboardConfig struct {
	// BasePath is where the dashboard is mounted, e.g. "/jobs". Links are built
	// relative to it, so it must match the mux pattern.
	BasePath string

	// Auth gates every request. Leaving it nil is the right thing on a laptop and
	// safe on a server: the dashboard then serves loopback clients only. Set it to
	// BasicAuth for a deployment, or AllowAll to deliberately open it up.
	Auth Authorizer
}

// Authorizer reports whether r may see the dashboard. It takes the
// ResponseWriter so an implementation can send a challenge — BasicAuth needs to
// write WWW-Authenticate or the browser never prompts. When it returns false it
// owns the response; the dashboard writes nothing further.
type Authorizer func(w http.ResponseWriter, r *http.Request) bool

// Dashboard serves the UI and its JSON API. It needs a driver implementing
// Monitor; with any other driver it reports that this storage has no
// introspection rather than failing at startup, so a dashboard route does not
// stop an application from booting.
func Dashboard(store Storage, cfg DashboardConfig) http.Handler {
	if cfg.Auth == nil {
		cfg.Auth = LoopbackOnly
	}
	api := &apiServer{store: store, base: strings.TrimSuffix(cfg.BasePath, "/")}
	mux := api.routes()
	mux.Handle("/", uiHandler(api.base))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !cfg.Auth(w, r) {
			return
		}
		// Strip the mount point so the routes above are written against a fixed
		// shape regardless of where the dashboard is mounted.
		http.StripPrefix(api.base, mux).ServeHTTP(w, r)
	})
}

// LoopbackOnly admits requests that originate on this machine. It is the default
// Authorizer: a dashboard needs no configuration during development and is not
// silently world-readable once deployed.
//
// A forwarding header means a proxy is in front, and the proxy itself is very
// often on loopback — which would otherwise make this wide open to the internet.
// Such requests are refused: a dashboard behind a reverse proxy must declare a
// real Authorizer.
func LoopbackOnly(w http.ResponseWriter, r *http.Request) bool {
	forwarded := r.Header.Get("X-Forwarded-For") != "" ||
		r.Header.Get("X-Real-Ip") != "" ||
		r.Header.Get("Forwarded") != ""

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)

	if forwarded || ip == nil || !ip.IsLoopback() {
		http.Error(w, "jobsync: dashboard is restricted to loopback; set DashboardConfig.Auth", http.StatusForbidden)
		return false
	}
	return true
}

// BasicAuth gates the dashboard on HTTP basic credentials. Comparison is
// constant-time over digests, so neither the value nor the length of the real
// credentials leaks through response timing.
func BasicAuth(username, password string) Authorizer {
	wantUser, wantPass := sha256.Sum256([]byte(username)), sha256.Sum256([]byte(password))

	return func(w http.ResponseWriter, r *http.Request) bool {
		user, pass, ok := r.BasicAuth()
		if ok {
			gotUser, gotPass := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(pass))
			if subtle.ConstantTimeCompare(gotUser[:], wantUser[:])&
				subtle.ConstantTimeCompare(gotPass[:], wantPass[:]) == 1 {
				return true
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="jobsync", charset="UTF-8"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
}

// AllowAll disables authorization. Name it explicitly in the call so that an
// open dashboard is always a decision somebody made, and is greppable.
func AllowAll(http.ResponseWriter, *http.Request) bool { return true }

// AuthorizeFunc adapts an existing session or SSO check, e.g. one that reads a
// cookie and redirects to a login page.
func AuthorizeFunc(fn func(*http.Request) bool, onDeny http.Handler) Authorizer {
	return func(w http.ResponseWriter, r *http.Request) bool {
		if fn(r) {
			return true
		}
		onDeny.ServeHTTP(w, r)
		return false
	}
}
