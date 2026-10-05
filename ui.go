package jobsync

import (
	_ "embed"
	"net/http"
	"strings"
)

//go:embed ui/dashboard.html
var dashboardHTML string

// uiHandler serves the single-page dashboard.
//
// One embedded file, no CDN and no build step in the consumer's pipeline: the
// whole point of this dashboard is that mounting it is one line, and a project
// that has to run npm to see its own queues has lost that.
func uiHandler(base string) http.Handler {
	// The page needs to know where it was mounted to build API URLs. Injecting it
	// server-side beats having the script guess from location.pathname, which
	// breaks the moment the dashboard is behind a proxy that rewrites paths.
	page := strings.Replace(dashboardHTML,
		"<script>\nconst BASE",
		"<script>\nwindow.JOBSYNC_BASE = "+jsString(base)+";\nconst BASE", 1)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The page is rebuilt on every deploy and is tiny; caching it only makes
		// an operator stare at a stale dashboard after an upgrade.
		w.Header().Set("Cache-Control", "no-store")
		w.Write([]byte(page))
	})
}

// jsString quotes a Go string for embedding in a <script> block. It escapes the
// closing-tag sequence as well as the usual characters: "</script>" inside a
// string literal still ends the block as far as the HTML parser is concerned.
func jsString(s string) string {
	r := strings.NewReplacer(
		`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`,
		"<", `\x3c`, ">", `\x3e`, "&", `\x26`,
	)
	return `"` + r.Replace(s) + `"`
}
