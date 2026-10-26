package nodeagent

import (
	"bytes"
	"encoding/json"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/inferroute"
)

const maxInferenceRequestBytes = 20 << 20

func (a *Agent) handleTransparentProxy(w http.ResponseWriter, r *http.Request) {
	routePath := "/" + strings.TrimPrefix(r.PathValue("path"), "/")
	route, ok := inferroute.Lookup(r.Method, routePath)
	if !ok {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxInferenceRequestBytes))
	if err != nil {
		http.Error(w, "request body is too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	var request struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		http.Error(w, "invalid inference request JSON", http.StatusBadRequest)
		return
	}

	engineName := r.Header.Get(inferroute.HeaderEngine)
	eng := a.engineForModel(request.Model, engineName)
	if eng == nil || !engine.SupportsProtocol(eng.Name(), string(route.Protocol)) {
		http.Error(w, "selected model or protocol is unavailable on this node", http.StatusNotFound)
		return
	}
	target, err := url.Parse(engineURL(eng))
	if err != nil || target.Scheme == "" || target.Host == "" {
		http.Error(w, "selected engine has no valid endpoint", http.StatusServiceUnavailable)
		return
	}
	target.Path = strings.TrimRight(target.Path, "/") + route.Path
	target.RawPath = ""
	upstreamTarget := *target

	proxy := &httputil.ReverseProxy{
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
			logging.Errorf("nodeagent", "nodeagent: transparent inference proxy failed engine=%s: %v", eng.Name(), proxyErr)
			http.Error(w, "inference engine is unavailable", http.StatusBadGateway)
		},
		Rewrite: func(pr *httputil.ProxyRequest) {
			targetURL := upstreamTarget
			targetURL.RawQuery = pr.In.URL.RawQuery
			pr.Out.URL = &targetURL
			pr.Out.Host = upstreamTarget.Host
			pr.Out.Header.Del(inferroute.HeaderEngine)
			pr.Out.Header.Del(inferroute.HeaderRequestID)
			pr.Out.Header.Del(inferroute.HeaderAttemptID)
			preserveForwardingHeaders(pr)
			pr.Out.Body = io.NopCloser(bytes.NewReader(body))
			pr.Out.ContentLength = int64(len(body))
		},
	}
	proxy.ServeHTTP(w, r)
}

var forwardingHeaders = [...]string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"}

// preserveForwardingHeaders keeps the headers ReverseProxy forwarded before
// the move from Director to Rewrite: Rewrite strips the client's forwarding
// headers, whereas Director passed them through and appended the client IP to
// X-Forwarded-For.
func preserveForwardingHeaders(pr *httputil.ProxyRequest) {
	for _, name := range forwardingHeaders {
		if values, ok := pr.In.Header[name]; ok {
			pr.Out.Header[name] = slices.Clone(values)
		}
	}
	if clientIP, _, err := net.SplitHostPort(pr.In.RemoteAddr); err == nil {
		if prior := pr.Out.Header["X-Forwarded-For"]; len(prior) > 0 {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		pr.Out.Header.Set("X-Forwarded-For", clientIP)
	}
}
