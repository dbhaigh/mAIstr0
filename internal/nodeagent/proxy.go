package nodeagent

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
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

	proxy := httputil.NewSingleHostReverseProxy(&upstreamTarget)
	proxy.FlushInterval = -1
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
		log.Printf("nodeagent: transparent inference proxy failed engine=%s: %v", eng.Name(), proxyErr)
		http.Error(w, "inference engine is unavailable", http.StatusBadGateway)
	}
	proxy.Director = func(out *http.Request) {
		out.Method = r.Method
		targetURL := upstreamTarget
		out.URL = &targetURL
		out.URL.RawQuery = r.URL.RawQuery
		out.Host = upstreamTarget.Host
		out.Header = r.Header.Clone()
		out.Header.Del(inferroute.HeaderEngine)
		out.Header.Del(inferroute.HeaderRequestID)
		out.Header.Del(inferroute.HeaderAttemptID)
		out.Body = io.NopCloser(bytes.NewReader(body))
		out.ContentLength = int64(len(body))
	}
	proxy.ServeHTTP(w, r)
}
