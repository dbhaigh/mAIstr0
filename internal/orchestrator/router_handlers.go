package orchestrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/workpool"
)

const (
	routerRequestTimeout  = time.Duration(config.DefaultRouterRequestTimeoutSeconds) * time.Second
	routerMaxAttempts     = 5
	routerMaxErrorBytes   = 1 << 20
	maxRouterRequestBytes = 20 << 20
)

var routerStreamBufferPool = sync.Pool{
	New: func() any { return make([]byte, 32*1024) },
}

func (s *Server) handleRouterEvents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"events":           s.routerEvents.snapshot(),
		"circuit_breakers": s.routerCircuitBreakerSnapshots(),
	})
}

func (s *Server) handleRouterRequest(w http.ResponseWriter, r *http.Request) {
	routePath := "/" + strings.TrimPrefix(r.PathValue("path"), "/")
	route, ok := inferroute.Lookup(r.Method, routePath)
	if !ok {
		http.NotFound(w, r)
		return
	}
	requestID, err := newRouterRequestID()
	if err != nil {
		log.Printf("orchestrator: generate router request ID failed: %v", err)
		writeRouterError(w, http.StatusInternalServerError, "could not initialize request")
		return
	}
	w.Header().Set("X-Request-ID", requestID)
	startedAt := time.Now()
	defer func() {
		s.metrics.observeRouterRequest(time.Since(startedAt))
	}()

	limit := s.routerClients.acquire(r)
	if limit.Reason != "" {
		status := http.StatusTooManyRequests
		failureClass := "client_rate_limit"
		message := "per-client request rate limit exceeded"
		if limit.Reason == "concurrency" {
			failureClass = "client_concurrency_limit"
			message = "per-client concurrency limit exceeded"
		} else if limit.Reason == "capacity" {
			status = http.StatusServiceUnavailable
			failureClass = "client_limit_capacity"
			message = "client limit capacity is temporarily unavailable"
		}
		w.Header().Set("Retry-After", fmt.Sprintf("%d", limit.RetryAfter))
		s.recordRouterEvent(routerRequestEvent{
			RequestID: requestID, Phase: "rejected", Timestamp: time.Now(),
			Status: status, FailureClass: failureClass,
		})
		writeRouterError(w, status, message)
		return
	}
	defer limit.Release()

	requestBody, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRouterRequestBytes))
	if err != nil {
		writeRouterError(w, http.StatusRequestEntityTooLarge, "request body is too large or unreadable")
		s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "rejected", Timestamp: time.Now(), FailureClass: "invalid_request"})
		return
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(requestBody, &payload); err != nil || payload == nil {
		writeRouterError(w, http.StatusBadRequest, "invalid inference request JSON")
		s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "rejected", Timestamp: time.Now(), FailureClass: "invalid_request"})
		return
	}
	preferredModel := ""
	if rawModel, found := payload["model"]; found {
		if err := json.Unmarshal(rawModel, &preferredModel); err != nil {
			writeRouterError(w, http.StatusBadRequest, "model must be a string")
			s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "rejected", Timestamp: time.Now(), FailureClass: "invalid_request"})
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), routerRequestTimeout)
	defer cancel()
	tried := make(map[string]bool)
	backoff := 100 * time.Millisecond
	dispatchedAttempts := 0
	lastAttemptFailureClass := ""
	for attemptNumber := 1; attemptNumber <= routerMaxAttempts; attemptNumber++ {
		if err := ctx.Err(); err != nil {
			failureClass := "timeout"
			if errors.Is(err, context.Canceled) {
				failureClass = "cancelled"
			}
			s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: failureClass})
			writeRouterContextError(w, r, ctx)
			return
		}
		assignment, assignErr := s.assignRouterRequest(preferredModel, route.Protocol, tried)
		if assignErr != nil {
			failureClass := "no_compatible_node"
			status := http.StatusServiceUnavailable
			if attemptNumber > 1 {
				failureClass = "attempts_exhausted"
				status = http.StatusBadGateway
				if lastAttemptFailureClass == "time_to_first_byte_timeout" {
					failureClass = lastAttemptFailureClass
					status = http.StatusGatewayTimeout
				}
			}
			s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: failureClass})
			writeRouterError(w, status, "no additional compatible inference node is available")
			return
		}
		pairKey := routerPairKey(assignment.NodeID, assignment.Model)
		if !s.routerBreakers.acquire(assignment.NodeID, assignment.Model) {
			tried[pairKey] = true
			continue
		}
		tried[pairKey] = true
		attemptID := fmt.Sprintf("%s:%d", requestID, attemptNumber)
		node, exists := s.registry.Get(assignment.NodeID)
		if !exists || !node.Healthy || node.Address != assignment.Address {
			s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
			lastAttemptFailureClass = "node_unavailable"
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: time.Since(startedAt).Milliseconds(),
				DurationMs:   time.Since(startedAt).Milliseconds(),
				FailureClass: "node_unavailable",
			})
			continue
		}

		s.metrics.changeRouterQueueDepth(1)
		reservation, err := s.dispatcherPort().reservations().Reserve(ctx, []workpool.CapacityTarget{{
			NodeID: assignment.NodeID, Address: assignment.Address,
		}})
		s.metrics.changeRouterQueueDepth(-1)
		if err != nil {
			s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
			if ctx.Err() != nil {
				continue
			}
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: time.Since(startedAt).Milliseconds(),
				DurationMs:   time.Since(startedAt).Milliseconds(),
				FailureClass: "reservation",
			})
			lastAttemptFailureClass = "reservation"
			continue
		}
		reservedNodeID := reservation.Target().NodeID
		s.metrics.recordCapacityReservation()
		queueMs := time.Since(startedAt).Milliseconds()
		s.registry.IncrementLoad(reservedNodeID, 1)
		s.recordRouterEvent(routerRequestEvent{
			RequestID: requestID, AttemptID: attemptID, Phase: "attempt_started",
			Timestamp: time.Now(), NodeID: reservedNodeID, Model: assignment.Model,
			Engine: assignment.Engine, QueueMs: queueMs,
		})

		body := requestBody
		if preferredModel == "" {
			payload["model"], _ = json.Marshal(assignment.Model)
			body, err = json.Marshal(payload)
			if err != nil {
				s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
				s.releaseRouterReservation(reservedNodeID, reservation)
				writeRouterError(w, http.StatusInternalServerError, "could not prepare inference request")
				return
			}
		}
		upstreamURL := strings.TrimRight(assignment.Address, "/") + "/proxy" + route.Path
		if r.URL.RawQuery != "" {
			upstreamURL += "?" + r.URL.RawQuery
		}
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		upstreamReq, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, upstreamURL, bytes.NewReader(body))
		if err != nil {
			attemptCancel()
			s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
			s.releaseRouterReservation(reservedNodeID, reservation)
			writeRouterError(w, http.StatusInternalServerError, "could not prepare inference request")
			return
		}
		upstreamReq.Header = r.Header.Clone()
		stripHopHeaders(upstreamReq.Header)
		upstreamReq.Header.Set(inferroute.HeaderEngine, assignment.Engine)
		upstreamReq.Header.Set(inferroute.HeaderRequestID, requestID)
		upstreamReq.Header.Set(inferroute.HeaderAttemptID, attemptID)
		attemptStarted := time.Now()
		if dispatchedAttempts > 0 {
			s.metrics.recordRetry()
		}
		dispatchedAttempts++
		s.metrics.recordDispatch()
		s.metrics.recordRouterAttempt()
		var ttfbExpired atomic.Bool
		ttfbTimer := time.AfterFunc(s.routerTTFBTimeout, func() {
			ttfbExpired.Store(true)
			attemptCancel()
		})
		resp, requestErr := s.nodeClientPort().DoDispatch(upstreamReq)

		if requestErr != nil {
			ttfbTimer.Stop()
			attemptCancel()
			s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
			s.releaseRouterReservation(reservedNodeID, reservation)
			failureClass := "transport"
			if ctx.Err() != nil {
				failureClass = "timeout"
				if errors.Is(ctx.Err(), context.Canceled) {
					failureClass = "cancelled"
				}
			} else if ttfbExpired.Load() {
				failureClass = "time_to_first_byte_timeout"
			} else if errors.Is(requestErr, context.DeadlineExceeded) {
				failureClass = "timeout"
			} else if errors.Is(requestErr, context.Canceled) {
				failureClass = "cancelled"
			}
			lastAttemptFailureClass = failureClass
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: queueMs, DurationMs: time.Since(attemptStarted).Milliseconds(),
				FailureClass: failureClass,
			})
			if ctx.Err() != nil {
				writeRouterContextError(w, r, ctx)
				return
			}
			if attemptNumber < routerMaxAttempts && !sleepWithContext(ctx, backoff) {
				writeRouterContextError(w, r, ctx)
				return
			}
			backoff = min(backoff*2, 800*time.Millisecond)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			ttfbTimer.Stop()
			responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, routerMaxErrorBytes+1))
			resp.Body.Close()
			attemptCancel()
			s.releaseRouterReservation(reservedNodeID, reservation)
			if ctx.Err() != nil {
				s.recordRouterEvent(routerRequestEvent{
					RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
					Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
					Engine: assignment.Engine, QueueMs: queueMs, Status: resp.StatusCode,
					DurationMs:   time.Since(attemptStarted).Milliseconds(),
					FailureClass: "timeout",
				})
				writeRouterContextError(w, r, ctx)
				return
			}
			truncated := len(responseBody) > routerMaxErrorBytes
			if truncated {
				responseBody = responseBody[:routerMaxErrorBytes]
			}
			failureClass, retryable := classifyRouterStatus(resp.StatusCode, responseBody)
			if readErr != nil && retryable {
				failureClass = "upstream_read"
			}
			lastAttemptFailureClass = failureClass
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: queueMs, Status: resp.StatusCode,
				DurationMs:   time.Since(attemptStarted).Milliseconds(),
				FailureClass: failureClass,
			})
			if retryable && attemptNumber < routerMaxAttempts && ctx.Err() == nil {
				if !sleepWithContext(ctx, backoff) {
					writeRouterContextError(w, r, ctx)
					return
				}
				backoff = min(backoff*2, 800*time.Millisecond)
				continue
			}
			copyRouterHeaders(w.Header(), resp.Header)
			if truncated {
				w.Header().Del("Content-Length")
			}
			w.Header().Set("X-Request-ID", requestID)
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(responseBody)
			return
		}

		s.metrics.changeRouterActiveStreams(1)
		activeStream := true
		finishStream := func() {
			if activeStream {
				s.metrics.changeRouterActiveStreams(-1)
				activeStream = false
			}
		}
		firstByte := make([]byte, 1)
		n, readErr := io.ReadFull(resp.Body, firstByte)
		firstByteMs := time.Since(attemptStarted).Milliseconds()
		ttfbTimer.Stop()
		if ttfbExpired.Load() || (n == 0 && readErr != nil && !errors.Is(readErr, io.EOF)) {
			resp.Body.Close()
			attemptCancel()
			finishStream()
			s.releaseRouterReservation(reservedNodeID, reservation)
			failureClass := "upstream_read"
			if ctx.Err() != nil {
				failureClass = "timeout"
				if errors.Is(ctx.Err(), context.Canceled) {
					failureClass = "cancelled"
				}
			} else if ttfbExpired.Load() {
				failureClass = "time_to_first_byte_timeout"
			}
			lastAttemptFailureClass = failureClass
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: queueMs, Status: resp.StatusCode,
				TimeToFirstByteMs: firstByteMs, DurationMs: time.Since(attemptStarted).Milliseconds(),
				FailureClass: failureClass,
			})
			if ctx.Err() != nil {
				writeRouterContextError(w, r, ctx)
				return
			}
			if attemptNumber < routerMaxAttempts && ctx.Err() == nil {
				if !sleepWithContext(ctx, backoff) {
					writeRouterContextError(w, r, ctx)
					return
				}
				backoff = min(backoff*2, 800*time.Millisecond)
				continue
			}
			if failureClass == "time_to_first_byte_timeout" {
				writeRouterError(w, http.StatusGatewayTimeout, "inference upstream did not send content before the first-byte timeout")
				return
			}
			writeRouterError(w, http.StatusBadGateway, "inference upstream failed before sending content")
			return
		}
		if n > 0 {
			s.metrics.observeRouterTTFB(time.Since(attemptStarted))
		}

		copyRouterHeaders(w.Header(), resp.Header)
		w.Header().Set("X-Request-ID", requestID)
		w.WriteHeader(resp.StatusCode)
		if n > 0 {
			if _, err := w.Write(firstByte[:n]); err != nil {
				resp.Body.Close()
				attemptCancel()
				finishStream()
				s.releaseRouterReservation(reservedNodeID, reservation)
				failureClass := "client_write"
				if r.Context().Err() != nil {
					failureClass = "cancelled"
				}
				s.recordRouterEvent(routerRequestEvent{
					RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
					Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
					Engine: assignment.Engine, QueueMs: queueMs, Status: resp.StatusCode,
					TimeToFirstByteMs: firstByteMs, DurationMs: time.Since(attemptStarted).Milliseconds(),
					FailureClass: failureClass,
				})
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		streamErr := error(nil)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			streamErr = readErr
		} else if readErr == nil {
			idleReader := newRouterIdleTimeoutReader(resp.Body, s.routerIdleTimeout, attemptCancel)
			streamErr = streamRouterResponse(w, idleReader)
			_ = idleReader.Close()
		}
		resp.Body.Close()
		attemptCancel()
		finishStream()
		s.releaseRouterReservation(reservedNodeID, reservation)
		event := routerRequestEvent{
			RequestID: requestID, AttemptID: attemptID, Phase: "attempt_succeeded",
			Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
			Engine: assignment.Engine, QueueMs: queueMs, TimeToFirstByteMs: firstByteMs,
			DurationMs: time.Since(attemptStarted).Milliseconds(),
			Status:     resp.StatusCode,
		}
		if streamErr != nil {
			event.Phase = "attempt_failed"
			event.FailureClass = "stream_interrupted"
			if r.Context().Err() != nil {
				event.FailureClass = "cancelled"
			} else if errors.Is(streamErr, errRouterIdleStreamTimeout) {
				event.FailureClass = "idle_stream_timeout"
			} else if ctx.Err() != nil {
				event.FailureClass = "timeout"
			}
			log.Printf("orchestrator: router stream ended after response commit request_id=%s node=%s failure_class=%s",
				requestID, assignment.NodeID, event.FailureClass)
		}
		s.recordRouterEvent(event)
		return
	}
	failureClass := "attempts_exhausted"
	status := http.StatusBadGateway
	if lastAttemptFailureClass == "time_to_first_byte_timeout" {
		failureClass = lastAttemptFailureClass
		status = http.StatusGatewayTimeout
	}
	s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: failureClass})
	writeRouterError(w, status, "inference could not be completed on a compatible node")
}

func (s *Server) assignRouterRequest(model string, protocol inferroute.Protocol, tried map[string]bool) (scheduler.Assignment, error) {
	nodes := s.dispatcherPort().schedulerNodes()
	eligible := make([]cluster.NodeStatus, 0, len(nodes))
	for _, node := range nodes {
		models := make([]engine.Model, 0, len(node.Models))
		for _, candidate := range node.Models {
			engineName := candidate.Engine
			if engineName == "" || !node.EngineReady(engineName) {
				continue
			}
			if model != "" && !strings.EqualFold(candidate.Name, model) {
				continue
			}
			compatible := false
			for _, advertised := range candidate.Protocols {
				if strings.EqualFold(advertised, string(protocol)) {
					compatible = true
					break
				}
			}
			if len(candidate.Protocols) == 0 {
				compatible = engine.SupportsProtocol(engineName, string(protocol))
			}
			if !compatible || tried[routerPairKey(node.ID, candidate.Name)] {
				continue
			}
			if !s.routerBreakers.eligible(node.ID, candidate.Name) {
				continue
			}
			models = append(models, candidate)
		}
		if len(models) > 0 {
			node.Models = models
			eligible = append(eligible, node)
		}
	}
	subtask := scheduler.Subtask{
		ID: "router-inference", Description: "Transparent inference request",
		TaskType: "general", PreferredModel: model,
	}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{subtask}, eligible, s.dispatcherPort().experience())
	if err != nil {
		return scheduler.Assignment{}, err
	}
	if len(assignments) != 1 {
		return scheduler.Assignment{}, fmt.Errorf("scheduler returned no inference assignment")
	}
	return assignments[0], nil
}

func (s *Server) releaseRouterReservation(nodeID string, reservation workpool.CapacityReservation) {
	s.registry.IncrementLoad(nodeID, -1)
	reservation.Release()
}

func (s *Server) recordRouterEvent(event routerRequestEvent) {
	event.Timestamp = event.Timestamp.UTC()
	s.routerEvents.append(event)
	switch event.Phase {
	case "rejected":
		s.metrics.recordRouterFailure(event.FailureClass)
	case "finished":
		s.metrics.recordRouterFailure(event.FailureClass)
	case "attempt_failed":
		s.metrics.recordRouterFailure(event.FailureClass)
		if isTransientRouterFailure(event.FailureClass) {
			s.routerBreakers.recordFailure(event.NodeID, event.Model)
		} else if event.Status >= http.StatusOK {
			s.routerBreakers.recordHealthyResponse(event.NodeID, event.Model)
		}
	case "attempt_succeeded":
		s.routerBreakers.recordSuccess(event.NodeID, event.Model)
	}
	if event.Phase == "attempt_failed" || event.Phase == "attempt_succeeded" {
		failureClass := event.FailureClass
		if failureClass == "" {
			failureClass = "none"
		}
		log.Printf("orchestrator: router dispatch request_id=%s attempt_id=%s node_id=%s model=%s engine=%s duration_ms=%d status=%d failure_class=%s success=%t",
			event.RequestID, event.AttemptID, event.NodeID, event.Model, event.Engine,
			event.DurationMs, event.Status, failureClass, event.Phase == "attempt_succeeded")
	}
}

func routerPairKey(nodeID, model string) string { return nodeID + "\x00" + model }

func newRouterRequestID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func writeRouterError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message}})
}

func writeRouterContextError(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	if r.Context().Err() != nil {
		return
	}
	status := http.StatusGatewayTimeout
	message := "inference request deadline exceeded"
	if errors.Is(ctx.Err(), context.Canceled) {
		status = http.StatusRequestTimeout
		message = "inference request was interrupted"
	}
	writeRouterError(w, status, message)
}

func classifyRouterStatus(status int, body []byte) (string, bool) {
	switch {
	case status == http.StatusRequestTimeout:
		return "timeout", true
	case status == http.StatusTooManyRequests:
		return "rate_limit", true
	case status == http.StatusInternalServerError || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout:
		return "upstream_5xx", true
	case status == http.StatusNotFound && strings.Contains(strings.ToLower(string(body)), "model"):
		return "stale_model", true
	default:
		return "client_error", false
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func stripHopHeaders(header http.Header) {
	for _, connection := range header.Values("Connection") {
		for _, name := range strings.Split(connection, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{
		"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		header.Del(name)
	}
}

func copyRouterHeaders(dst, src http.Header) {
	stripHopHeaders(dst)
	connectionTokens := make(map[string]bool)
	for _, connection := range src.Values("Connection") {
		for _, name := range strings.Split(connection, ",") {
			connectionTokens[http.CanonicalHeaderKey(strings.TrimSpace(name))] = true
		}
	}
	for name, values := range src {
		if strings.EqualFold(name, "Connection") || strings.EqualFold(name, "Keep-Alive") ||
			strings.EqualFold(name, "Proxy-Authenticate") || strings.EqualFold(name, "Proxy-Authorization") ||
			strings.EqualFold(name, "Te") || strings.EqualFold(name, "Trailer") ||
			strings.EqualFold(name, "Transfer-Encoding") || strings.EqualFold(name, "Upgrade") ||
			connectionTokens[http.CanonicalHeaderKey(name)] {
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func streamRouterResponse(w http.ResponseWriter, body io.Reader) error {
	buf := routerStreamBufferPool.Get().([]byte)
	defer routerStreamBufferPool.Put(buf)
	return httpx.NewStreamWriter(w).Copy(body, buf)
}
