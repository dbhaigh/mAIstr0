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
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/scheduler"
)

const (
	routerRequestTimeout  = 2 * time.Minute
	routerMaxAttempts     = 5
	routerMaxErrorBytes   = 1 << 20
	maxRouterRequestBytes = 20 << 20
)

func (s *Server) handleRouterEvents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"events": s.routerEvents.snapshot()})
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
			}
			s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: failureClass})
			writeRouterError(w, status, "no additional compatible inference node is available")
			return
		}
		pairKey := routerPairKey(assignment.NodeID, assignment.Model)
		tried[pairKey] = true
		attemptID := fmt.Sprintf("%s:%d", requestID, attemptNumber)
		node, exists := s.registry.Get(assignment.NodeID)
		if !exists || !node.Healthy || node.Address != assignment.Address {
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: time.Since(startedAt).Milliseconds(),
				FailureClass: "node_unavailable",
			})
			continue
		}

		gate := s.dispatcher.nodeDispatchGate(assignment.NodeID, node.MaxConcurrentTasks)
		capacity := func() (int, int, int, bool) {
			return s.dispatcher.dispatchCapacity(ctx, gate, assignment.NodeID, assignment.Address)
		}
		if err := gate.acquire(ctx, capacity); err != nil {
			if ctx.Err() != nil {
				continue
			}
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: time.Since(startedAt).Milliseconds(),
				FailureClass: "reservation",
			})
			continue
		}
		select {
		case s.dispatcher.slots <- struct{}{}:
		case <-ctx.Done():
			gate.release()
			continue
		}
		queueMs := time.Since(startedAt).Milliseconds()
		s.registry.IncrementLoad(assignment.NodeID, 1)
		s.recordRouterEvent(routerRequestEvent{
			RequestID: requestID, AttemptID: attemptID, Phase: "attempt_started",
			Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
			Engine: assignment.Engine, QueueMs: queueMs,
		})

		body := requestBody
		if preferredModel == "" {
			payload["model"], _ = json.Marshal(assignment.Model)
			body, err = json.Marshal(payload)
			if err != nil {
				s.releaseRouterReservation(assignment.NodeID, gate)
				writeRouterError(w, http.StatusInternalServerError, "could not prepare inference request")
				return
			}
		}
		upstreamURL := strings.TrimRight(assignment.Address, "/") + "/proxy" + route.Path
		if r.URL.RawQuery != "" {
			upstreamURL += "?" + r.URL.RawQuery
		}
		upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(body))
		if err != nil {
			s.releaseRouterReservation(assignment.NodeID, gate)
			writeRouterError(w, http.StatusInternalServerError, "could not prepare inference request")
			return
		}
		upstreamReq.Header = r.Header.Clone()
		stripHopHeaders(upstreamReq.Header)
		upstreamReq.Header.Set(inferroute.HeaderEngine, assignment.Engine)
		upstreamReq.Header.Set(inferroute.HeaderRequestID, requestID)
		upstreamReq.Header.Set(inferroute.HeaderAttemptID, attemptID)
		attemptStarted := time.Now()
		resp, requestErr := s.nodeClient.dispatcher.Do(upstreamReq)

		if requestErr != nil {
			s.releaseRouterReservation(assignment.NodeID, gate)
			failureClass := "transport"
			if errors.Is(requestErr, context.DeadlineExceeded) {
				failureClass = "timeout"
			} else if errors.Is(requestErr, context.Canceled) {
				failureClass = "cancelled"
			}
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: queueMs, FailureClass: failureClass,
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
			responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, routerMaxErrorBytes+1))
			resp.Body.Close()
			s.releaseRouterReservation(assignment.NodeID, gate)
			if ctx.Err() != nil {
				s.recordRouterEvent(routerRequestEvent{
					RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
					Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
					Engine: assignment.Engine, QueueMs: queueMs, Status: resp.StatusCode,
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
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: queueMs, Status: resp.StatusCode,
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

		firstByte := make([]byte, 1)
		n, readErr := resp.Body.Read(firstByte)
		firstByteMs := time.Since(attemptStarted).Milliseconds()
		if n == 0 && readErr != nil && !errors.Is(readErr, io.EOF) {
			resp.Body.Close()
			s.releaseRouterReservation(assignment.NodeID, gate)
			s.recordRouterEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: queueMs, Status: resp.StatusCode,
				TimeToFirstByteMs: firstByteMs, FailureClass: "upstream_read",
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
			writeRouterError(w, http.StatusBadGateway, "inference upstream failed before sending content")
			return
		}

		copyRouterHeaders(w.Header(), resp.Header)
		w.Header().Set("X-Request-ID", requestID)
		w.WriteHeader(resp.StatusCode)
		if n > 0 {
			if _, err := w.Write(firstByte[:n]); err != nil {
				resp.Body.Close()
				s.releaseRouterReservation(assignment.NodeID, gate)
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
			streamErr = streamRouterResponse(w, resp.Body)
		}
		resp.Body.Close()
		s.releaseRouterReservation(assignment.NodeID, gate)
		event := routerRequestEvent{
			RequestID: requestID, AttemptID: attemptID, Phase: "attempt_succeeded",
			Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
			Engine: assignment.Engine, QueueMs: queueMs, TimeToFirstByteMs: firstByteMs,
			Status: resp.StatusCode,
		}
		if streamErr != nil {
			event.Phase = "attempt_failed"
			event.FailureClass = "stream_interrupted"
			if r.Context().Err() != nil {
				event.FailureClass = "cancelled"
			}
			log.Printf("orchestrator: router stream ended after response commit request_id=%s node=%s: %v", requestID, assignment.NodeID, streamErr)
		}
		s.recordRouterEvent(event)
		return
	}
	s.recordRouterEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: "attempts_exhausted"})
	writeRouterError(w, http.StatusBadGateway, "inference could not be completed on a compatible node")
}

func (s *Server) assignRouterRequest(model string, protocol inferroute.Protocol, tried map[string]bool) (scheduler.Assignment, error) {
	nodes := s.dispatcher.schedulerNodes()
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
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{subtask}, eligible, s.dispatcher.experience())
	if err != nil {
		return scheduler.Assignment{}, err
	}
	if len(assignments) != 1 {
		return scheduler.Assignment{}, fmt.Errorf("scheduler returned no inference assignment")
	}
	return assignments[0], nil
}

func (s *Server) releaseRouterReservation(nodeID string, gate *nodeDispatchGate) {
	s.registry.IncrementLoad(nodeID, -1)
	gate.release()
	<-s.dispatcher.slots
}

func (s *Server) recordRouterEvent(event routerRequestEvent) {
	event.Timestamp = event.Timestamp.UTC()
	s.routerEvents.append(event)
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
	buf := make([]byte, 32*1024)
	flusher, canFlush := w.(http.Flusher)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			return nil
		}
	}
}
