package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
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

// routerStreamBufferPool holds *[]byte so Put does not allocate a slice header.
var routerStreamBufferPool = sync.Pool{
	New: func() any {
		buffer := make([]byte, 32*1024)
		return &buffer
	},
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
	requestID, err := httpx.NewRequestID()
	if err != nil {
		logging.Errorf("orchestrator", "orchestrator: generate router request ID failed: %v", err)
		writeRouterError(w, http.StatusInternalServerError, "could not initialize request")
		return
	}
	w.Header().Set("X-Request-ID", requestID)
	startedAt := time.Now()
	terminal := routerRequestEvent{RequestID: requestID, Phase: "failed", FailureClass: "request_preparation"}
	recordEvent := func(event routerRequestEvent) {
		switch event.Phase {
		case "rejected", "finished", "attempt_failed", "attempt_succeeded":
			terminal = event
		}
		s.recordRouterEvent(event)
	}
	defer func() {
		s.metrics.observeRouterRequest(time.Since(startedAt))
		if terminal.Phase == "attempt_succeeded" {
			terminal.Phase = "completed"
		} else {
			terminal.Phase = "failed"
			if errors.Is(r.Context().Err(), context.Canceled) {
				terminal.FailureClass = "cancelled"
			} else if errors.Is(r.Context().Err(), context.DeadlineExceeded) || time.Since(startedAt) >= routerRequestTimeout {
				terminal.FailureClass = "timeout"
			}
		}
		terminal.Timestamp = time.Now()
		terminal.DurationMs = time.Since(startedAt).Milliseconds()
		s.recordRouterEvent(terminal)
	}()
	recordEvent(routerRequestEvent{RequestID: requestID, Phase: "accepted", Timestamp: startedAt})

	limit := s.routerClients.acquire(r)
	if limit.Reason != "" {
		status := http.StatusTooManyRequests
		failureClass := "client_rate_limit"
		message := "per-client request rate limit exceeded"
		switch limit.Reason {
		case "concurrency":
			failureClass = "client_concurrency_limit"
			message = "per-client concurrency limit exceeded"
		case "capacity":
			status = http.StatusServiceUnavailable
			failureClass = "client_limit_capacity"
			message = "client limit capacity is temporarily unavailable"
		}
		w.Header().Set("Retry-After", fmt.Sprintf("%d", limit.RetryAfter))
		recordEvent(routerRequestEvent{
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
		recordEvent(routerRequestEvent{RequestID: requestID, Phase: "rejected", Timestamp: time.Now(), FailureClass: "invalid_request"})
		return
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(requestBody, &payload); err != nil || payload == nil {
		writeRouterError(w, http.StatusBadRequest, "invalid inference request JSON")
		recordEvent(routerRequestEvent{RequestID: requestID, Phase: "rejected", Timestamp: time.Now(), FailureClass: "invalid_request"})
		return
	}
	preferredModel := ""
	if rawModel, found := payload["model"]; found {
		if err := json.Unmarshal(rawModel, &preferredModel); err != nil {
			writeRouterError(w, http.StatusBadRequest, "model must be a string")
			recordEvent(routerRequestEvent{RequestID: requestID, Phase: "rejected", Timestamp: time.Now(), FailureClass: "invalid_request"})
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
			recordEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: failureClass})
			writeRouterContextError(w, r, ctx)
			return
		}
		assignment, assignErr := s.assignRouterRequest(preferredModel, route.Protocol, tried)
		if assignErr != nil {
			if errors.Is(assignErr, apperr.Unavailable) {
				recordEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: "shard_group_unavailable"})
				writeRouterError(w, apperr.StatusCode(assignErr), assignErr.Error())
				return
			}
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
			recordEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: failureClass})
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
		recordEvent(routerRequestEvent{RequestID: requestID, AttemptID: attemptID, Phase: "scheduled",
			Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model, Engine: assignment.Engine})
		node, exists := s.registry.Get(assignment.NodeID)
		if !exists || !node.Healthy || node.Address != assignment.Address {
			s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
			lastAttemptFailureClass = "node_unavailable"
			recordEvent(routerRequestEvent{
				RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
				Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
				Engine: assignment.Engine, QueueMs: time.Since(startedAt).Milliseconds(),
				DurationMs:   time.Since(startedAt).Milliseconds(),
				FailureClass: "node_unavailable",
			})
			continue
		}

		shardGroupID := strings.TrimPrefix(assignment.Engine, "shard-group:")
		isShardGroup := shardGroupID != assignment.Engine
		upstreamPrefix := "/proxy"
		upstreamEngine := assignment.Engine
		if isShardGroup {
			group, ok := s.shards.Get(shardGroupID)
			if !ok || group.State != ShardGroupReady {
				s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
				recordEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: "shard_group_unavailable"})
				writeRouterError(w, http.StatusServiceUnavailable, apperr.UnavailableError(errors.New("shard group is not ready")).Error())
				return
			}
			var head ShardGroupMember
			for _, member := range group.Members {
				if member.Role == "head" {
					head = member
					break
				}
			}
			if head.NodeID != assignment.NodeID || head.Address != assignment.Address {
				s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
				recordEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: "shard_group_unavailable"})
				writeRouterError(w, http.StatusServiceUnavailable, apperr.UnavailableError(errors.New("shard group head is unavailable")).Error())
				return
			}
			upstreamPrefix = "/api/engines/managed/" + url.PathEscape(head.EngineID) + "/proxy"
			upstreamEngine = "shard/" + string(group.Mode)
		}

		var reservation workpool.CapacityReservation
		recordEvent(routerRequestEvent{RequestID: requestID, AttemptID: attemptID, Phase: "queued", Timestamp: time.Now(),
			NodeID: assignment.NodeID, Model: assignment.Model, Engine: assignment.Engine})
		if isShardGroup {
			releaseRequest, err := s.shards.AcquireRequest(ctx, shardGroupID)
			if err != nil {
				// AcquireRequest returns no release callback on error.
				s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
				recordEvent(routerRequestEvent{RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
					Timestamp: time.Now(), FailureClass: "reservation"})
				if ctx.Err() != nil {
					return
				}
				writeRouterError(w, http.StatusServiceUnavailable, apperr.UnavailableError(err).Error())
				return
			}
			group, exists := s.shards.Get(shardGroupID)
			verifyErr := errShardGroupNotFound
			if exists {
				for _, member := range group.Members {
					if member.Role == "head" {
						verifyErr = s.nodeClient.VerifyModel(ctx, member.Address, member.EngineID, group.Model, false)
						break
					}
				}
			}
			if verifyErr != nil {
				releaseRequest()
				s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
				recordEvent(routerRequestEvent{RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
					Timestamp: time.Now(), FailureClass: "stale_model"})
				if ctx.Err() != nil {
					return
				}
				writeRouterError(w, http.StatusServiceUnavailable, "shard head model does not match saved group; stop and start the group to restore its model: "+verifyErr.Error())
				return
			}
			reservation = staticRouterReservation{target: workpool.CapacityTarget{
				NodeID: assignment.NodeID, Address: assignment.Address,
			}, release: releaseRequest}
		} else {
			s.metrics.changeRouterQueueDepth(1)
			reservation, err = s.dispatcherPort().reservations().Reserve(ctx, []workpool.CapacityTarget{{
				NodeID: assignment.NodeID, Address: assignment.Address,
			}})
			s.metrics.changeRouterQueueDepth(-1)
			if err != nil {
				s.routerBreakers.releaseProbe(assignment.NodeID, assignment.Model)
				if ctx.Err() != nil {
					continue
				}
				recordEvent(routerRequestEvent{
					RequestID: requestID, AttemptID: attemptID, Phase: "attempt_failed",
					Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model,
					Engine: assignment.Engine, QueueMs: time.Since(startedAt).Milliseconds(),
					DurationMs:   time.Since(startedAt).Milliseconds(),
					FailureClass: "reservation",
				})
				lastAttemptFailureClass = "reservation"
				continue
			}
		}
		reservedNodeID := reservation.Target().NodeID
		s.metrics.recordCapacityReservation()
		queueMs := time.Since(startedAt).Milliseconds()
		s.registry.IncrementLoad(reservedNodeID, 1)
		recordEvent(routerRequestEvent{
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
		upstreamURL := strings.TrimRight(assignment.Address, "/") + upstreamPrefix + route.Path
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
		upstreamReq.Header.Set(inferroute.HeaderEngine, upstreamEngine)
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
		recordEvent(routerRequestEvent{RequestID: requestID, AttemptID: attemptID, Phase: "dispatched",
			Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model, Engine: assignment.Engine})
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
			recordEvent(routerRequestEvent{
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

		recordEvent(routerRequestEvent{RequestID: requestID, AttemptID: attemptID, Phase: "upstream_response",
			Timestamp: time.Now(), NodeID: assignment.NodeID, Model: assignment.Model, Engine: assignment.Engine, Status: resp.StatusCode})
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			ttfbTimer.Stop()
			responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, routerMaxErrorBytes+1))
			_ = resp.Body.Close()
			attemptCancel()
			s.releaseRouterReservation(reservedNodeID, reservation)
			if ctx.Err() != nil {
				recordEvent(routerRequestEvent{
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
			failureClass, retryable := classifyUpstreamStatus(resp.StatusCode, responseBody)
			if readErr != nil && retryable {
				failureClass = "upstream_read"
			}
			lastAttemptFailureClass = failureClass
			recordEvent(routerRequestEvent{
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
			_ = resp.Body.Close()
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
			recordEvent(routerRequestEvent{
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
			recordEvent(routerRequestEvent{RequestID: requestID, AttemptID: attemptID, Phase: "first_upstream_byte",
				Timestamp: time.Now(), TimeToFirstByteMs: firstByteMs, Status: resp.StatusCode})
		}

		copyRouterHeaders(w.Header(), resp.Header)
		w.Header().Set("X-Request-ID", requestID)
		w.WriteHeader(resp.StatusCode)
		if n > 0 {
			if _, err := w.Write(firstByte[:n]); err != nil {
				_ = resp.Body.Close()
				attemptCancel()
				finishStream()
				s.releaseRouterReservation(reservedNodeID, reservation)
				failureClass := "client_write"
				if r.Context().Err() != nil {
					failureClass = "cancelled"
				}
				recordEvent(routerRequestEvent{
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
		_ = resp.Body.Close()
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
			slog.Default().Warn("router stream ended after response commit",
				"component", "orchestrator",
				"request_id", requestID,
				"failure_class", event.FailureClass,
			)
		}
		recordEvent(event)
		return
	}
	failureClass := "attempts_exhausted"
	status := http.StatusBadGateway
	if lastAttemptFailureClass == "time_to_first_byte_timeout" {
		failureClass = lastAttemptFailureClass
		status = http.StatusGatewayTimeout
	}
	recordEvent(routerRequestEvent{RequestID: requestID, Phase: "finished", Timestamp: time.Now(), FailureClass: failureClass})
	writeRouterError(w, status, "inference could not be completed on a compatible node")
}

func (s *Server) assignRouterRequest(model string, protocol inferroute.Protocol, tried map[string]bool) (scheduler.Assignment, error) {
	started := time.Now()
	defer func() { s.metrics.recordAssignment(time.Since(started)) }()
	if model != "" && s.shards != nil {
		if group, found := s.shards.GroupForModel(model); found {
			if group.State != ShardGroupReady {
				scheduler.LogDecision("router_shard", "group_unready", cluster.NodeStatus{}, "", 0, scheduler.ScoreFactors{})
				return scheduler.Assignment{}, apperr.UnavailableError(fmt.Errorf("shard group for model %q is %s", model, group.State))
			}
			var head ShardGroupMember
			for _, member := range group.Members {
				if member.Role == "head" {
					head = member
					break
				}
			}
			node, ok := s.registry.Get(head.NodeID)
			if !ok || !node.Healthy || node.Address != head.Address {
				scheduler.LogDecision("router_shard", "head_unavailable", node, "", 0, scheduler.ScoreFactors{})
				return scheduler.Assignment{}, apperr.UnavailableError(fmt.Errorf("shard group head for model %q is unavailable", model))
			}
			if tried[routerPairKey(head.NodeID, group.Model.Name)] {
				scheduler.LogDecision("router_shard", "already_tried", node, group.Model.Name, 0, scheduler.ScoreFactors{})
				return scheduler.Assignment{}, apperr.UnavailableError(fmt.Errorf("shard group for model %q has no additional inference head", model))
			}
			scheduler.LogDecision("router_shard", "head_selected", node, group.Model.Name, 0, scheduler.ScoreFactors{})
			return scheduler.Assignment{
				NodeID: head.NodeID, Address: head.Address, Model: group.Model.Name,
				Engine: "shard-group:" + group.ID,
			}, nil
		}
	}
	nodes := s.dispatcherPort().schedulerNodes()
	eligible := make([]cluster.NodeStatus, 0, len(nodes))
	for _, node := range nodes {
		models := node.Models[:0]
		for _, candidate := range node.Models {
			engineName := candidate.Engine
			if engineName == "" || !node.EngineReady(engineName) {
				scheduler.LogDecision("router_filter", "engine_unready", node, candidate.Name, 0, scheduler.ScoreFactors{})
				continue
			}
			if model != "" && !strings.EqualFold(candidate.Name, model) {
				scheduler.LogDecision("router_filter", "model_filter", node, candidate.Name, 0, scheduler.ScoreFactors{})
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
			if !compatible {
				scheduler.LogDecision("router_filter", "protocol_incompatible", node, candidate.Name, 0, scheduler.ScoreFactors{})
				continue
			}
			if tried[routerPairKey(node.ID, candidate.Name)] {
				scheduler.LogDecision("router_filter", "already_tried", node, candidate.Name, 0, scheduler.ScoreFactors{})
				continue
			}
			if !s.routerBreakers.eligible(node.ID, candidate.Name) {
				scheduler.LogDecision("router_filter", "circuit_open", node, candidate.Name, 0, scheduler.ScoreFactors{})
				continue
			}
			scheduler.LogDecision("router_filter", "eligible", node, candidate.Name, 0, scheduler.ScoreFactors{})
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

type staticRouterReservation struct {
	target  workpool.CapacityTarget
	release func()
}

func (r staticRouterReservation) Target() workpool.CapacityTarget { return r.target }
func (r staticRouterReservation) Release() {
	if r.release != nil {
		r.release()
	}
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
		slog.Default().Info("router dispatch",
			"component", "orchestrator",
			"request_id", event.RequestID,
			"attempt_id", event.AttemptID,
			"duration_ms", event.DurationMs,
			"http_status", event.Status,
			"failure_class", failureClass,
			"success", event.Phase == "attempt_succeeded",
		)
	}
	if logger := slog.Default(); logger.Enabled(context.Background(), slog.LevelDebug) {
		logger.Debug("router lifecycle",
			"component", "orchestrator", "request_id", event.RequestID, "attempt_id", event.AttemptID,
			"phase", event.Phase, "http_status", event.Status, "failure_class", event.FailureClass,
			"duration_ms", event.DurationMs, "queue_ms", event.QueueMs, "first_byte_ms", event.TimeToFirstByteMs)
	}
}

func routerPairKey(nodeID, model string) string { return nodeID + "\x00" + model }

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
		for {
			name, remaining, found := strings.Cut(connection, ",")
			header.Del(strings.TrimSpace(name))
			if !found {
				break
			}
			connection = remaining
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
	var tokenStorage [8]string
	connectionTokens := tokenStorage[:0]
	for _, connection := range src.Values("Connection") {
		for {
			name, remaining, found := strings.Cut(connection, ",")
			connectionTokens = append(connectionTokens, http.CanonicalHeaderKey(strings.TrimSpace(name)))
			if !found {
				break
			}
			connection = remaining
		}
	}
	for name, values := range src {
		if strings.EqualFold(name, "Connection") || strings.EqualFold(name, "Keep-Alive") ||
			strings.EqualFold(name, "Proxy-Authenticate") || strings.EqualFold(name, "Proxy-Authorization") ||
			strings.EqualFold(name, "Te") || strings.EqualFold(name, "Trailer") ||
			strings.EqualFold(name, "Transfer-Encoding") || strings.EqualFold(name, "Upgrade") {
			continue
		}
		if len(connectionTokens) > 0 {
			canonicalName := http.CanonicalHeaderKey(name)
			connectionSpecific := false
			for _, token := range connectionTokens {
				if token == canonicalName {
					connectionSpecific = true
					break
				}
			}
			if connectionSpecific {
				continue
			}
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func streamRouterResponse(w http.ResponseWriter, body io.Reader) error {
	buf := routerStreamBufferPool.Get().(*[]byte)
	defer routerStreamBufferPool.Put(buf)
	return httpx.NewStreamWriter(w).Copy(body, *buf)
}
