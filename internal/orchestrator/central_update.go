package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/selfupdate"
	"github.com/maistr0/maistr0/internal/version"
)

const centralUpdateFanoutConcurrency = 4

func (s *Server) HasNodesBehind(release string) bool {
	if s.registry == nil {
		return false
	}
	for _, node := range s.registry.All() {
		if !node.Healthy || node.ID == s.SelfID() {
			continue
		}
		if node.Version == "" || version.Compare(node.Version, release) < 0 {
			return true
		}
	}
	return false
}

func (s *Server) BroadcastCentralUpdate(ctx context.Context, request selfupdate.UpgradeRequest) error {
	if request.Central == nil {
		return errors.New("central update request is required")
	}
	if err := request.Validate(); err != nil {
		return fmt.Errorf("validate central update announcement: %w", err)
	}
	if !s.Elected() {
		return errors.New("this instance is not the current orchestrator")
	}
	if s.registry == nil {
		return errors.New("node registry is unavailable")
	}
	nodes := make([]cluster.NodeStatus, 0)
	for _, node := range s.registry.All() {
		if !node.Healthy || node.ID == s.SelfID() ||
			(node.Version != "" && version.Compare(node.Version, request.Central.Catalog.Version) >= 0) {
			continue
		}
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	if len(nodes) == 0 {
		return nil
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode central update announcement: %w", err)
	}
	workerCount := min(centralUpdateFanoutConcurrency, len(nodes))
	jobs := make(chan cluster.NodeStatus)
	errs := make(chan error, len(nodes))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for node := range jobs {
				if err := s.sendCentralUpdate(ctx, node, body); err != nil {
					errs <- err
				}
			}
		}()
	}
	for _, node := range nodes {
		select {
		case jobs <- node:
		case <-ctx.Done():
			errs <- ctx.Err()
			goto done
		}
	}

done:
	close(jobs)
	workers.Wait()
	close(errs)
	var failures []error
	for err := range errs {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (s *Server) sendCentralUpdate(ctx context.Context, node cluster.NodeStatus, body []byte) error {
	if !s.Elected() {
		return errors.New("orchestrator authority changed during central update broadcast")
	}
	parsed, err := url.Parse(node.Address)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Hostname() == "" ||
		parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("node %s has an invalid HTTPS address", node.ID)
	}
	requestContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestContext, http.MethodPost, strings.TrimRight(node.Address, "/")+"/api/upgrade", bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create central update request for node %s: %w", node.ID, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.nodeClientPort().Do(request)
	if err != nil {
		return fmt.Errorf("send central update to node %s: %w", node.ID, err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode == http.StatusConflict {
		return nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
		return fmt.Errorf("node %s rejected central update with %s", node.ID, response.Status)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	return nil
}
