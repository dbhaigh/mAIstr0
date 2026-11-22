package selfupdate

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/version"
)

type CentralUpdateCoordinator interface {
	Elected() bool
	HasNodesBehind(string) bool
	BroadcastCentralUpdate(context.Context, UpgradeRequest) error
}

type CentralUpdateOptions struct {
	ManifestURL        string
	PublisherPublicKey ed25519.PublicKey
	PollInterval       time.Duration
	Distribution       Distribution
	Client             *http.Client
	Coordinator        func() CentralUpdateCoordinator
	QueueSelfUpdate    func(context.Context, UpgradeRequest) error
}

func RunCentralUpdates(ctx context.Context, options CentralUpdateOptions) {
	if err := ValidateUpdateManifestURL(options.ManifestURL); err != nil {
		logging.Errorf("update", "central updates are enabled without a valid manifest URL: %v", err)
		return
	}
	if len(options.PublisherPublicKey) != ed25519.PublicKeySize {
		logging.Errorf("update", "central updates are enabled without a valid publisher public key")
		return
	}
	if options.Coordinator == nil || options.QueueSelfUpdate == nil {
		logging.Errorf("update", "central updates are missing a coordinator or upgrade queue")
		return
	}
	if options.Distribution.Executable == "" {
		distribution, err := CurrentDistribution()
		if err != nil {
			logging.Errorf("update", "cannot locate installed executable for central updates: %v", err)
			return
		}
		options.Distribution = distribution
	}
	if options.PollInterval <= 0 {
		options.PollInterval = DefaultCentralUpdatePollInterval
	}
	ticker := time.NewTicker(options.PollInterval)
	defer ticker.Stop()
	for {
		if err := pollCentralUpdate(ctx, options); err != nil && ctx.Err() == nil {
			logging.Warnf("update", "central update check failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func pollCentralUpdate(ctx context.Context, options CentralUpdateOptions) error {
	coordinator := options.Coordinator()
	if coordinator == nil || !coordinator.Elected() {
		return nil
	}
	pendingPath := options.Distribution.Executable + ".central-update-pending"
	pending, err := readPendingCentralUpdate(pendingPath, options.PublisherPublicKey)
	if err != nil {
		return fmt.Errorf("read pending central rollout: %w", err)
	}
	if pending != nil && pending.ManifestURL != options.ManifestURL {
		if err := removePendingCentralUpdate(pendingPath); err != nil {
			return fmt.Errorf("discard pending rollout for previous manifest URL: %w", err)
		}
		pending = nil
	}
	if pending != nil {
		switch version.Compare(options.Distribution.Version, pending.Catalog.Version) {
		case 0:
			if coordinator.HasNodesBehind(pending.Catalog.Version) {
				if err := coordinator.BroadcastCentralUpdate(ctx, UpgradeRequest{Central: pending}); err != nil {
					return fmt.Errorf("broadcast central release %s after orchestrator restart: %w", pending.Catalog.Version, err)
				}
			}
			if err := removePendingCentralUpdate(pendingPath); err != nil {
				return fmt.Errorf("clear completed central rollout marker: %w", err)
			}
			logging.Infof("update", "central release %s is active on the orchestrator; cluster rollout announced", pending.Catalog.Version)
			return nil
		case 1:
			if err := removePendingCentralUpdate(pendingPath); err != nil {
				return fmt.Errorf("clear superseded central rollout marker: %w", err)
			}
		}
	}

	catalog, err := FetchReleaseCatalog(ctx, options.Client, options.ManifestURL, options.PublisherPublicKey)
	if err != nil {
		return err
	}
	switch version.Compare(catalog.Version, options.Distribution.Version) {
	case -1:
		return nil
	case 0:
		if coordinator.HasNodesBehind(catalog.Version) {
			update := CentralUpdateRequest{ManifestURL: options.ManifestURL, Catalog: catalog}
			if err := coordinator.BroadcastCentralUpdate(ctx, UpgradeRequest{Central: &update}); err != nil {
				return fmt.Errorf("broadcast central release %s: %w", catalog.Version, err)
			}
		}
		return nil
	}

	update := CentralUpdateRequest{ManifestURL: options.ManifestURL, Catalog: catalog}
	if err := writePendingCentralUpdate(pendingPath, update); err != nil {
		return fmt.Errorf("persist central rollout before self-update: %w", err)
	}
	logging.Infof("update", "central release %s is newer than orchestrator %s; staging self-update before cluster rollout",
		catalog.Version, options.Distribution.Version)
	if err := options.QueueSelfUpdate(ctx, UpgradeRequest{Central: &update}); err != nil {
		return fmt.Errorf("queue central orchestrator self-update to %s: %w", catalog.Version, err)
	}
	return nil
}

func writePendingCentralUpdate(path string, update CentralUpdateRequest) error {
	if err := update.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(update, "", "  ")
	if err != nil {
		return fmt.Errorf("encode pending central update: %w", err)
	}
	data = append(data, '\n')
	file, err := os.CreateTemp(filepath.Dir(path), ".maistr0-central-update-*.json")
	if err != nil {
		return fmt.Errorf("create pending central update file: %w", err)
	}
	temporary := file.Name()
	defer func() { _ = os.Remove(temporary) }()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("restrict pending central update file: %w", err)
	}
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("write pending central update file: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("replace pending central update file: %w", err)
	}
	return nil
}

func readPendingCentralUpdate(path string, publisherKey ed25519.PublicKey) (*CentralUpdateRequest, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pending central update file: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var update CentralUpdateRequest
	if err := decoder.Decode(&update); err != nil {
		return nil, fmt.Errorf("decode pending central update file: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("pending central update file contains trailing JSON")
	}
	if err := update.Validate(); err != nil {
		return nil, err
	}
	if err := update.Catalog.VerifySignature(publisherKey); err != nil {
		return nil, fmt.Errorf("verify pending central update file: %w", err)
	}
	return &update, nil
}

func removePendingCentralUpdate(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
