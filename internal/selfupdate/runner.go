package selfupdate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

var Admission Gate

type Peer struct{ ID, Address string }

type Source struct {
	Identity *peeridentity.Identity
	Peers    []Peer
}

// Run stages at most one newer image at a time. Idle is checked only after
// fencing new work across both roles of the process.
func Run(ctx context.Context, source func() Source, idle func() bool, shutdown func()) {
	distribution, err := CurrentDistribution()
	if err != nil {
		logging.Errorf("update", "cannot locate installed executable: %v", err)
		return
	}
	var staged string
	var manifest Manifest
	var publisher, certificate string
	lastErrors := make(map[string]string)
	defer func() {
		if staged != "" {
			if err := os.Remove(staged); err != nil {
				logging.Warnf("update", "cannot remove staged image: %v", err)
			}
		}
	}()
	check := func() {
		if staged == "" {
			current := source()
			if current.Identity == nil {
				return
			}
			for _, peer := range current.Peers {
				parsed, err := url.Parse(peer.Address)
				if err != nil || parsed.Scheme != "https" || parsed.User != nil || !current.Identity.IsTrustedID(peer.ID) {
					continue
				}
				transport := &http.Transport{TLSClientConfig: current.Identity.PinnedClientTLSConfig(peer.ID)}
				client := &http.Client{Transport: transport}
				offeredCertificate := current.Identity.TrustedPeers()[peer.ID]
				path, offered, downloadErr := Download(ctx, client, peer.Address, distribution.Executable)
				transport.CloseIdleConnections()
				if downloadErr != nil {
					if !errors.Is(downloadErr, ErrNoUpdate) && lastErrors[peer.ID] != downloadErr.Error() {
						logging.Warnf("update", "peer %s update not staged: %v", peer.ID, downloadErr)
						lastErrors[peer.ID] = downloadErr.Error()
					}
					continue
				}
				staged, manifest = path, offered
				publisher, certificate = peer.ID, offeredCertificate
				delete(lastErrors, peer.ID)
				logging.Infof("update", "staged paired-peer release %s from %s; waiting for idle", manifest.Version, peer.ID)
				break
			}
		}
		if staged == "" || !Admission.Freeze() {
			return
		}
		current := source()
		if current.Identity == nil || certificate == "" || current.Identity.TrustedPeers()[publisher] != certificate {
			Admission.Thaw()
			logging.Warnf("update", "discarding staged release because publisher %s is no longer pinned", publisher)
			if err := os.Remove(staged); err != nil {
				logging.Warnf("update", "remove revoked staged image: %v", err)
			}
			staged = ""
			return
		}
		if !idle() {
			Admission.Thaw()
			return
		}
		if err := LaunchHelper(distribution.Executable, staged, manifest, os.Args[1:]); err != nil {
			Admission.Thaw()
			logging.Errorf("update", "cannot apply peer release %s: %v", manifest.Version, err)
			if err := os.Remove(staged); err != nil {
				logging.Warnf("update", "cannot remove rejected image: %v", err)
			}
			staged = ""
			return
		}
		logging.Infof("update", "idle process is restarting for paired-peer release %s", manifest.Version)
		shutdown()
		panic(fmt.Sprintf("peer update shutdown returned for release %s", manifest.Version))
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}
