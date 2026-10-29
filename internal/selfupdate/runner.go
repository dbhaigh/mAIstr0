package selfupdate

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/version"
)

var Admission Gate

type Peer struct{ ID, Address string }

type peerRelease struct {
	peer        Peer
	manifest    Manifest
	certificate string
}

type Source struct {
	Identity           *peeridentity.Identity
	Peers              []Peer
	PublisherPublicKey ed25519.PublicKey
}

// Run stages at most one newer image at a time. Idle is checked only after
// fencing new work across both roles of the process.
func Run(ctx context.Context, source func() Source, idle func() bool, shutdown func()) {
	RunWithControllerHandoff(ctx, source, idle, shutdown, ControllerHandoff{})
}

func RunWithControllerHandoff(ctx context.Context, source func() Source, idle func() bool, shutdown func(), handoff ControllerHandoff) {
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
			offers := make([]peerRelease, 0, len(current.Peers))
			for _, peer := range current.Peers {
				parsed, err := url.Parse(peer.Address)
				if err != nil || parsed.Scheme != "https" || parsed.User != nil || !current.Identity.IsTrustedID(peer.ID) {
					continue
				}
				transport := &http.Transport{TLSClientConfig: current.Identity.PinnedClientTLSConfig(peer.ID)}
				client := httpx.NewClient(httpx.ClientOptions{Transport: transport})
				offeredCertificate := current.Identity.TrustedPeers()[peer.ID]
				var offered Manifest
				var downloadErr error
				if len(current.PublisherPublicKey) == 0 {
					offered, downloadErr = fetchManifest(ctx, client, peer.Address)
				} else {
					offered, downloadErr = fetchManifest(ctx, client, peer.Address, current.PublisherPublicKey)
				}
				transport.CloseIdleConnections()
				if downloadErr != nil {
					if !errors.Is(downloadErr, ErrNoUpdate) && lastErrors[peer.ID] != downloadErr.Error() {
						logging.Warnf("update", "peer %s update manifest unavailable: %v", peer.ID, downloadErr)
						lastErrors[peer.ID] = downloadErr.Error()
					}
					continue
				}
				delete(lastErrors, peer.ID)
				if version.Compare(offered.Version, distribution.Version) > 0 {
					offers = append(offers, peerRelease{peer: peer, manifest: offered, certificate: offeredCertificate})
				}
			}
			for _, offer := range newestPeerReleases(distribution.Version, offers) {
				transport := &http.Transport{TLSClientConfig: current.Identity.PinnedClientTLSConfig(offer.peer.ID)}
				client := httpx.NewClient(httpx.ClientOptions{Transport: transport})
				path, downloadErr := downloadManifest(ctx, client, offer.peer.Address, distribution.Executable, offer.manifest)
				transport.CloseIdleConnections()
				if downloadErr != nil {
					if lastErrors[offer.peer.ID] != downloadErr.Error() {
						logging.Warnf("update", "peer %s release %s not staged: %v", offer.peer.ID, offer.manifest.Version, downloadErr)
						lastErrors[offer.peer.ID] = downloadErr.Error()
					}
					continue
				}
				staged, manifest = path, offer.manifest
				publisher, certificate = offer.peer.ID, offer.certificate
				delete(lastErrors, offer.peer.ID)
				logging.Infof("update", "staged newest paired-peer release %s from %s; waiting for idle", manifest.Version, publisher)
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
		if err := LaunchHelperWithControllerHandoff(distribution.Executable, staged, manifest, os.Args[1:], handoff); err != nil {
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

func newestPeerReleases(currentVersion string, offers []peerRelease) []peerRelease {
	var newest []peerRelease
	for _, offer := range offers {
		if version.Compare(offer.manifest.Version, currentVersion) <= 0 {
			continue
		}
		if len(newest) == 0 || version.Compare(offer.manifest.Version, newest[0].manifest.Version) > 0 {
			newest = []peerRelease{offer}
		} else if version.Compare(offer.manifest.Version, newest[0].manifest.Version) == 0 {
			newest = append(newest, offer)
		}
	}
	return newest
}
