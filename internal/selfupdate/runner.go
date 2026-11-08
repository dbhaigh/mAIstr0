package selfupdate

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/version"
)

var Admission Gate

var (
	ErrUpgradeInProgress   = errors.New("an upgrade request is already queued")
	ErrPeerUpdatesDisabled = errors.New("peer updates are disabled")
)

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
	PrepareRejoin      func(context.Context) error
	ClearRejoin        func(context.Context) error
	ReportStatus       func(cluster.UpgradeStatus)
}

type UpgradeRequest struct {
	Path   string `json:"path,omitempty"`
	PeerID string `json:"peer_id,omitempty"`
}

func (request UpgradeRequest) Validate() error {
	if err := request.ValidateSource(); err != nil {
		return err
	}
	if strings.TrimSpace(request.Path) != "" && (!filepath.IsAbs(request.Path) || len(request.Path) > 4096) {
		return errors.New("upgrade path must be an absolute local path")
	}
	return nil
}

func (request UpgradeRequest) ValidateSource() error {
	hasPath := strings.TrimSpace(request.Path) != ""
	hasPeer := strings.TrimSpace(request.PeerID) != ""
	if hasPath == hasPeer {
		return errors.New("exactly one upgrade source path or peer_id is required")
	}
	if hasPath && len(request.Path) > 4096 {
		return errors.New("upgrade path is too long")
	}
	if hasPeer && (request.PeerID != strings.TrimSpace(request.PeerID) || len(request.PeerID) > 255) {
		return errors.New("upgrade peer_id is invalid")
	}
	return nil
}

// Run stages at most one newer image at a time. Idle is checked only after
// fencing new work across both roles of the process.
func Run(ctx context.Context, source func() Source, idle func() bool, shutdown func()) {
	RunWithControllerHandoff(ctx, source, idle, shutdown, ControllerHandoff{})
}

func RunWithControllerHandoff(ctx context.Context, source func() Source, idle func() bool, shutdown func(), handoff ControllerHandoff) {
	RunWithControllerHandoffAndRequests(ctx, source, idle, shutdown, handoff, nil, true)
}

// RunWithControllerHandoffAndRequests serves explicitly requested upgrades
// even when automatic peer-update checks are disabled.
func RunWithControllerHandoffAndRequests(
	ctx context.Context,
	source func() Source,
	idle func() bool,
	shutdown func(),
	handoff ControllerHandoff,
	requests <-chan UpgradeRequest,
	peerUpdatesEnabled bool,
) {
	distribution, err := CurrentDistribution()
	if err != nil {
		reportUpgradeStatus(source(), cluster.UpgradeStatus{
			State: "failed", Phase: "locating_executable", TicketStatus: "not_requested",
			FailureReason: err.Error(),
		})
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
		if current.PrepareRejoin == nil {
			Admission.Thaw()
			logging.Errorf("update", "cannot apply peer release %s without an upgrade rejoin ticket provider", manifest.Version)
			return
		}
		if err := current.PrepareRejoin(ctx); err != nil {
			Admission.Thaw()
			logging.Errorf("update", "cannot prepare rejoin for peer release %s: %v", manifest.Version, err)
			return
		}
		if err := LaunchHelperWithControllerHandoff(distribution.Executable, staged, manifest, os.Args[1:], handoff); err != nil {
			Admission.Thaw()
			if current.ClearRejoin != nil {
				if clearErr := current.ClearRejoin(ctx); clearErr != nil {
					logging.Errorf("update", "cannot clear rejected rejoin ticket: %v", clearErr)
				}
			}
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
	var tick <-chan time.Time
	if peerUpdatesEnabled {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			check()
		case request, ok := <-requests:
			if !ok {
				requests = nil
				continue
			}
			if err := request.Validate(); err != nil {
				reportUpgradeStatus(source(), cluster.UpgradeStatus{
					State: "failed", Phase: "validating_source", TicketStatus: "not_requested",
					FailureReason: err.Error(),
				})
				logging.Warnf("update", "rejected manual upgrade request: %v", err)
				continue
			}
			if request.PeerID != "" && !peerUpdatesEnabled {
				reportUpgradeStatus(source(), cluster.UpgradeStatus{
					State: "failed", Phase: "validating_source", TicketStatus: "not_requested",
					FailureReason: ErrPeerUpdatesDisabled.Error(),
				})
				logging.Warnf("update", "rejected peer upgrade request while peer updates are disabled")
				continue
			}
			if err := applyRequestedUpgrade(ctx, request, distribution, source, idle, shutdown, handoff); err != nil {
				logging.Errorf("update", "manual upgrade failed: %v", err)
			}
		}
	}
}

func applyRequestedUpgrade(
	ctx context.Context,
	request UpgradeRequest,
	distribution Distribution,
	source func() Source,
	idle func() bool,
	shutdown func(),
	handoff ControllerHandoff,
) (resultErr error) {
	current := source()
	phase := "validating_source"
	progress := 0
	ticketStatus := "not_requested"
	report := func(state, reason string) {
		reportUpgradeStatus(current, cluster.UpgradeStatus{
			State: state, Phase: phase, Progress: progress,
			TicketStatus: ticketStatus, FailureReason: reason,
		})
	}
	report("running", "")
	defer func() {
		if resultErr != nil {
			if ticketStatus == "requesting" {
				ticketStatus = "failed"
			}
			report("failed", resultErr.Error())
		}
	}()
	var staged string
	var manifest Manifest
	publisher, certificate := "", ""
	if request.Path != "" {
		phase = "staging_local_binary"
		progress = 5
		report("running", "")
		path, inspected, err := stageLocalBinary(ctx, request.Path, distribution)
		if err != nil {
			return err
		}
		staged, manifest = path, inspected
	} else {
		phase = "staging_peer_binary"
		progress = 5
		report("running", "")
		if current.Identity == nil {
			return errors.New("node identity is unavailable for peer update")
		}
		if !current.Identity.IsTrustedID(request.PeerID) {
			return errors.New("upgrade source is not a paired peer")
		}
		var peer *Peer
		for i := range current.Peers {
			if current.Peers[i].ID == request.PeerID {
				peer = &current.Peers[i]
				break
			}
		}
		if peer == nil {
			return errors.New("upgrade source is not currently available")
		}
		parsed, err := url.Parse(peer.Address)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
			(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("upgrade source must be a paired HTTPS peer")
		}
		transport := &http.Transport{TLSClientConfig: current.Identity.PinnedClientTLSConfig(peer.ID)}
		defer transport.CloseIdleConnections()
		client := httpx.NewClient(httpx.ClientOptions{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("peer update redirects are not permitted")
			},
		})
		if len(current.PublisherPublicKey) == 0 {
			manifest, err = fetchManifest(ctx, client, peer.Address)
		} else {
			manifest, err = fetchManifest(ctx, client, peer.Address, current.PublisherPublicKey)
		}
		if err != nil {
			return fmt.Errorf("read peer update manifest: %w", err)
		}
		if version.Compare(manifest.Version, distribution.Version) <= 0 {
			return ErrNoUpdate
		}
		staged, err = downloadManifest(ctx, client, peer.Address, distribution.Executable, manifest)
		if err != nil {
			return fmt.Errorf("stage peer update: %w", err)
		}
		publisher = peer.ID
		certificate = current.Identity.TrustedPeers()[publisher]
		if certificate == "" {
			_ = os.Remove(staged)
			return errors.New("peer update source is no longer pinned")
		}
	}
	progress = 40
	phase = "binary_staged"
	report("running", "")
	keepStaged := false
	defer func() {
		if !keepStaged && staged != "" {
			if err := os.Remove(staged); err != nil && !errors.Is(err, os.ErrNotExist) {
				logging.Warnf("update", "cannot remove failed manual upgrade image: %v", err)
			}
		}
	}()

	phase = "waiting_for_idle"
	progress = 55
	report("running", "")
	if err := waitForUpgradeIdle(ctx, idle); err != nil {
		return err
	}
	frozen := true
	defer func() {
		if frozen {
			Admission.Thaw()
		}
	}()
	current = source()
	if publisher != "" &&
		(current.Identity == nil || current.Identity.TrustedPeers()[publisher] != certificate) {
		return fmt.Errorf("paired update publisher %s is no longer pinned", publisher)
	}
	if current.PrepareRejoin == nil {
		return errors.New("node cannot request a rejoin ticket from its current orchestrator")
	}
	phase = "requesting_rejoin_ticket"
	progress = 75
	ticketStatus = "requesting"
	report("running", "")
	if err := current.PrepareRejoin(ctx); err != nil {
		ticketStatus = "failed"
		return fmt.Errorf("prepare trusted rejoin before stopping: %w", err)
	}
	ticketStatus = "ready"
	phase = "rejoin_ticket_ready"
	progress = 85
	report("running", "")
	phase = "replacing_binary"
	progress = 90
	report("restarting", "")
	if err := LaunchHelperWithControllerHandoff(distribution.Executable, staged, manifest, os.Args[1:], handoff); err != nil {
		ticketStatus = "failed"
		if current.ClearRejoin != nil {
			if clearErr := current.ClearRejoin(ctx); clearErr != nil {
				logging.Errorf("update", "cannot clear rejected rejoin ticket: %v", clearErr)
			}
		}
		return fmt.Errorf("launch replacement helper: %w", err)
	}
	keepStaged = true
	frozen = false
	phase = "restarting"
	progress = 95
	report("restarting", "")
	logging.Infof("update", "restarting for explicitly requested release %s", manifest.Version)
	shutdown()
	panic(fmt.Sprintf("manual upgrade shutdown returned for release %s", manifest.Version))
}

func waitForUpgradeIdle(ctx context.Context, idle func() bool) error {
	for {
		if Admission.Freeze() {
			if idle() {
				return nil
			}
			Admission.Thaw()
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func reportUpgradeStatus(source Source, status cluster.UpgradeStatus) {
	if source.ReportStatus == nil {
		return
	}
	status.UpdatedAt = time.Now().UTC()
	source.ReportStatus(status)
}

func stageLocalBinary(ctx context.Context, source string, distribution Distribution) (string, Manifest, error) {
	source, err := filepath.Abs(source)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("resolve local upgrade path: %w", err)
	}
	resolvedSource, err := filepath.EvalSymlinks(source)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("resolve local upgrade executable: %w", err)
	}
	sourceFile, err := os.Open(resolvedSource)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("open local upgrade executable: %w", err)
	}
	defer closeutil.Discard(sourceFile)
	sourceInfo, err := sourceFile.Stat()
	if err != nil {
		return "", Manifest{}, fmt.Errorf("inspect local upgrade executable: %w", err)
	}
	if !sourceInfo.Mode().IsRegular() {
		return "", Manifest{}, errors.New("local upgrade source must be a regular file")
	}
	targetInfo, err := os.Stat(distribution.Executable)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("inspect installed executable: %w", err)
	}
	if os.SameFile(sourceInfo, targetInfo) {
		return "", Manifest{}, errors.New("local upgrade source is the currently installed executable")
	}
	if err := ctx.Err(); err != nil {
		return "", Manifest{}, err
	}
	file, err := os.CreateTemp(filepath.Dir(distribution.Executable), ".maistr0-update-*"+filepath.Ext(distribution.Executable))
	if err != nil {
		return "", Manifest{}, fmt.Errorf("create staged local executable: %w", err)
	}
	path := file.Name()
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	size, copyErr := io.Copy(file, io.LimitReader(sourceFile, MaxBinaryBytes+1))
	if err := errors.Join(copyErr, file.Sync(), file.Close()); err != nil {
		return "", Manifest{}, fmt.Errorf("copy local upgrade executable: %w", err)
	}
	if size <= 0 || size > MaxBinaryBytes {
		return "", Manifest{}, errors.New("local upgrade executable has an unsupported size")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return "", Manifest{}, fmt.Errorf("make staged local executable runnable: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", Manifest{}, err
	}
	release, err := executableVersion(path)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("read local upgrade executable version: %w", err)
	}
	if version.Compare(release, distribution.Version) <= 0 {
		return "", Manifest{}, ErrNoUpdate
	}
	manifest, err := Inspect(path, release)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("validate local upgrade executable: %w", err)
	}
	if manifest.OS != runtime.GOOS || manifest.Arch != runtime.GOARCH {
		return "", Manifest{}, errors.New("local upgrade executable platform does not match this node")
	}
	if err := ctx.Err(); err != nil {
		return "", Manifest{}, err
	}
	ok = true
	return path, manifest, nil
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
