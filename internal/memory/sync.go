package memory

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	SyncProtocolVersion      = 2
	FirstSyncProtocolVersion = 1
	SyncPageCursorHeader     = "X-Maistr0-Sync-Cursor"
	SyncPageUpperHeader      = "X-Maistr0-Sync-Upper-Watermarks"
)

const legacyOrigin = "legacy"

const (
	SyncVersionHeader    = "X-Maistr0-Sync-Version"
	SyncWatermarksHeader = "X-Maistr0-Sync-Watermarks"
)

func SupportsIncrementalSync(version int) bool {
	return version >= FirstSyncProtocolVersion && version <= SyncProtocolVersion
}

func SupportsSyncPages(version int) bool {
	return version == SyncProtocolVersion
}

func SetSyncRequestHeaders(request *http.Request, watermarks map[string]uint64) error {
	return SetSyncHeaders(request.Header, watermarks)
}

func SetSyncResponseHeaders(response http.ResponseWriter, watermarks map[string]uint64) error {
	return SetSyncHeaders(response.Header(), watermarks)
}

func SetSyncHeaders(headers http.Header, watermarks map[string]uint64) error {
	encoded, err := json.Marshal(watermarks)
	if err != nil {
		return err
	}
	headers.Set(SyncVersionHeader, strconv.Itoa(SyncProtocolVersion))
	headers.Set(SyncWatermarksHeader, base64.RawURLEncoding.EncodeToString(encoded))
	return nil
}

func ReadSyncRequestHeaders(request *http.Request) (int, map[string]uint64, error) {
	return ReadSyncHeaders(request.Header)
}

func ReadSyncResponseHeaders(response *http.Response) (int, map[string]uint64, error) {
	return ReadSyncHeaders(response.Header)
}

func SetSyncPageRequestHeaders(request *http.Request, cursor, upper map[string]uint64) error {
	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	upperBytes, err := json.Marshal(upper)
	if err != nil {
		return err
	}
	request.Header.Set(SyncPageCursorHeader, base64.RawURLEncoding.EncodeToString(cursorBytes))
	request.Header.Set(SyncPageUpperHeader, base64.RawURLEncoding.EncodeToString(upperBytes))
	return nil
}

func ReadSyncPageRequestHeaders(request *http.Request) (map[string]uint64, map[string]uint64, error) {
	cursorHeader := request.Header.Get(SyncPageCursorHeader)
	upperHeader := request.Header.Get(SyncPageUpperHeader)
	if cursorHeader == "" && upperHeader == "" {
		return nil, nil, nil
	}
	if cursorHeader == "" || upperHeader == "" {
		return nil, nil, fmt.Errorf("memory: incomplete sync page request headers")
	}
	cursor, err := decodeWatermarksHeader(cursorHeader, "sync page cursor")
	if err != nil {
		return nil, nil, err
	}
	upper, err := decodeWatermarksHeader(upperHeader, "sync page upper watermarks")
	if err != nil {
		return nil, nil, err
	}
	return cursor, upper, nil
}

func SetSyncPageResponseHeader(response http.ResponseWriter, cursor map[string]uint64) error {
	if len(cursor) == 0 {
		return nil
	}
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	response.Header().Set(SyncPageCursorHeader, base64.RawURLEncoding.EncodeToString(encoded))
	return nil
}

func ReadSyncPageResponseHeader(response *http.Response) (map[string]uint64, bool, error) {
	header := response.Header.Get(SyncPageCursorHeader)
	if header == "" {
		return nil, false, nil
	}
	cursor, err := decodeWatermarksHeader(header, "sync page cursor")
	return cursor, true, err
}

func decodeWatermarksHeader(header, name string) (map[string]uint64, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return nil, fmt.Errorf("memory: invalid %s encoding: %w", name, err)
	}
	var watermarks map[string]uint64
	if err := json.Unmarshal(encoded, &watermarks); err != nil {
		return nil, fmt.Errorf("memory: invalid %s: %w", name, err)
	}
	if watermarks == nil {
		watermarks = make(map[string]uint64)
	}
	return watermarks, nil
}

func ReadSyncHeaders(headers http.Header) (int, map[string]uint64, error) {
	versionHeader := headers.Get(SyncVersionHeader)
	if versionHeader == "" {
		return 0, nil, nil
	}
	version, err := strconv.Atoi(versionHeader)
	if err != nil {
		return 0, nil, fmt.Errorf("memory: invalid sync protocol version %q", versionHeader)
	}
	watermarks := make(map[string]uint64)
	header := headers.Get(SyncWatermarksHeader)
	if header == "" {
		return version, watermarks, nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return 0, nil, fmt.Errorf("memory: invalid sync watermarks encoding: %w", err)
	}
	if err := json.Unmarshal(encoded, &watermarks); err != nil {
		return 0, nil, fmt.Errorf("memory: invalid sync watermarks: %w", err)
	}
	return version, watermarks, nil
}

func SnapshotForPeer(store Store, peerVersion int, watermarks map[string]uint64) Snapshot {
	if !SupportsIncrementalSync(peerVersion) {
		return store.Snapshot()
	}
	localWatermarks := store.Watermarks()
	for origin, sequence := range watermarks {
		if sequence > localWatermarks[origin] {
			return store.Snapshot()
		}
	}
	return store.SnapshotSince(watermarks)
}

func snapshotSince(snapshot Snapshot, watermarks map[string]uint64) Snapshot {
	filtered := Snapshot{
		SyncVersion: SyncProtocolVersion,
		Watermarks:  cloneWatermarks(snapshot.Watermarks),
	}
	for _, experience := range snapshot.Experiences {
		if recordIsNewer(experience.Origin, experience.Sequence, watermarks) {
			filtered.Experiences = append(filtered.Experiences, experience)
		}
	}
	for _, fact := range snapshot.Facts {
		if recordIsNewer(fact.Origin, fact.Sequence, watermarks) {
			filtered.Facts = append(filtered.Facts, fact)
		}
	}
	for _, performance := range snapshot.Performances {
		if recordIsNewer(performance.Origin, performance.Sequence, watermarks) {
			filtered.Performances = append(filtered.Performances, performance)
		}
	}
	for _, dialogue := range snapshot.Dialogues {
		if recordIsNewer(dialogue.Origin, dialogue.Sequence, watermarks) {
			filtered.Dialogues = append(filtered.Dialogues, dialogue)
		}
	}
	return filtered
}

func recordIsNewer(origin string, sequence uint64, watermarks map[string]uint64) bool {
	return origin == "" || sequence == 0 || sequence > watermarks[origin]
}

func cloneWatermarks(watermarks map[string]uint64) map[string]uint64 {
	cloned := make(map[string]uint64, len(watermarks))
	for origin, sequence := range watermarks {
		cloned[origin] = sequence
	}
	return cloned
}

func mergeWatermarks(current, incoming map[string]uint64) map[string]uint64 {
	merged := cloneWatermarks(current)
	for origin, sequence := range incoming {
		if sequence > merged[origin] {
			merged[origin] = sequence
		}
	}
	return merged
}

func FilterMemoryWatermarks(watermarks map[string]uint64) map[string]uint64 {
	filtered := make(map[string]uint64)
	for origin, sequence := range watermarks {
		if !strings.HasPrefix(origin, "task:") {
			filtered[origin] = sequence
		}
	}
	return filtered
}
