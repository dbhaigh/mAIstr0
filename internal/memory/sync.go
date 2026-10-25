package memory

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	SyncProtocolVersion            = 3
	FirstSyncProtocolVersion       = 1
	SyncPageProtocolVersion        = 2
	SyncCompressionProtocolVersion = 3
	SyncCompressionThreshold       = 1024
	SyncPageCursorHeader           = "X-Maistr0-Sync-Cursor"
	SyncPageUpperHeader            = "X-Maistr0-Sync-Upper-Watermarks"
)

const legacyOrigin = "legacy"

var ErrSyncBodyTooLarge = errors.New("sync body exceeds configured limit")

const (
	SyncVersionHeader    = "X-Maistr0-Sync-Version"
	SyncWatermarksHeader = "X-Maistr0-Sync-Watermarks"
)

func SupportsIncrementalSync(version int) bool {
	return version >= FirstSyncProtocolVersion && version <= SyncProtocolVersion
}

func SupportsSyncPages(version int) bool {
	return version >= SyncPageProtocolVersion && version <= SyncProtocolVersion
}

func SupportsSyncCompression(version int) bool {
	return version >= SyncCompressionProtocolVersion && version <= SyncProtocolVersion
}

func SupportsEmptySync(version int) bool {
	return SupportsSyncCompression(version)
}

func SetSyncCompressionRequestHeader(request *http.Request) {
	request.Header.Set("Accept-Encoding", "gzip")
}

func AcceptsSyncCompression(request *http.Request) bool {
	for _, value := range strings.Split(request.Header.Get("Accept-Encoding"), ",") {
		parts := strings.SplitN(strings.TrimSpace(value), ";", 2)
		if !strings.EqualFold(strings.TrimSpace(parts[0]), "gzip") {
			continue
		}
		if len(parts) == 1 {
			return true
		}
		parameter := strings.TrimSpace(parts[1])
		if strings.HasPrefix(strings.ToLower(parameter), "q=") {
			quality, err := strconv.ParseFloat(strings.TrimSpace(parameter[2:]), 64)
			if err == nil && quality > 0 {
				return true
			}
		}
	}
	return false
}

func EncodeSyncBody(body []byte, peerVersion int, accepted bool) ([]byte, bool, error) {
	if !accepted || !SupportsSyncCompression(peerVersion) || len(body) < SyncCompressionThreshold {
		return body, false, nil
	}
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, false, fmt.Errorf("create gzip sync body: %w", err)
	}
	if _, err := writer.Write(body); err != nil {
		return nil, false, fmt.Errorf("compress sync body: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, false, fmt.Errorf("finish gzip sync body: %w", err)
	}
	if compressed.Len() >= len(body) {
		return body, false, nil
	}
	return compressed.Bytes(), true, nil
}

func DecodeSyncBody(reader io.Reader, contentEncoding string, protocolVersion int, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("sync body limit must not be negative")
	}
	encoded, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read sync body: %w", err)
	}
	if int64(len(encoded)) > maxBytes {
		return nil, fmt.Errorf("%w: limit is %d bytes", ErrSyncBodyTooLarge, maxBytes)
	}
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "":
		return encoded, nil
	case "gzip":
		if !SupportsSyncCompression(protocolVersion) {
			return nil, fmt.Errorf("gzip sync body requires protocol version %d", SyncCompressionProtocolVersion)
		}
		reader, err := gzip.NewReader(bytes.NewReader(encoded))
		if err != nil {
			return nil, fmt.Errorf("open gzip sync body: %w", err)
		}
		decoded, readErr := io.ReadAll(io.LimitReader(reader, maxBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("decompress sync body: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close gzip sync body: %w", closeErr)
		}
		if int64(len(decoded)) > maxBytes {
			return nil, fmt.Errorf("%w after decompression: limit is %d bytes", ErrSyncBodyTooLarge, maxBytes)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported sync content encoding %q", contentEncoding)
	}
}

func WriteSyncJSON(w http.ResponseWriter, status int, value any, peerVersion int, acceptsCompression bool) (int, error) {
	body, err := json.Marshal(value)
	if err != nil {
		wrapped := fmt.Errorf("marshal sync response: %w", err)
		http.Error(w, "could not encode sync response", http.StatusInternalServerError)
		return 0, wrapped
	}
	body, compressed, err := EncodeSyncBody(body, peerVersion, acceptsCompression)
	if err != nil {
		http.Error(w, "could not encode sync response", http.StatusInternalServerError)
		return 0, err
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if compressed {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.WriteHeader(status)
	written, err := w.Write(body)
	if err != nil {
		return written, fmt.Errorf("write sync response: %w", err)
	}
	return written, nil
}

func SetSyncRequestHeaders(request *http.Request, watermarks map[string]uint64) error {
	return SetSyncHeaders(request.Header, watermarks)
}

func SetSyncResponseHeaders(response http.ResponseWriter, watermarks map[string]uint64) error {
	return SetSyncHeaders(response.Header(), watermarks)
}

func SetSyncResponseHeadersForPeer(response http.ResponseWriter, watermarks map[string]uint64, peerVersion int) error {
	version := SyncProtocolVersion
	switch {
	case peerVersion <= 0:
		version = FirstSyncProtocolVersion
	case peerVersion < SyncProtocolVersion:
		version = peerVersion
	}
	return SetSyncHeadersVersion(response.Header(), watermarks, version)
}

func SetSyncHeaders(headers http.Header, watermarks map[string]uint64) error {
	return SetSyncHeadersVersion(headers, watermarks, SyncProtocolVersion)
}

func SetSyncHeadersVersion(headers http.Header, watermarks map[string]uint64, version int) error {
	encoded, err := json.Marshal(watermarks)
	if err != nil {
		return err
	}
	headers.Set(SyncVersionHeader, strconv.Itoa(version))
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

func PayloadFromSnapshot(snapshot Snapshot) (Payload, error) {
	recordIDs, err := RecordIDs(snapshot)
	if err != nil {
		return Payload{}, err
	}
	return Payload{RecordIDs: recordIDs, Snapshot: snapshot}, nil
}

func PayloadDifference(payload Payload, excludedRecordIDs map[string]struct{}) (Payload, error) {
	filtered := Snapshot{
		Experiences:  make([]Experience, 0),
		Facts:        make([]Fact, 0),
		Performances: make([]Performance, 0),
		Dialogues:    make([]DialogueRecord, 0),
		SyncVersion:  payload.Snapshot.SyncVersion,
		Watermarks:   cloneWatermarks(payload.Snapshot.Watermarks),
	}
	for _, experience := range payload.Snapshot.Experiences {
		if _, exists := excludedRecordIDs[string(experienceFingerprintKey(experience))]; !exists {
			filtered.Experiences = append(filtered.Experiences, experience)
		}
	}
	for _, fact := range payload.Snapshot.Facts {
		id, err := syncRecordID("fact", fact.Origin, fact.Sequence, fact)
		if err != nil {
			return Payload{}, err
		}
		if _, exists := excludedRecordIDs[id]; !exists {
			filtered.Facts = append(filtered.Facts, fact)
		}
	}
	for _, performance := range payload.Snapshot.Performances {
		id, err := syncRecordID("performance", performance.Origin, performance.Sequence, performance)
		if err != nil {
			return Payload{}, err
		}
		if _, exists := excludedRecordIDs[id]; !exists {
			filtered.Performances = append(filtered.Performances, performance)
		}
	}
	for _, dialogue := range payload.Snapshot.Dialogues {
		id, err := syncRecordID("dialogue", dialogue.Origin, dialogue.Sequence, dialogue)
		if err != nil {
			return Payload{}, err
		}
		if _, exists := excludedRecordIDs[id]; !exists {
			filtered.Dialogues = append(filtered.Dialogues, dialogue)
		}
	}
	return PayloadFromSnapshot(filtered)
}

func RecordIDs(snapshot Snapshot) ([]string, error) {
	recordIDs := make([]string, 0, len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Performances)+len(snapshot.Dialogues))
	seen := make(map[string]struct{})
	appendID := func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		recordIDs = append(recordIDs, id)
	}
	for _, experience := range snapshot.Experiences {
		appendID(string(experienceFingerprintKey(experience)))
	}
	for _, fact := range snapshot.Facts {
		id, err := syncRecordID("fact", fact.Origin, fact.Sequence, fact)
		if err != nil {
			return nil, err
		}
		appendID(id)
	}
	for _, performance := range snapshot.Performances {
		id, err := syncRecordID("performance", performance.Origin, performance.Sequence, performance)
		if err != nil {
			return nil, err
		}
		appendID(id)
	}
	for _, dialogue := range snapshot.Dialogues {
		id, err := syncRecordID("dialogue", dialogue.Origin, dialogue.Sequence, dialogue)
		if err != nil {
			return nil, err
		}
		appendID(id)
	}
	return recordIDs, nil
}

func CombinePayloads(payloads []Payload) (Payload, error) {
	combined := Payload{Snapshot: Snapshot{
		Experiences:  make([]Experience, 0),
		Facts:        make([]Fact, 0),
		Performances: make([]Performance, 0),
		Dialogues:    make([]DialogueRecord, 0),
		Watermarks:   make(map[string]uint64),
	}}
	seen := make(map[string]struct{})
	appendID := func(id string) bool {
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
		combined.RecordIDs = append(combined.RecordIDs, id)
		return true
	}
	for _, payload := range payloads {
		if payload.Snapshot.SyncVersion > combined.Snapshot.SyncVersion {
			combined.Snapshot.SyncVersion = payload.Snapshot.SyncVersion
		}
		combined.Snapshot.Watermarks = mergeWatermarks(combined.Snapshot.Watermarks, payload.Snapshot.Watermarks)
		for _, experience := range payload.Snapshot.Experiences {
			if appendID(string(experienceFingerprintKey(experience))) {
				combined.Snapshot.Experiences = append(combined.Snapshot.Experiences, experience)
			}
		}
		for _, fact := range payload.Snapshot.Facts {
			id, err := syncRecordID("fact", fact.Origin, fact.Sequence, fact)
			if err != nil {
				return Payload{}, err
			}
			if appendID(id) {
				combined.Snapshot.Facts = append(combined.Snapshot.Facts, fact)
			}
		}
		for _, performance := range payload.Snapshot.Performances {
			id, err := syncRecordID("performance", performance.Origin, performance.Sequence, performance)
			if err != nil {
				return Payload{}, err
			}
			if appendID(id) {
				combined.Snapshot.Performances = append(combined.Snapshot.Performances, performance)
			}
		}
		for _, dialogue := range payload.Snapshot.Dialogues {
			id, err := syncRecordID("dialogue", dialogue.Origin, dialogue.Sequence, dialogue)
			if err != nil {
				return Payload{}, err
			}
			if appendID(id) {
				combined.Snapshot.Dialogues = append(combined.Snapshot.Dialogues, dialogue)
			}
		}
	}
	return combined, nil
}

func SnapshotDifference(snapshot, known Snapshot) (Snapshot, error) {
	knownIDs, err := RecordIDs(known)
	if err != nil {
		return Snapshot{}, err
	}
	knownSet := make(map[string]struct{}, len(knownIDs))
	for _, id := range knownIDs {
		knownSet[id] = struct{}{}
	}
	payload, err := PayloadFromSnapshot(snapshot)
	if err != nil {
		return Snapshot{}, err
	}
	difference, err := PayloadDifference(payload, knownSet)
	if err != nil {
		return Snapshot{}, err
	}
	return difference.Snapshot, nil
}

func syncRecordID(kind, origin string, sequence uint64, record any) (string, error) {
	if origin != "" && sequence != 0 {
		return fmt.Sprintf("%s:%q:%d", kind, origin, sequence), nil
	}
	body, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("memory: encode %s record identity: %w", kind, err)
	}
	return fmt.Sprintf("%s:legacy:%x", kind, sha256.Sum256(body)), nil
}
