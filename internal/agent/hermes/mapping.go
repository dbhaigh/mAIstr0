package hermes

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const mappingVersion = 1

type sessionMapping struct {
	Version        int    `json:"version"`
	SessionID      string `json:"session_id"`
	Pending        bool   `json:"pending"`
	RunID          string `json:"run_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	LastSeq        int64  `json:"last_seq"`
}

func ensureMappingDirectory(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsRune(path, '\x00') {
		return "", ErrInvalidConfiguration
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", ErrSessionMapping
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrSessionMapping
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", ErrSessionMapping
	}
	return filepath.Clean(resolved), nil
}

func mappingFilename(sessionID string) string {
	digest := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(digest[:]) + ".json"
}

func validSessionID(value string) bool {
	return strings.HasPrefix(value, "hermes-") && len(value) <= 256 &&
		!strings.ContainsAny(value, "/\\\r\n\x00")
}

func validRunID(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func validIdempotencyKey(value string) bool {
	if value == "" || len(value) > 255 {
		return false
	}
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}

func validateMapping(mapping sessionMapping) error {
	if mapping.Version != mappingVersion || !validSessionID(mapping.SessionID) || mapping.LastSeq < -1 {
		return ErrSessionMapping
	}
	if mapping.Pending {
		if !validIdempotencyKey(mapping.IdempotencyKey) ||
			mapping.RunID != "" && !validRunID(mapping.RunID) {
			return ErrSessionMapping
		}
		return nil
	}
	if mapping.RunID != "" || mapping.IdempotencyKey != "" || mapping.LastSeq != -1 {
		return ErrSessionMapping
	}
	return nil
}

func saveMapping(directory string, mapping sessionMapping) error {
	if err := validateMapping(mapping); err != nil {
		return err
	}
	data, err := json.Marshal(mapping)
	if err != nil {
		return ErrSessionMapping
	}
	temp, err := os.CreateTemp(directory, ".hermes-session-*")
	if err != nil {
		return ErrSessionMapping
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return ErrSessionMapping
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return ErrSessionMapping
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return ErrSessionMapping
	}
	if err := temp.Close(); err != nil {
		return ErrSessionMapping
	}
	target := filepath.Join(directory, mappingFilename(mapping.SessionID))
	if info, err := os.Lstat(target); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return ErrSessionMapping
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrSessionMapping
	}
	if err := os.Rename(tempPath, target); err != nil {
		return ErrSessionMapping
	}
	return nil
}

func loadMapping(directory, sessionID string) (sessionMapping, error) {
	if !validSessionID(sessionID) {
		return sessionMapping{}, ErrSessionMapping
	}
	path := filepath.Join(directory, mappingFilename(sessionID))
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return sessionMapping{}, ErrSessionMapping
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return sessionMapping{}, ErrSessionMapping
	}
	var mapping sessionMapping
	if json.Unmarshal(data, &mapping) != nil || mapping.SessionID != sessionID ||
		validateMapping(mapping) != nil {
		return sessionMapping{}, ErrSessionMapping
	}
	return mapping, nil
}

func removeMapping(directory, sessionID string) error {
	if !validSessionID(sessionID) {
		return ErrSessionMapping
	}
	err := os.Remove(filepath.Join(directory, mappingFilename(sessionID)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrSessionMapping
	}
	return nil
}

func newSessionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", ErrSessionMapping
	}
	return "hermes-" + hex.EncodeToString(value), nil
}

func newIdempotencyKey() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", ErrSessionMapping
	}
	return "maistr0-" + hex.EncodeToString(value), nil
}
