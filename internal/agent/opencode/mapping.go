package opencode

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/maistr0/maistr0/internal/agent/execution"
)

const mappingVersion = 1

type sessionMapping struct {
	Version   int    `json:"version"`
	SessionID string `json:"session_id"`
	NativeID  string `json:"native_id"`
	CursorID  string `json:"cursor_id,omitempty"`
	PendingID string `json:"pending_id,omitempty"`
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

func canonicalDirectory(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsRune(path, '\x00') {
		return "", execution.ErrInvalidConfig
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", execution.ErrInvalidConfig
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", execution.ErrInvalidConfig
	}
	return filepath.Clean(resolved), nil
}

func pathsOverlap(first, second string) bool {
	return pathWithin(first, second) || pathWithin(second, first)
}

func pathWithin(parent, candidate string) bool {
	relative, err := filepath.Rel(parent, candidate)
	if err != nil {
		return false
	}
	if relative == "." {
		return true
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func mappingFilename(sessionID string) string {
	digest := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(digest[:]) + ".json"
}

func saveMapping(directory string, mapping sessionMapping) error {
	if mapping.Version != mappingVersion || mapping.SessionID == "" || !validNativeSessionID(mapping.NativeID) ||
		mapping.CursorID != "" && !validNativeMessageID(mapping.CursorID) ||
		mapping.PendingID != "" && !validNativeMessageID(mapping.PendingID) {
		return ErrSessionMapping
	}
	data, err := json.Marshal(mapping)
	if err != nil {
		return ErrSessionMapping
	}
	temp, err := os.CreateTemp(directory, ".opencode-session-*")
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
	if strings.TrimSpace(sessionID) == "" || strings.ContainsRune(sessionID, '\x00') {
		return sessionMapping{}, ErrSessionMapping
	}
	path := filepath.Join(directory, mappingFilename(sessionID))
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return sessionMapping{}, ErrSessionMapping
		}
		return sessionMapping{}, ErrSessionMapping
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return sessionMapping{}, ErrSessionMapping
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return sessionMapping{}, ErrSessionMapping
	}
	var mapping sessionMapping
	if json.Unmarshal(data, &mapping) != nil || mapping.Version != mappingVersion ||
		mapping.SessionID != sessionID || !validNativeSessionID(mapping.NativeID) ||
		mapping.CursorID != "" && !validNativeMessageID(mapping.CursorID) ||
		mapping.PendingID != "" && !validNativeMessageID(mapping.PendingID) {
		return sessionMapping{}, ErrSessionMapping
	}
	return mapping, nil
}

func removeMapping(directory, sessionID string) error {
	err := os.Remove(filepath.Join(directory, mappingFilename(sessionID)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrSessionMapping
	}
	return nil
}

func validNativeSessionID(value string) bool {
	return validNativeID(value, "ses_")
}

func validNativeMessageID(value string) bool {
	return validNativeID(value, "msg_")
}

func validNativeID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) || len(value) > 160 {
		return false
	}
	for _, char := range value[len(prefix):] {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return len(value) > len(prefix)
}
