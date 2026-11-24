package openhands

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const mappingVersion = 1

type sessionMapping struct {
	Version         int       `json:"version"`
	SessionID       string    `json:"session_id"`
	ConversationID  string    `json:"conversation_id"`
	Workspace       string    `json:"workspace"`
	LastSeq         int64     `json:"last_seq"`
	Pending         bool      `json:"pending"`
	PendingSeen     bool      `json:"pending_seen"`
	PendingStartSeq int64     `json:"pending_start_seq"`
	PendingSince    time.Time `json:"pending_since,omitempty"`
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

func saveMapping(directory string, mapping sessionMapping) error {
	if mapping.Version != mappingVersion || strings.TrimSpace(mapping.SessionID) == "" ||
		!validConversationID(mapping.ConversationID) || mapping.Workspace == "" || mapping.LastSeq < -1 ||
		mapping.PendingStartSeq < -1 {
		return ErrSessionMapping
	}
	data, err := json.Marshal(mapping)
	if err != nil {
		return ErrSessionMapping
	}
	temp, err := os.CreateTemp(directory, ".openhands-session-*")
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
		mapping.SessionID != sessionID || !validConversationID(mapping.ConversationID) ||
		mapping.Workspace == "" || mapping.LastSeq < -1 || mapping.PendingStartSeq < -1 {
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
