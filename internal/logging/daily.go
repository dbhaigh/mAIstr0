package logging

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const dateFormat = "2006-01-02"

type DailyFile struct {
	mu            sync.Mutex
	path          string
	file          *os.File
	date          string
	retentionDays int
}

func OpenDailyFile(path string, retentionDays int) (*DailyFile, error) {
	if retentionDays < 1 {
		return nil, errors.New("log retention must be at least one day")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	now := time.Now()
	log := &DailyFile{
		path:          path,
		file:          file,
		date:          now.Format(dateFormat),
		retentionDays: retentionDays,
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}
	if info.ModTime().Format(dateFormat) != log.date {
		log.date = info.ModTime().Format(dateFormat)
		if err := log.rotate(now); err != nil {
			if log.file != nil {
				_ = log.file.Close()
			}
			return nil, fmt.Errorf("rotate previous log file: %w", err)
		}
	} else if err := log.prune(now); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("prune old log files: %w", err)
	}
	return log, nil
}

func (l *DailyFile) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if today := now.Format(dateFormat); today != l.date {
		if err := l.rotate(now); err != nil {
			reportLogWriteError(err)
			return 0, err
		}
	}
	n, err := l.file.Write(p)
	if err != nil {
		reportLogWriteError(err)
	}
	return n, err
}

func (l *DailyFile) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *DailyFile) rotate(now time.Time) error {
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("flush current log: %w", err)
	}
	if err := l.file.Close(); err != nil {
		l.file = nil
		return fmt.Errorf("close current log: %w", err)
	}
	l.file = nil

	extension := filepath.Ext(l.path)
	name := strings.TrimSuffix(filepath.Base(l.path), extension)
	archive := fmt.Sprintf("%s-%s-%s%s", name, l.date, now.Format("150405.000000000"), extension)
	archivePath := filepath.Join(filepath.Dir(l.path), archive)
	if err := os.Rename(l.path, archivePath); err != nil {
		file, reopenErr := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		l.file = file
		return errors.Join(fmt.Errorf("archive current log: %w", err), reopenErr)
	}

	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create new daily log: %w", err)
	}
	l.file = file
	l.date = now.Format(dateFormat)
	if err := l.prune(now); err != nil {
		return fmt.Errorf("prune old log files: %w", err)
	}
	return nil
}

func (l *DailyFile) prune(now time.Time) error {
	files, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		return fmt.Errorf("read log directory: %w", err)
	}
	name := filepath.Base(l.path)
	extension := filepath.Ext(name)
	prefix := strings.TrimSuffix(name, extension) + "-"
	cutoff := time.Date(now.Year(), now.Month(), now.Day()-l.retentionDays+1, 0, 0, 0, 0, now.Location())
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), prefix) || !strings.HasSuffix(file.Name(), extension) {
			continue
		}
		archiveName := strings.TrimSuffix(strings.TrimPrefix(file.Name(), prefix), extension)
		if len(archiveName) < len(dateFormat)+1 || archiveName[len(dateFormat)] != '-' {
			continue
		}
		date, err := time.ParseInLocation(dateFormat, archiveName[:len(dateFormat)], now.Location())
		if err != nil || !date.Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(filepath.Dir(l.path), file.Name())); err != nil {
			return fmt.Errorf("remove expired log %s: %w", file.Name(), err)
		}
	}
	return nil
}

func reportLogWriteError(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "logging: failed to write or rotate daily log: %v\n", err)
}

var _ io.WriteCloser = (*DailyFile)(nil)
