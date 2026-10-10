package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDailyFileRotatesAndRetainsSevenDays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "maistr0.log")
	log, err := OpenDailyFile(path, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err := log.Write([]byte("before rollover\n")); err != nil {
		t.Fatal(err)
	}

	yesterday := time.Now().AddDate(0, 0, -1)
	log.date = yesterday.Format(dateFormat)
	if _, err := log.Write([]byte("after rollover\n")); err != nil {
		t.Fatal(err)
	}
	archives, err := filepath.Glob(filepath.Join(filepath.Dir(path), "maistr0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 1 || !strings.Contains(filepath.Base(archives[0]), yesterday.Format(dateFormat)) {
		t.Fatalf("daily archives = %v, want one archive for %s", archives, yesterday.Format(dateFormat))
	}
	archived, err := os.ReadFile(archives[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(archived) != "before rollover\n" {
		t.Fatalf("archived log = %q", archived)
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(current) != "after rollover\n" {
		t.Fatalf("current log = %q", current)
	}
}

func TestDailyFilePrunesArchivesOlderThanSevenCalendarDays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "maistr0.log")
	log, err := OpenDailyFile(path, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	today := time.Now()
	for daysAgo := 0; daysAgo < 10; daysAgo++ {
		day := today.AddDate(0, 0, -daysAgo).Format(dateFormat)
		name := "maistr0-" + day + "-120000.000000000.log"
		if err := os.WriteFile(filepath.Join(dir, name), []byte("archive"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.prune(today); err != nil {
		t.Fatal(err)
	}
	archives, err := filepath.Glob(filepath.Join(dir, "maistr0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 7 {
		t.Fatalf("retained %d archives, want 7", len(archives))
	}
	cutoff := today.AddDate(0, 0, -6).Format(dateFormat)
	for _, archive := range archives {
		day := filepath.Base(archive)[len("maistr0-") : len("maistr0-")+len(dateFormat)]
		if day < cutoff {
			t.Errorf("expired archive retained: %s", filepath.Base(archive))
		}
	}
}
