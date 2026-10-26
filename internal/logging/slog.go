package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
)

var (
	processLevel = new(slog.LevelVar)
	urlUserInfo  = regexp.MustCompile(`(?i)(https?://)[^/\s@]+@`)
	secretQuery  = regexp.MustCompile(`(?i)([?&](?:api[_-]?key|token|auth(?:orization)?|password|secret|pin)=)[^&\s]+`)
)

type currentStandardLoggerWriter struct{}

func (currentStandardLoggerWriter) Write(value []byte) (int, error) {
	return log.Writer().Write(value)
}

func init() {
	processLevel.Set(slog.LevelInfo)
	standardOutput := log.Writer()
	slog.SetDefault(NewSlogLogger(currentStandardLoggerWriter{}, io.Discard, processLevel))
	log.SetOutput(standardOutput)
}

func NewSlogLogger(console, file io.Writer, level slog.Leveler) *slog.Logger {
	if console == nil {
		console = io.Discard
	}
	if file == nil {
		file = io.Discard
	}
	if level == nil {
		level = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(io.MultiWriter(NewConsoleWriter(console), file), &slog.HandlerOptions{
		Level: level,
	})
	return slog.New(handler)
}

func ConfigureSlog(console, file io.Writer) *slog.Logger {
	processLevel.Set(slog.LevelInfo)
	logger := NewSlogLogger(console, file, processLevel)
	slog.SetDefault(logger)
	stdlibLogger := slog.NewLogLogger(logger.Handler().WithAttrs([]slog.Attr{
		slog.String("component", "stdlib"),
	}), slog.LevelInfo)
	log.SetOutput(stdlibLogger.Writer())
	log.SetFlags(0)
	return logger
}

func SetLevel(value string) error {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		processLevel.Set(slog.LevelDebug)
	case "info":
		processLevel.Set(slog.LevelInfo)
	case "warn":
		processLevel.Set(slog.LevelWarn)
	case "error":
		processLevel.Set(slog.LevelError)
	default:
		return fmt.Errorf("log level must be debug, info, warn, or error")
	}
	return nil
}

func Infof(component, format string, args ...any) {
	logf(slog.LevelInfo, false, component, format, args...)
}

func Warnf(component, format string, args ...any) {
	logf(slog.LevelWarn, false, component, format, args...)
}

func Errorf(component, format string, args ...any) {
	logf(slog.LevelError, false, component, format, args...)
}

func FileInfof(component, format string, args ...any) {
	logf(slog.LevelInfo, true, component, format, args...)
}

func FileWarnf(component, format string, args ...any) {
	logf(slog.LevelWarn, true, component, format, args...)
}

func FileErrorf(component, format string, args ...any) {
	logf(slog.LevelError, true, component, format, args...)
}

func logf(level slog.Level, fileOnly bool, component, format string, args ...any) {
	if strings.HasPrefix(format, "FILE_ONLY: ") {
		fileOnly = true
		format = strings.TrimPrefix(format, "FILE_ONLY: ")
	}
	message, errorTypes := safeFormat(format, args)
	attrs := []any{"component", component}
	if fileOnly {
		attrs = append(attrs, "file_only", true)
	}
	if len(errorTypes) != 0 {
		attrs = append(attrs, "error_type", strings.Join(errorTypes, ","))
	}
	slog.Default().Log(context.Background(), level, message, attrs...)
}

func safeFormat(format string, args []any) (string, []string) {
	safeArgs := append([]any(nil), args...)
	errorTypes := make([]string, 0, len(args))
	for index, arg := range safeArgs {
		switch value := arg.(type) {
		case error:
			errorTypes = append(errorTypes, safeErrorType(value))
			safeArgs[index] = safeErrorType(value)
		case string:
			safeArgs[index] = redactURLSecrets(value)
		case []byte:
			safeArgs[index] = fmt.Sprintf("<%d bytes>", len(value))
		}
	}
	return fmt.Sprintf(format, safeArgs...), errorTypes
}

func safeErrorType(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return fmt.Sprintf("%T", err)
	}
}

func redactURLSecrets(value string) string {
	value = urlUserInfo.ReplaceAllString(value, `${1}[redacted]@`)
	value = secretQuery.ReplaceAllString(value, `${1}[redacted]`)
	if parsed, err := url.Parse(value); err == nil && parsed.Host != "" &&
		(parsed.Scheme == "http" || parsed.Scheme == "https") &&
		(parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "") {
		if parsed.User != nil {
			parsed.User = url.User("[redacted]")
		}
		if parsed.RawQuery != "" {
			parsed.RawQuery = "redacted"
		}
		if parsed.Fragment != "" {
			parsed.Fragment = "redacted"
			parsed.RawFragment = ""
		}
		value = parsed.String()
	}
	return value
}
