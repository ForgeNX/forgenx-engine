/*
 * Copyright 2026 Scott Walter, MMFP Solutions LLC
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See LICENSE for more details.
 */

package logging

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Level represents the severity of a log message.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

var (
	globalLevel Level = LevelInfo
	globalMu    sync.RWMutex
)

// File sink: when configured via SetLogFile, every log line is written to this
// file (in addition to stdout) so logs survive container recreation. The file
// is size-rotated to bound disk usage. Writes are mutex-guarded and buffered by
// the OS (no per-line fsync), so the cost over stdout-only is negligible.
var (
	fileMu       sync.Mutex
	logFile      *os.File
	logFilePath  string
	logFileSize  int64
	maxFileBytes int64 = 50 * 1024 * 1024 // 50 MB per file
	maxFileKeep  int   = 5                // engine.log + .1 .. .4  -> 250 MB max
)

// SetLogFile enables durable file logging at the given path (in addition to
// stdout). Safe to call once at startup. If the file can't be opened, logging
// continues to stdout only. Passing an empty path disables file logging.
func SetLogFile(path string) {
	fileMu.Lock()
	defer fileMu.Unlock()
	if logFile != nil {
		logFile.Close()
		logFile = nil
	}
	logFilePath = path
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stdout, "[%s] [main] [WARN] could not open log file %q: %v (stdout only)\n",
			time.Now().Format("2006-01-02 15:04:05"), path, err)
		return
	}
	logFile = f
	if fi, err := f.Stat(); err == nil {
		logFileSize = fi.Size()
	}
}

// writeFile appends a line to the log file, rotating first if it would exceed
// the size cap. No-op if file logging is disabled.
func writeFile(line string) {
	fileMu.Lock()
	defer fileMu.Unlock()
	if logFile == nil {
		return
	}
	if logFileSize+int64(len(line)) > maxFileBytes {
		rotateLocked()
	}
	n, err := logFile.WriteString(line)
	if err == nil {
		logFileSize += int64(n)
	}
}

// rotateLocked rolls engine.log -> engine.log.1 -> ... dropping the oldest,
// then reopens a fresh engine.log. Caller must hold fileMu.
func rotateLocked() {
	if logFile == nil || logFilePath == "" {
		return
	}
	logFile.Close()
	logFile = nil
	for i := maxFileKeep - 1; i >= 1; i-- {
		older := fmt.Sprintf("%s.%d", logFilePath, i)
		newer := logFilePath
		if i > 1 {
			newer = fmt.Sprintf("%s.%d", logFilePath, i-1)
		}
		os.Rename(newer, older)
	}
	f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stdout, "[%s] [main] [WARN] log rotation reopen failed: %v\n",
			time.Now().Format("2006-01-02 15:04:05"), err)
		return
	}
	logFile = f
	logFileSize = 0
}

// Module constants for consistent tagging.
const (
	ModuleMain    = "main"
	ModuleStratum = "stratum"
	ModuleEngine  = "engine"
	ModuleCoin    = "coin"
	ModuleRPC     = "rpc"
	ModuleMetrics = "metrics"
	ModuleConfig  = "config"
	ModuleZMQ     = "zmq"
)

// Logger provides module-tagged leveled logging to stdout.
type Logger struct {
	module string
}

// New creates a new Logger with the given module tag.
func New(module string) *Logger {
	return &Logger{module: module}
}

// SetGlobalLevel sets the minimum log level for all loggers.
func SetGlobalLevel(level string) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalLevel = ParseLevel(level)
}

// GetGlobalLevel returns the current global log level.
func GetGlobalLevel() Level {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalLevel
}

// ParseLevel converts a string to a Level. Defaults to LevelInfo for unknown values.
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "fatal":
		return LevelFatal
	default:
		return LevelInfo
	}
}

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

func (l *Logger) log(level Level, format string, args ...interface{}) {
	if level < GetGlobalLevel() {
		return
	}
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("[%s] [%s] [%s] %s\n", timestamp, l.module, level, msg)
	fmt.Fprint(os.Stdout, line)
	writeFile(line)

	if level == LevelFatal {
		os.Exit(1)
	}
}

// Debug logs a message at DEBUG level.
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(LevelDebug, format, args...)
}

// Info logs a message at INFO level.
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(LevelInfo, format, args...)
}

// Warn logs a message at WARN level.
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(LevelWarn, format, args...)
}

// Error logs a message at ERROR level.
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(LevelError, format, args...)
}

// Fatal logs a message at FATAL level and exits the process.
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(LevelFatal, format, args...)
}
