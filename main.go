package flock

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/opensvc/locker"
)

type (
	// T wraps flock and dumps JSON data in the lock file
	// hinting about what holds the lock.
	T struct {
		locker.Locker
		Path      string
		sessionId string
	}

	Meta struct {
		At        time.Time `json:"at"`
		PID       int       `json:"pid"`
		Intent    string    `json:"intent"`
		SessionID string    `json:"session_id"`
	}
)

var (
	truncate      = os.Truncate
	remove        = os.Remove
	retryInterval = 500 * time.Millisecond
)

// New allocate a file lock struct from the Locker provider.
func New(p string, sessionId string, lockP func(string) locker.Locker) *T {
	return &T{
		Locker:    lockP(p),
		Path:      p,
		sessionId: sessionId,
	}
}

// Lock acquires an exclusive file lock on the file and writes a JSON
// formatted structure hinting who holds the lock and with what
// intention.
func (t *T) Lock(timeout time.Duration, intent string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err = t.LockContext(ctx, retryInterval)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.New("lock timeout exceeded")
		}
		return
	}
	err = t.writeMeta(intent)
	return
}

func (t *T) writeMeta(intent string) error {
	m := Meta{
		At:        time.Now(),
		PID:       os.Getpid(),
		Intent:    intent,
		SessionID: t.sessionId,
	}
	enc := json.NewEncoder(t)
	return enc.Encode(m)
}

// Probe attempts to acquire a file lock. If successful, it releases the lock and returns an empty Meta.
// If the lock is already held by another process, it reads and returns the current lock metadata.
// Note: Reading the metadata may fail if the other process has not finished writing it.
func (t *T) Probe() (Meta, error) {
	err := t.TryLock()
	if err == nil {
		// lock acquired
		_ = t.UnLock()
		return Meta{}, nil
	}
	// lock conflict, read current lock meta
	return t.readMeta()
}

func (t *T) readMeta() (Meta, error) {
	var m Meta
	file, err := os.Open(t.Path)
	if err != nil {
		return m, err
	}
	defer func() { _ = file.Close() }()
	dec := json.NewDecoder(file)
	err = dec.Decode(&m)
	return m, err
}

// UnLock releases the file lock acquired by Lock.
func (t *T) UnLock() error {
	_ = truncate(t.Path, 0)
	_ = remove(t.Path)
	return t.Locker.UnLock()
}
