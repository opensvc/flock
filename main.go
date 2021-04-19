package flock

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/opensvc/locker"
	"io"
	"os"
	"time"
)

type (
	// T wraps flock and dumps a json data in the lock file
	// hinting about what holds the lock.
	T struct {
		locker.Locker
		Path      string
		sessionId string
	}

	meta struct {
		PID       int    `json:"pid"`
		Intent    string `json:"intent"`
		SessionID string `json:"session_id"`
	}
)

var (
	truncate = os.Truncate
	remove   = os.Remove
	//defaultLockProvider = fcntllock.New
	retryInterval = 500 * time.Millisecond
)

// New allocate a file lock struct from Locker provider.
func New(p string, sessionId string, lockP func(string) locker.Locker) *T {
	return &T{
		Locker:    lockP(p),
		Path:      p,
		sessionId: sessionId,
	}
}

//
// Lock acquires an exclusive file lock on the file and write a json
// formatted structure hinting who holds the lock and with what
// intention.
//
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
	err = t.writeMeta(t, intent)
	return
}

func (t T) writeMeta(w io.Writer, intent string) error {
	m := meta{
		PID:       os.Getpid(),
		Intent:    intent,
		SessionID: t.sessionId,
	}
	enc := json.NewEncoder(w)
	return enc.Encode(m)
}

// UnLock releases the file lock acquired by Lock.
func (t *T) UnLock() error {
	_ = truncate(t.Path, 0)
	_ = remove(t.Path)
	return t.Locker.UnLock()
}
