package flock

import (
	"encoding/json"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/opensvc/locker"
	mockLocker "github.com/opensvc/locker/mock_locker"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func setup(t *testing.T) (prov func(string) locker.Locker, lck *mockLocker.MockLocker) {
	ctrl := gomock.NewController(t)
	lck = mockLocker.NewMockLocker(ctrl)
	prov = func(string) locker.Locker {
		return lck
	}
	return
}

func TestLock(t *testing.T) {
	t.Run("Ensure write data to lock file when lock succeed", func(t *testing.T) {
		prov, mockLck := setup(t)
		lck := New("foo.lck", "session1", prov)
		var b []byte
		mockLck.EXPECT().LockContext(gomock.Any(), 500*time.Millisecond).Return(nil)
		mockLck.EXPECT().
			Write(gomock.AssignableToTypeOf(b)).
			Do(func(arg []byte) {
				b = arg
			}).
			Return(0, nil)

		err := lck.Lock(100*time.Millisecond, "intent1")
		assert.Equal(t, nil, err)

		found := meta{}
		if err := json.Unmarshal(b, &found); err != nil {
			t.Fatalf("expected written data : %+v\n", b)
		}
		assert.Equal(t, "intent1", found.Intent)
		assert.Equal(t, "session1", found.SessionID)
	})

	t.Run("Ensure return error if lock fail", func(t *testing.T) {
		prov, mockLck := setup(t)

		mockLck.EXPECT().LockContext(gomock.Any(), gomock.Any()).Return(errors.New("can't lock"))

		err := New("foo.lck", "sessionId2", prov).Lock(100*time.Millisecond, "intent1")
		assert.Equal(t, errors.New("can't lock"), err)
	})
}

func TestUnLock(t *testing.T) {
	t.Run("Ensure unlock succeed", func(t *testing.T) {
		prov, mockLck := setup(t)

		mockLck.EXPECT().UnLock().Return(nil)
		l := New("foo.lck", "sessionX", prov)

		err := l.UnLock()
		assert.Equal(t, nil, err)
	})
}
