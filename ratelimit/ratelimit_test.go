// MIT License
//
// Copyright (c) 2022-2026 GoAkt Team
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/kv"
)

// fakeStore is a minimal, single-process kv.Store used to exercise Limiter without a
// real cluster engine. Locking and TTL semantics mirror the documented contract of
// kv.Store closely enough to validate the read-modify-write path Limiter relies on.
type fakeStore struct {
	mu     sync.Mutex
	values map[string]fakeEntry
	locks  map[string]int64
	token  int64

	// forcedErr, when set, is returned by every method call, letting tests assert
	// that Limiter propagates arbitrary store errors (e.g. cluster degradation)
	// unchanged.
	forcedErr error

	// denyLock, when true, makes TryLock always report ErrLockNotAcquired, to
	// exercise the ErrLimiterBusy retry-budget path.
	denyLock bool

	// dropOnNextGet, when set, makes the next Get for that key delete the entry
	// and report ErrKeyNotFound, simulating a window that expires between the
	// PutIfAbsent race and the post-lock read.
	dropOnNextGet string
}

type fakeEntry struct {
	value     []byte
	expiresAt time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		values: make(map[string]fakeEntry),
		locks:  make(map[string]int64),
	}
}

func (s *fakeStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forcedErr != nil {
		return nil, s.forcedErr
	}
	if s.dropOnNextGet == key {
		s.dropOnNextGet = ""
		delete(s.values, key)
		return nil, kv.ErrKeyNotFound
	}
	entry, ok := s.values[key]
	if !ok || (!entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt)) {
		return nil, kv.ErrKeyNotFound
	}
	return entry.value, nil
}

func (s *fakeStore) Put(_ context.Context, key string, value []byte, opts ...kv.PutOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forcedErr != nil {
		return s.forcedErr
	}
	s.values[key] = s.newEntry(value, opts...)
	return nil
}

func (s *fakeStore) PutIfAbsent(_ context.Context, key string, value []byte, opts ...kv.PutOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forcedErr != nil {
		return s.forcedErr
	}
	if entry, ok := s.values[key]; ok && (entry.expiresAt.IsZero() || time.Now().Before(entry.expiresAt)) {
		return kv.ErrKeyExists
	}
	s.values[key] = s.newEntry(value, opts...)
	return nil
}

func (s *fakeStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forcedErr != nil {
		return s.forcedErr
	}
	delete(s.values, key)
	return nil
}

func (s *fakeStore) TryLock(_ context.Context, key string, ttl time.Duration) (kv.Lock, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forcedErr != nil {
		return nil, s.forcedErr
	}
	if s.denyLock {
		return nil, kv.ErrLockNotAcquired
	}
	if _, held := s.locks[key]; held {
		return nil, kv.ErrLockNotAcquired
	}
	s.token++
	token := s.token
	s.locks[key] = token
	time.AfterFunc(ttl, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.locks[key] == token {
			delete(s.locks, key)
		}
	})
	return &fakeLock{store: s, key: key, token: token}, nil
}

func (s *fakeStore) newEntry(value []byte, opts ...kv.PutOption) fakeEntry {
	ttl := kv.ResolveTTL(opts...)
	entry := fakeEntry{value: value}
	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}
	return entry
}

type fakeLock struct {
	store *fakeStore
	key   string
	token int64
}

func (l *fakeLock) Unlock(context.Context) error {
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.locks[l.key] != l.token {
		return kv.ErrLockNotHeld
	}
	delete(l.store.locks, l.key)
	return nil
}

func TestNew(t *testing.T) {
	t.Run("nil store is rejected", func(t *testing.T) {
		l, err := New(nil, 10, time.Second)
		require.Nil(t, l)
		require.ErrorIs(t, err, ErrNilStore)
	})
	t.Run("non-positive limit is rejected", func(t *testing.T) {
		l, err := New(newFakeStore(), 0, time.Second)
		require.Nil(t, l)
		require.ErrorIs(t, err, ErrInvalidLimit)
	})
	t.Run("non-positive window is rejected", func(t *testing.T) {
		l, err := New(newFakeStore(), 10, 0)
		require.Nil(t, l)
		require.ErrorIs(t, err, ErrInvalidWindow)
	})
	t.Run("valid configuration succeeds", func(t *testing.T) {
		l, err := New(newFakeStore(), 10, time.Second)
		require.NoError(t, err)
		require.NotNil(t, l)
	})
}

func TestAllow(t *testing.T) {
	ctx := context.Background()

	t.Run("admits up to the limit then denies", func(t *testing.T) {
		store := newFakeStore()
		limiter, err := New(store, 3, time.Minute)
		require.NoError(t, err)

		for i := range 3 {
			ok, err := limiter.Allow(ctx, "user-1")
			require.NoError(t, err)
			require.Truef(t, ok, "request %d should be admitted", i)
		}

		ok, err := limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.False(t, ok, "request beyond the limit must be denied")
	})

	t.Run("keys are independent", func(t *testing.T) {
		store := newFakeStore()
		limiter, err := New(store, 1, time.Minute)
		require.NoError(t, err)

		ok, err := limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = limiter.Allow(ctx, "user-2")
		require.NoError(t, err)
		require.True(t, ok, "a different key must have its own budget")
	})

	t.Run("a new window resets the budget", func(t *testing.T) {
		store := newFakeStore()
		limiter, err := New(store, 1, time.Minute)
		require.NoError(t, err)

		start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		limiter.now = func() time.Time { return start }

		ok, err := limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.False(t, ok, "still inside the same window")

		limiter.now = func() time.Time { return start.Add(time.Minute) }
		ok, err = limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.True(t, ok, "a fresh window must reset the budget")
	})
}

func TestAllowN(t *testing.T) {
	ctx := context.Background()

	t.Run("non-positive n is rejected", func(t *testing.T) {
		store := newFakeStore()
		limiter, err := New(store, 10, time.Minute)
		require.NoError(t, err)

		ok, err := limiter.AllowN(ctx, "user-1", 0)
		require.False(t, ok)
		require.ErrorIs(t, err, ErrInvalidN)
	})

	t.Run("a burst exceeding the remaining budget is denied without partial admission", func(t *testing.T) {
		store := newFakeStore()
		limiter, err := New(store, 5, time.Minute)
		require.NoError(t, err)

		ok, err := limiter.AllowN(ctx, "user-1", 3)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = limiter.AllowN(ctx, "user-1", 3)
		require.NoError(t, err)
		require.False(t, ok, "3 more would exceed the limit of 5 with 3 already spent")

		// the failed AllowN(3) call above must not have consumed any budget
		ok, err = limiter.AllowN(ctx, "user-1", 2)
		require.NoError(t, err)
		require.True(t, ok, "the remaining 2 units of budget must still be available")
	})

	t.Run("an over-limit burst as the window's first call must not poison the window", func(t *testing.T) {
		store := newFakeStore()
		limiter, err := New(store, 3, time.Minute)
		require.NoError(t, err)

		// fast path: first touch of the window is already over the limit
		ok, err := limiter.AllowN(ctx, "user-1", 10)
		require.NoError(t, err)
		require.False(t, ok)

		// the rejected burst must not have seeded the counter with 10:
		// the full budget must remain available within the same window
		for i := 0; i < 3; i++ {
			ok, err = limiter.Allow(ctx, "user-1")
			require.NoError(t, err)
			require.True(t, ok, "request %d must still fit the untouched budget", i+1)
		}

		ok, err = limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.False(t, ok, "budget of 3 is now genuinely spent")
	})

	t.Run("an over-limit burst rebuilding an expired window must not poison it", func(t *testing.T) {
		store := newFakeStore()
		limiter, err := New(store, 3, time.Minute)
		require.NoError(t, err)

		// force the slow path: the counter key exists at PutIfAbsent time but is
		// gone by the post-lock Get, i.e. the window expired in between
		windowKey := limiter.windowKey("user-1")
		require.NoError(t, store.Put(ctx, windowKey, encodeCount(1)))
		store.dropOnNextGet = windowKey

		ok, err := limiter.AllowN(ctx, "user-1", 10)
		require.NoError(t, err)
		require.False(t, ok)

		for i := 0; i < 3; i++ {
			ok, err = limiter.Allow(ctx, "user-1")
			require.NoError(t, err)
			require.True(t, ok, "request %d must still fit the untouched budget", i+1)
		}
	})
}

func TestAllowPropagatesStoreErrors(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	store.forcedErr = errors.New("cluster is not enabled")

	limiter, err := New(store, 10, time.Minute)
	require.NoError(t, err)

	ok, err := limiter.Allow(ctx, "user-1")
	require.False(t, ok)
	require.ErrorIs(t, err, store.forcedErr)
}

func TestAllowReturnsErrLimiterBusyOnPersistentLockContention(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	limiter, err := New(store, 10, time.Minute, WithMaxRetries(2), WithRetryBackoff(time.Millisecond))
	require.NoError(t, err)

	// prime the window's counter so Allow takes the lock-guarded slow path.
	ok, err := limiter.Allow(ctx, "user-1")
	require.NoError(t, err)
	require.True(t, ok)

	store.mu.Lock()
	store.denyLock = true
	store.mu.Unlock()

	ok, err = limiter.Allow(ctx, "user-1")
	require.False(t, ok)
	require.ErrorIs(t, err, ErrLimiterBusy)
}

// TestAllowWindowRolloverAdmitsUpToDoubleLimit documents the fixed-window
// boundary effect called out in the package doc comment: a burst straddling
// two windows can admit up to 2x Limit requests (limit spent at the tail of
// the first window, plus a fresh limit at the head of the second), but never
// more than that.
func TestAllowWindowRolloverAdmitsUpToDoubleLimit(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	const limit = 3
	limiter, err := New(store, limit, time.Minute)
	require.NoError(t, err)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	limiter.now = func() time.Time { return start }

	// spend the entire budget at the tail of the first window
	for i := 0; i < limit; i++ {
		ok, err := limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.Truef(t, ok, "request %d in window 1 should be admitted", i)
	}
	ok, err := limiter.Allow(ctx, "user-1")
	require.NoError(t, err)
	require.False(t, ok, "window 1's budget is exhausted")

	// roll into the next window and spend its full budget too
	limiter.now = func() time.Time { return start.Add(time.Minute) }
	for i := 0; i < limit; i++ {
		ok, err := limiter.Allow(ctx, "user-1")
		require.NoError(t, err)
		require.Truef(t, ok, "request %d in window 2 should be admitted", i)
	}

	// the ceiling: exactly 2x limit total, never more
	ok, err = limiter.Allow(ctx, "user-1")
	require.NoError(t, err)
	require.False(t, ok, "2x limit is the documented ceiling for a boundary-straddling burst")
}

// TestAllowNExactlyAtLimit verifies that n == limit is admitted in full (the
// boundary is inclusive), and that the window's budget is then fully spent.
func TestAllowNExactlyAtLimit(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	const limit = 5
	limiter, err := New(store, limit, time.Minute)
	require.NoError(t, err)

	ok, err := limiter.AllowN(ctx, "user-1", limit)
	require.NoError(t, err)
	require.True(t, ok, "n == limit must be admitted")

	ok, err = limiter.Allow(ctx, "user-1")
	require.NoError(t, err)
	require.False(t, ok, "the budget is now fully spent")

	ok, err = limiter.AllowN(ctx, "user-1", 1)
	require.NoError(t, err)
	require.False(t, ok, "no budget remains for any further request")
}

// TestAllowConcurrentAcrossWindowExpiry races concurrent Allow calls against a
// clock that flips from one window to the next mid-burst, under -race. The
// only invariant asserted is the documented ceiling: admission across the
// boundary must never exceed 2x the limit, no matter how the calls interleave
// with the window rollover.
func TestAllowConcurrentAcrossWindowExpiry(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	const limit = 20
	limiter, err := New(store, limit, time.Minute)
	require.NoError(t, err)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var now atomic.Value
	now.Store(start)
	limiter.now = func() time.Time { return now.Load().(time.Time) }

	const attempts = 400
	var admitted atomic.Int64
	var wg sync.WaitGroup
	wg.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer wg.Done()
			// flip the clock to the next window partway through the burst so
			// some goroutines race the rollover itself.
			if i == attempts/2 {
				now.Store(start.Add(time.Minute))
			}
			ok, err := limiter.Allow(ctx, "shared-key")
			require.NoError(t, err)
			if ok {
				admitted.Add(1)
			}
		}(i)
	}
	wg.Wait()

	require.LessOrEqualf(t, admitted.Load(), int64(2*limit), "a boundary-straddling burst must never admit more than 2x the limit")
	require.Positive(t, admitted.Load(), "at least some requests should be admitted")
}

func TestAllowConcurrentStaysWithinLimit(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	const limit = 50
	limiter, err := New(store, limit, time.Minute)
	require.NoError(t, err)

	const attempts = 200
	var admitted atomic.Int64
	var wg sync.WaitGroup
	wg.Add(attempts)
	for range attempts {
		go func() {
			defer wg.Done()
			ok, err := limiter.Allow(ctx, "shared-key")
			require.NoError(t, err)
			if ok {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()

	require.EqualValues(t, limit, admitted.Load(), "exactly limit requests must be admitted under concurrent load")
}
