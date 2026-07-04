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

// Package ratelimit implements a cluster-wide rate limiter on top of the exposed
// cluster kv.Store (see the kv package), so that the effective limit for a key holds
// across every node in the cluster instead of being multiplied by the replica count.
//
// The algorithm is a fixed-window counter: for a given key, all requests that land in
// the same Window-sized slice of wall-clock time share one counter, capped at Limit.
// The counter is created with Store.PutIfAbsent (which doubles as the TTL-backed
// cleanup mechanism -- the counter entry expires on its own once the window elapses)
// and incremented under a short-lived Store.TryLock so concurrent callers on the same
// node or different nodes serialize on the same counter instead of racing a
// read-modify-write. This keeps the primitive expressible purely in terms of the
// put-if-absent + TTL + lock surface the cluster kv.Store already exposes, at the cost
// of the classic fixed-window boundary effect: a burst straddling a window boundary can
// admit up to 2x Limit requests in the worst case. Callers that need a smoother profile
// should budget for that tolerance or layer a smaller Window on top.
package ratelimit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/tochemey/goakt/v4/kv"
)

const (
	// defaultLockTTL bounds how long the counter lock for a key may be held. It only
	// needs to outlive a single Get+Put round-trip against the kv store.
	defaultLockTTL = 2 * time.Second
	// defaultMaxRetries bounds how many times AllowN retries acquiring the counter
	// lock when it loses the race to a concurrent caller for the same key.
	defaultMaxRetries = 20
	// defaultRetryBackoff is the pause between counter lock acquisition attempts.
	defaultRetryBackoff = 5 * time.Millisecond

	lockKeySuffix = ":lock"
)

// Limiter is a cluster-wide, fixed-window rate limiter backed by a kv.Store.
type Limiter struct {
	store        kv.Store
	limit        int
	window       time.Duration
	lockTTL      time.Duration
	maxRetries   int
	retryBackoff time.Duration
	now          func() time.Time
}

// Option configures optional behavior of a Limiter created with New.
type Option func(*Limiter)

// WithLockTTL overrides how long the internal counter lock for a key may be held
// before it is automatically released. The default is 2 seconds.
func WithLockTTL(ttl time.Duration) Option {
	return func(l *Limiter) {
		l.lockTTL = ttl
	}
}

// WithMaxRetries overrides how many times AllowN retries acquiring the counter lock
// for a key before giving up with ErrLimiterBusy. The default is 20.
func WithMaxRetries(n int) Option {
	return func(l *Limiter) {
		l.maxRetries = n
	}
}

// WithRetryBackoff overrides the pause between counter lock acquisition attempts.
// The default is 5 milliseconds.
func WithRetryBackoff(d time.Duration) Option {
	return func(l *Limiter) {
		l.retryBackoff = d
	}
}

// New creates a Limiter that admits at most limit requests per window for any given
// key, enforced cluster-wide through store. store is typically obtained by calling
// actor.ActorSystem.KV(); its methods return gerrors.ErrActorSystemNotStarted or
// gerrors.ErrClusterDisabled when cluster mode is unavailable, and Allow/AllowN
// propagate those errors unchanged so callers get the same typed degradation signal
// as the rest of the cluster kv surface.
func New(store kv.Store, limit int, window time.Duration, opts ...Option) (*Limiter, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	if window <= 0 {
		return nil, ErrInvalidWindow
	}

	l := &Limiter{
		store:        store,
		limit:        limit,
		window:       window,
		lockTTL:      defaultLockTTL,
		maxRetries:   defaultMaxRetries,
		retryBackoff: defaultRetryBackoff,
		now:          time.Now,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

// Allow reports whether a single request for key is admitted under the configured
// limit for the current window. It is equivalent to AllowN(ctx, key, 1).
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n requests for key are admitted under the configured limit
// for the current window. n counts against the same per-key, per-window budget as
// every other call for that key, cluster-wide.
func (l *Limiter) AllowN(ctx context.Context, key string, n int) (bool, error) {
	if n <= 0 {
		return false, ErrInvalidN
	}

	windowKey := l.windowKey(key)

	// A rejected request must not consume any budget: seeding the fresh window
	// with n on rejection would poison the whole window and starve later,
	// smaller requests until it expires.
	admit := n <= l.limit
	seed := 0
	if admit {
		seed = n
	}

	// Fast path: this call is the first to touch the window, so PutIfAbsent both
	// creates the counter and settles the admission decision in one round-trip.
	if err := l.store.PutIfAbsent(ctx, windowKey, encodeCount(seed), kv.WithTTL(l.window)); err == nil {
		return admit, nil
	} else if !errors.Is(err, kv.ErrKeyExists) {
		return false, err
	}

	// Slow path: the window's counter already exists. Serialize the read-modify-write
	// against concurrent callers (same node or another node) with a short-lived lock.
	lock, err := l.acquireCounterLock(ctx, windowKey)
	if err != nil {
		return false, err
	}
	defer func() { _ = lock.Unlock(ctx) }()

	raw, err := l.store.Get(ctx, windowKey)
	if err != nil {
		if !errors.Is(err, kv.ErrKeyNotFound) {
			return false, err
		}
		// The counter expired between the PutIfAbsent race above and acquiring the
		// lock: this call now starts a fresh window.
		if err := l.store.Put(ctx, windowKey, encodeCount(seed), kv.WithTTL(l.window)); err != nil {
			return false, err
		}
		return admit, nil
	}

	current, err := decodeCount(raw)
	if err != nil {
		return false, err
	}

	if current+n > l.limit {
		return false, nil
	}

	if err := l.store.Put(ctx, windowKey, encodeCount(current+n), kv.WithTTL(l.window)); err != nil {
		return false, err
	}
	return true, nil
}

// acquireCounterLock retries TryLock for the given window key's lock until it
// succeeds, the retry budget is exhausted (ErrLimiterBusy), the context is done, or a
// non-contention error occurs.
func (l *Limiter) acquireCounterLock(ctx context.Context, windowKey string) (kv.Lock, error) {
	lockKey := windowKey + lockKeySuffix
	for attempt := 0; ; attempt++ {
		lock, err := l.store.TryLock(ctx, lockKey, l.lockTTL)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, kv.ErrLockNotAcquired) {
			return nil, err
		}
		if attempt >= l.maxRetries {
			return nil, ErrLimiterBusy
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(l.retryBackoff):
		}
	}
}

// windowKey composes the storage key for the fixed window that the current instant
// belongs to, so that every caller within the same window (cluster-wide) shares one
// counter, and the transition to the next window happens implicitly by naming a
// different key.
func (l *Limiter) windowKey(key string) string {
	windowIndex := l.now().UnixNano() / int64(l.window)
	return "ratelimit:" + key + ":" + strconv.FormatInt(windowIndex, 10)
}

// encodeCount renders a counter value as the []byte the kv.Store expects.
func encodeCount(n int) []byte {
	return []byte(strconv.Itoa(n))
}

// decodeCount parses a counter value previously written by encodeCount.
func decodeCount(raw []byte) (int, error) {
	return strconv.Atoi(string(raw))
}
