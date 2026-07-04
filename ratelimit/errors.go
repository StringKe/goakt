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

import "errors"

var (
	// ErrNilStore is returned by New when the supplied kv.Store is nil.
	ErrNilStore = errors.New("ratelimit: kv store is nil")

	// ErrInvalidLimit is returned by New when the configured limit is not positive.
	ErrInvalidLimit = errors.New("ratelimit: limit must be greater than zero")

	// ErrInvalidWindow is returned by New when the configured window is not positive.
	ErrInvalidWindow = errors.New("ratelimit: window must be greater than zero")

	// ErrInvalidN is returned by AllowN when n is not positive.
	ErrInvalidN = errors.New("ratelimit: n must be greater than zero")

	// ErrLimiterBusy is returned when the distributed lock guarding a key's counter
	// could not be acquired within the configured retry budget. It signals contention,
	// not a rate limit decision: callers should treat it distinctly from a denied request.
	ErrLimiterBusy = errors.New("ratelimit: could not acquire the counter lock in time")
)
