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

package jobs

import "time"

// BackoffKind selects the retry backoff curve a RetryPolicy applies after a
// Nack.
type BackoffKind int

const (
	// BackoffFixed waits the same RetryPolicy.Base delay before every retry.
	BackoffFixed BackoffKind = iota
	// BackoffLinear waits RetryPolicy.Base multiplied by the attempt number,
	// i.e. it grows by a constant increment every retry.
	BackoffLinear
	// BackoffExponential doubles the delay every retry, starting at
	// RetryPolicy.Base.
	BackoffExponential
)

// String implements fmt.Stringer.
func (k BackoffKind) String() string {
	switch k {
	case BackoffFixed:
		return "fixed"
	case BackoffLinear:
		return "linear"
	case BackoffExponential:
		return "exponential"
	default:
		return "unknown"
	}
}

// maxShift bounds the exponent used by BackoffExponential so that attempt
// numbers accumulated by a long-lived job cannot overflow time.Duration; any
// RetryPolicy.MaxDelay cap is applied on top of it regardless.
const maxShift = 32

// RetryPolicy configures how many times a job may be attempted and how long it
// waits between attempts after a Nack. A zero-value RetryPolicy is invalid:
// use DefaultRetryPolicy or one of the constructors below.
type RetryPolicy struct {
	// Kind selects the backoff curve.
	Kind BackoffKind
	// Base is the fixed delay (BackoffFixed), the per-attempt increment
	// (BackoffLinear), or the initial delay that doubles every attempt
	// (BackoffExponential).
	Base time.Duration
	// MaxDelay caps the computed delay. Zero means uncapped.
	MaxDelay time.Duration
	// MaxAttempts is the total number of delivery attempts (including the
	// first) before the job is moved to the dead-letter state. Must be at
	// least 1; there is no "unlimited retries" sentinel.
	MaxAttempts int
}

// FixedBackoff returns a RetryPolicy that waits delay before every retry, up
// to maxAttempts total attempts.
func FixedBackoff(delay time.Duration, maxAttempts int) RetryPolicy {
	return RetryPolicy{Kind: BackoffFixed, Base: delay, MaxAttempts: maxAttempts}
}

// LinearBackoff returns a RetryPolicy whose delay grows by base every retry
// (base, 2*base, 3*base, ...), capped at maxDelay (zero means uncapped), up to
// maxAttempts total attempts.
func LinearBackoff(base, maxDelay time.Duration, maxAttempts int) RetryPolicy {
	return RetryPolicy{Kind: BackoffLinear, Base: base, MaxDelay: maxDelay, MaxAttempts: maxAttempts}
}

// ExponentialBackoff returns a RetryPolicy whose delay doubles every retry
// starting at base, capped at maxDelay (zero means uncapped), up to
// maxAttempts total attempts.
func ExponentialBackoff(base, maxDelay time.Duration, maxAttempts int) RetryPolicy {
	return RetryPolicy{Kind: BackoffExponential, Base: base, MaxDelay: maxDelay, MaxAttempts: maxAttempts}
}

// DefaultRetryPolicy is applied by NewJob when no WithRetryPolicy option is
// given: exponential backoff starting at one second, capped at one minute,
// five attempts total.
func DefaultRetryPolicy() RetryPolicy {
	return ExponentialBackoff(time.Second, time.Minute, 5)
}

// validate reports whether p can be used by a Store: MaxAttempts must allow at
// least one attempt.
func (p RetryPolicy) validate() error {
	if p.MaxAttempts < 1 {
		return ErrInvalidMaxAttempts
	}
	return nil
}

// NextDelay returns the backoff delay to wait before the given attempt number
// (1-based: the attempt that follows the first Nack is attempt 1). Store
// implementations call this from Nack to compute the job's next AvailableAt.
func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	var delay time.Duration
	switch p.Kind {
	case BackoffFixed:
		delay = p.Base
	case BackoffLinear:
		delay = p.Base * time.Duration(attempt)
	case BackoffExponential:
		shift := attempt - 1
		if shift > maxShift {
			shift = maxShift
		}
		delay = p.Base * time.Duration(uint64(1)<<uint(shift))
	default:
		delay = p.Base
	}

	if p.MaxDelay > 0 && (delay > p.MaxDelay || delay < 0) {
		delay = p.MaxDelay
	}
	return delay
}
