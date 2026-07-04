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

package jobs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/jobs"
)

func TestRetryPolicy_NextDelay(t *testing.T) {
	t.Run("fixed backoff never grows", func(t *testing.T) {
		policy := jobs.FixedBackoff(time.Second, 5)
		require.Equal(t, time.Second, policy.NextDelay(1))
		require.Equal(t, time.Second, policy.NextDelay(2))
		require.Equal(t, time.Second, policy.NextDelay(10))
	})

	t.Run("linear backoff grows by a constant increment", func(t *testing.T) {
		policy := jobs.LinearBackoff(time.Second, 0, 5)
		require.Equal(t, time.Second, policy.NextDelay(1))
		require.Equal(t, 2*time.Second, policy.NextDelay(2))
		require.Equal(t, 3*time.Second, policy.NextDelay(3))
	})

	t.Run("linear backoff respects the max delay cap", func(t *testing.T) {
		policy := jobs.LinearBackoff(time.Second, 2*time.Second, 5)
		require.Equal(t, 2*time.Second, policy.NextDelay(3))
	})

	t.Run("exponential backoff doubles every attempt", func(t *testing.T) {
		policy := jobs.ExponentialBackoff(time.Second, 0, 10)
		require.Equal(t, time.Second, policy.NextDelay(1))
		require.Equal(t, 2*time.Second, policy.NextDelay(2))
		require.Equal(t, 4*time.Second, policy.NextDelay(3))
		require.Equal(t, 8*time.Second, policy.NextDelay(4))
	})

	t.Run("exponential backoff respects the max delay cap", func(t *testing.T) {
		policy := jobs.ExponentialBackoff(time.Second, 5*time.Second, 10)
		require.Equal(t, 5*time.Second, policy.NextDelay(5))
	})

	t.Run("exponential backoff never overflows for a very large attempt count", func(t *testing.T) {
		policy := jobs.ExponentialBackoff(time.Second, time.Minute, 1000)
		delay := policy.NextDelay(999)
		require.Equal(t, time.Minute, delay)
		require.Positive(t, delay)
	})

	t.Run("attempt numbers below one are treated as one", func(t *testing.T) {
		policy := jobs.LinearBackoff(time.Second, 0, 5)
		require.Equal(t, policy.NextDelay(1), policy.NextDelay(0))
		require.Equal(t, policy.NextDelay(1), policy.NextDelay(-3))
	})
}

func TestDefaultRetryPolicy(t *testing.T) {
	policy := jobs.DefaultRetryPolicy()
	require.Equal(t, jobs.BackoffExponential, policy.Kind)
	require.Equal(t, 5, policy.MaxAttempts)
}

func TestBackoffKind_String(t *testing.T) {
	require.Equal(t, "fixed", jobs.BackoffFixed.String())
	require.Equal(t, "linear", jobs.BackoffLinear.String())
	require.Equal(t, "exponential", jobs.BackoffExponential.String())
	require.Equal(t, "unknown", jobs.BackoffKind(99).String())
}
