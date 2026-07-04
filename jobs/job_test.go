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
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

func TestNewJob(t *testing.T) {
	t.Run("happy path fills in defaults", func(t *testing.T) {
		job, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{A: 1, B: 2})
		require.NoError(t, err)
		require.NotEmpty(t, job.ID)
		require.Equal(t, "rewards", job.Queue)
		require.Equal(t, "aggregator", job.TargetName)
		require.NotEmpty(t, job.Payload)
		require.Equal(t, jobs.StatePending, job.State)
		require.Equal(t, jobs.DefaultRetryPolicy(), job.RetryPolicy)
		require.False(t, job.CreatedAt.IsZero())
		require.Equal(t, job.CreatedAt, job.AvailableAt)
	})

	t.Run("rejects an empty queue", func(t *testing.T) {
		_, err := jobs.NewJob("", "aggregator", &testpb.TestSum{})
		require.ErrorIs(t, err, jobs.ErrInvalidQueue)
	})

	t.Run("rejects an empty target name", func(t *testing.T) {
		_, err := jobs.NewJob("rewards", "", &testpb.TestSum{})
		require.ErrorIs(t, err, jobs.ErrInvalidTargetName)
	})

	t.Run("rejects a nil message", func(t *testing.T) {
		_, err := jobs.NewJob("rewards", "aggregator", nil)
		require.ErrorIs(t, err, jobs.ErrPayloadNotProto)
	})

	t.Run("WithDelay pushes AvailableAt into the future", func(t *testing.T) {
		job, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{}, jobs.WithDelay(time.Minute))
		require.NoError(t, err)
		require.True(t, job.AvailableAt.After(job.CreatedAt))
	})

	t.Run("WithID overrides the generated id", func(t *testing.T) {
		job, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{}, jobs.WithID("fixed-id"))
		require.NoError(t, err)
		require.Equal(t, "fixed-id", job.ID)
	})

	t.Run("WithRetryPolicy overrides the default", func(t *testing.T) {
		policy := jobs.FixedBackoff(time.Second, 1)
		job, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{}, jobs.WithRetryPolicy(policy))
		require.NoError(t, err)
		require.Equal(t, policy, job.RetryPolicy)
	})

	t.Run("rejects a retry policy with zero max attempts", func(t *testing.T) {
		policy := jobs.FixedBackoff(time.Second, 0)
		_, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{}, jobs.WithRetryPolicy(policy))
		require.ErrorIs(t, err, jobs.ErrInvalidMaxAttempts)
	})
}

func TestJob_Clone(t *testing.T) {
	job, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{A: 1, B: 2})
	require.NoError(t, err)
	job.ChildResults = []jobs.ChildResult{{ChildIndex: 0, Payload: []byte("x")}}

	clone := job.Clone()
	clone.Payload[0] = 0xFF
	clone.ChildResults[0].Payload[0] = 0xFF

	require.NotEqual(t, job.Payload[0], clone.Payload[0])
	require.NotEqual(t, job.ChildResults[0].Payload[0], clone.ChildResults[0].Payload[0])
}
