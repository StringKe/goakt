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

package kv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResolveTTL(t *testing.T) {
	t.Run("No options resolves to zero", func(t *testing.T) {
		assert.Equal(t, time.Duration(0), ResolveTTL())
	})
	t.Run("WithTTL sets the duration", func(t *testing.T) {
		assert.Equal(t, 5*time.Second, ResolveTTL(WithTTL(5*time.Second)))
	})
	t.Run("Last WithTTL wins", func(t *testing.T) {
		assert.Equal(t, 10*time.Second, ResolveTTL(WithTTL(5*time.Second), WithTTL(10*time.Second)))
	})
}
