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

package gateway_test

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/gateway"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/log"
)

func TestSSEHandlerDelivery(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/?id=sse-1", nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	pause.For(200 * time.Millisecond)
	require.True(t, registry.Has("sse-1"))

	require.NoError(t, registry.SendToConnection(context.Background(), "sse-1", []byte("hello")))

	reader := bufio.NewReader(resp.Body)
	line, err := readSSEDataLine(reader)
	require.NoError(t, err)
	require.Equal(t, "hello", line)
}

func TestSSEHandlerAuthRejected(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEAuthFunc(func(*http.Request) error {
			return errors.New("no token")
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// readSSEDataLine reads lines until it finds one prefixed with "data: ", and returns its
// content.
func readSSEDataLine(reader *bufio.Reader) (string, error) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data: ") {
			return strings.TrimPrefix(line, "data: "), nil
		}
	}
}
