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

package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Server is a thin convenience wrapper around http.Server that wires a Manager's
// cluster-shared, SNI-dynamic TLS certificates (and, optionally, Authenticated Origin
// Pulls mTLS verification) into the standard library's TLS listener. Plain HTTP
// handlers, WebSocket upgrades (NewWSHandler), and SSE streams (NewSSEHandler) are all
// just http.Handler values mounted on Server like on any other *http.Server - Server
// adds nothing actor- or cluster-related to the request path itself.
//
// Using Server is entirely optional: any application already managing its own
// *http.Server can instead set TLSConfig: manager.TLSConfig() directly.
type Server struct {
	httpServer *http.Server
	manager    *Manager
	originPull *AuthenticatedOriginPulls
}

// ServerOption configures a Server created with NewServer.
type ServerOption func(*Server)

// WithTLSManager configures Server to terminate TLS using manager's cluster-shared,
// SNI-dynamic certificates. Without this option, Server serves plain HTTP.
func WithTLSManager(manager *Manager) ServerOption {
	return func(s *Server) { s.manager = manager }
}

// WithAuthenticatedOriginPulls enables mTLS verification of inbound connections against
// pulls's configured CA (see AuthenticatedOriginPulls), rejecting any connection that
// does not present a valid client certificate. Requires WithTLSManager.
func WithAuthenticatedOriginPulls(pulls *AuthenticatedOriginPulls) ServerOption {
	return func(s *Server) { s.originPull = pulls }
}

// WithReadHeaderTimeout sets the underlying http.Server's ReadHeaderTimeout.
func WithReadHeaderTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.httpServer.ReadHeaderTimeout = d }
}

// NewServer creates a Server listening on addr and dispatching to handler.
func NewServer(addr string, handler http.Handler, opts ...ServerOption) (*Server, error) {
	s := &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.originPull != nil && s.manager == nil {
		return nil, fmt.Errorf("gateway: WithAuthenticatedOriginPulls requires WithTLSManager")
	}

	if s.manager != nil {
		cfg := s.manager.TLSConfig()
		if s.originPull != nil {
			if err := s.originPull.Apply(cfg); err != nil {
				return nil, err
			}
		}
		s.httpServer.TLSConfig = cfg
	}

	return s, nil
}

// ListenAndServe starts the Manager's renewal schedule (if any) and serves traffic,
// terminating TLS through it when WithTLSManager was set, or serving plain HTTP
// otherwise. It blocks until the server stops (see Shutdown) and returns
// http.ErrServerClosed on a clean shutdown.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.manager != nil {
		if err := s.manager.Start(ctx); err != nil {
			return err
		}
		// TLSConfig.GetCertificate handles certificate selection dynamically by SNI, so
		// no cert/key file path is needed here.
		return s.httpServer.ListenAndServeTLS("", "")
	}
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the server and, if configured, the Manager's renewal
// schedule.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.manager != nil {
		_ = s.manager.Stop(ctx)
	}
	return s.httpServer.Shutdown(ctx)
}
