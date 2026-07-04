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
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/gateway"
	"github.com/tochemey/goakt/v4/log"
)

// fakeIssuer is a CertIssuer that counts how many times Issue was actually called (as
// opposed to served from a cache/singleflight dedup), optionally sleeping to widen the
// window for concurrent callers to race.
type fakeIssuer struct {
	calls atomic.Int64
	delay time.Duration
	ttl   time.Duration
}

func (f *fakeIssuer) Issue(_ context.Context, domain string) (*gateway.Certificate, error) {
	f.calls.Add(1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	cert, key := generateTestCertificate(domain, time.Now().Add(f.ttl))
	return &gateway.Certificate{
		Domain:   domain,
		CertPEM:  cert,
		KeyPEM:   key,
		NotAfter: time.Now().Add(f.ttl),
	}, nil
}

func TestManagerGetCertificateSNILookup(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithRenewInterval(""),
		gateway.WithRenewBefore(time.Minute),
	)

	cert, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "a.example.com"})
	require.NoError(t, err)
	require.NotNil(t, cert)

	other, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "b.example.com"})
	require.NoError(t, err)
	require.NotNil(t, other)

	require.Equal(t, int64(2), issuer.calls.Load(), "one issuance call per distinct SNI domain")

	// asking for a.example.com again must be served from the hot cache, not re-issued.
	again, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "a.example.com"})
	require.NoError(t, err)
	require.NotNil(t, again)
	require.Equal(t, int64(2), issuer.calls.Load())
}

func TestManagerGetCertificateEmptySNI(t *testing.T) {
	system := newTestSystem(t)
	manager := gateway.NewManager(system, log.DiscardLogger, gateway.WithRenewInterval(""))

	_, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: ""})
	require.Error(t, err)
}

func TestManagerAllowedDomains(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("allowed.example.com"),
		gateway.WithRenewInterval(""),
	)

	_, err := manager.EnsureCertificate(context.Background(), "not-allowed.example.com")
	require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)

	_, err = manager.EnsureCertificate(context.Background(), "allowed.example.com")
	require.NoError(t, err)
}

func TestManagerNoIssuerConfigured(t *testing.T) {
	system := newTestSystem(t)
	manager := gateway.NewManager(system, log.DiscardLogger, gateway.WithRenewInterval(""))

	_, err := manager.EnsureCertificate(context.Background(), "example.com")
	require.ErrorIs(t, err, gateway.ErrNoIssuer)
}

// TestManagerIssuanceSingleFlight verifies that N concurrent EnsureCertificate calls for
// the same, previously-unissued domain result in exactly one call to the underlying
// CertIssuer - the local single-flight dedup layer that sits underneath (and, in cluster
// mode, in front of) the cluster-wide TryLock arbitration.
func TestManagerIssuanceSingleFlight(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour, delay: 100 * time.Millisecond}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithRenewInterval(""),
	)

	const concurrency = 20
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = manager.EnsureCertificate(context.Background(), "concurrent.example.com")
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), issuer.calls.Load(), "exactly one issuance call for a concurrent cold start on one node")
}
