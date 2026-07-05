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

// This file is a white-box (package gateway) test file so it can reach the unexported
// certLockKeyPrefix/certKVKeyPrefix cluster KV key formats Manager uses internally,
// needed to simulate a cluster issuance lock holder that never unlocks and a corrupted
// cluster KV record. See cert_manager_test.go (package gateway_test) for the black-box
// Manager tests.
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/testkit"
)

// TestDefaultRenewIntervalIsValidCron pins the default renewal schedule to a
// go-quartz-parseable expression: the original default used the descriptor form
// "@every 1h", which go-quartz rejects, so Manager.Start failed on any system that did
// not override WithRenewInterval.
func TestDefaultRenewIntervalIsValidCron(t *testing.T) {
	_, err := quartz.NewCronTrigger(defaultRenewInterval)
	require.NoError(t, err)
}

// internalClusterKindActor is a no-op Actor registered as a cluster kind purely to
// satisfy ClusterConfig's requirement that at least one actor kind be registered.
type internalClusterKindActor struct{}

var _ actor.Actor = (*internalClusterKindActor)(nil)

func (internalClusterKindActor) PreStart(*actor.Context) error { return nil }
func (internalClusterKindActor) Receive(*actor.ReceiveContext) {}
func (internalClusterKindActor) PostStop(*actor.Context) error { return nil }

// generateInternalTestCertificate creates a minimal, self-signed leaf certificate for
// domain, PEM-encoded along with its private key. Mirrors gateway_test's
// generateTestCertificate, duplicated here because this white-box file cannot import the
// external test package.
func generateInternalTestCertificate(domain string, notAfter time.Time) (certPEM, keyPEM []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

// internalFakeIssuer is a minimal CertIssuer for the white-box cluster tests in this file.
type internalFakeIssuer struct {
	calls int
	ttl   time.Duration
}

func (f *internalFakeIssuer) Issue(_ context.Context, domain string) (*Certificate, error) {
	f.calls++
	certPEM, keyPEM := generateInternalTestCertificate(domain, time.Now().Add(f.ttl))
	return &Certificate{
		Domain:   domain,
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: time.Now().Add(f.ttl),
	}, nil
}

// TestManagerRenewalHolderDies_TTLRecovery simulates a cluster issuance lock holder that
// acquires the lock and then never calls Unlock (e.g. the node crashed mid-issuance): a
// second node's EnsureCertificate call must not hang forever. It observes Manager's
// documented behavior: waitForClusterCert gives up and returns ErrIssuanceTimeout once its
// own deadline (now + lockTTL) elapses, since it only polls the KV store for a published
// certificate and never itself retries acquiring the lock.
func TestManagerRenewalHolderDies_TTLRecovery(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&internalClusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	node := multi.StartNode(ctx, "renewal-holder-dies")

	store, err := node.ActorSystem().KV()
	require.NoError(t, err)

	const domain = "holder-dies.example.com"
	const lockTTL = 400 * time.Millisecond

	// Simulate a node that won the issuance race and then crashed before publishing a
	// certificate or unlocking: acquire the lock directly and never release it.
	_, err = store.TryLock(ctx, certLockKeyPrefix+domain, lockTTL)
	require.NoError(t, err)

	manager := NewManager(node.ActorSystem(), log.DiscardLogger,
		WithRenewInterval(""),
		WithIssuanceLockTTL(lockTTL),
	)

	_, err = manager.EnsureCertificate(ctx, domain)
	require.ErrorIs(t, err, ErrIssuanceTimeout)
}

// TestManagerFromKV_CorruptRecordFallsBackToIssue verifies that a corrupted certificate
// record already present in the cluster KV store (e.g. from an incompatible previous
// version, or storage corruption) is not fatal: Manager must log and fall back to issuing
// a fresh certificate rather than panicking on the malformed JSON.
func TestManagerFromKV_CorruptRecordFallsBackToIssue(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&internalClusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	node := multi.StartNode(ctx, "corrupt-kv-record")

	store, err := node.ActorSystem().KV()
	require.NoError(t, err)

	const domain = "corrupt-kv.example.com"
	require.NoError(t, store.Put(ctx, certKVKeyPrefix+domain, []byte("{not valid json")))

	issuer := &internalFakeIssuer{ttl: time.Hour}
	manager := NewManager(node.ActorSystem(), log.DiscardLogger,
		WithCertIssuer(issuer),
		WithRenewInterval(""),
		WithIssuanceLockTTL(10*time.Second),
	)

	var ensureErr error
	require.NotPanics(t, func() {
		_, ensureErr = manager.EnsureCertificate(ctx, domain)
	})
	require.NoError(t, ensureErr)
	require.Equal(t, 1, issuer.calls, "the corrupt KV record must be discarded and re-issued, exactly once")
}
