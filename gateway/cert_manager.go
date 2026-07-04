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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/kv"
	"github.com/tochemey/goakt/v4/log"
)

const (
	// certRenewalActorName is the name of the internal actor that receives the renewal
	// schedule's cron tick. It intentionally does not start with the reserved "GoAkt"
	// actor name prefix.
	certRenewalActorName = "goaktGatewayCertRenewal"
	// certRenewalReference is the ScheduleOption reference used so the renewal schedule
	// can be canceled by Manager.Stop.
	certRenewalReference = "goakt-gateway-cert-renewal"

	certKVKeyPrefix   = "gateway:cert:"
	certLockKeyPrefix = "gateway:cert-lock:"

	defaultRenewBefore    = 30 * 24 * time.Hour
	defaultLockTTL        = 2 * time.Minute
	defaultRenewInterval  = "@every 1h"
	defaultWaitPollPeriod = 100 * time.Millisecond
)

// cachedCert is the parsed, ready-to-serve form of a Certificate kept in Manager's hot
// in-memory cache.
type cachedCert struct {
	tlsCert  *tls.Certificate
	notAfter time.Time
}

// Manager terminates TLS for one or more domains from certificates shared across the
// whole cluster: issuance for a given domain is arbitrated so exactly one node calls the
// configured CertIssuer, the result is distributed to every node through the actor
// system's cluster KV store (see kv.Store), and a cluster-single-fire schedule (see
// actor.WithClusterSingleFire) drives renewal ahead of expiry.
//
// Outside cluster mode, Manager still works: issuance is deduplicated locally (so
// concurrent handshakes for a cold domain only call the issuer once) and certificates
// are kept in the configured CertStore only.
//
// A Manager is safe for concurrent use. Use NewManager to construct one.
type Manager struct {
	system actor.ActorSystem
	logger log.Logger

	issuer         CertIssuer
	store          CertStore
	allowedDomains map[string]struct{}
	renewBefore    time.Duration
	lockTTL        time.Duration
	renewInterval  string

	// g deduplicates concurrent EnsureCertificate/renewCertificate calls for the same
	// domain on this node, independent of (and layered underneath) the cluster-wide
	// TryLock arbitration doEnsure performs when clustering is enabled.
	g singleflight.Group

	mu    sync.RWMutex
	certs map[string]*cachedCert

	renewalPID *actor.PID
}

// ManagerOption configures a Manager created with NewManager.
type ManagerOption func(*Manager)

// WithCertIssuer sets the CertIssuer used to obtain certificates that are not already
// cached or stored. Without one, Manager can only serve certificates that a CertStore
// already has (e.g. pre-populated via CertStore.Put).
func WithCertIssuer(issuer CertIssuer) ManagerOption {
	return func(m *Manager) { m.issuer = issuer }
}

// WithCertStore overrides the persistent CertStore layer. Defaults to a MemoryCertStore.
func WithCertStore(store CertStore) ManagerOption {
	return func(m *Manager) { m.store = store }
}

// WithAllowedDomains restricts issuance/serving to the given domains. Without this
// option, any domain requested via GetCertificate/EnsureCertificate is allowed.
func WithAllowedDomains(domains ...string) ManagerOption {
	return func(m *Manager) {
		m.allowedDomains = make(map[string]struct{}, len(domains))
		for _, d := range domains {
			m.allowedDomains[d] = struct{}{}
		}
	}
}

// WithRenewBefore sets how far ahead of a certificate's expiry Manager renews it.
// Defaults to 30 days.
func WithRenewBefore(d time.Duration) ManagerOption {
	return func(m *Manager) { m.renewBefore = d }
}

// WithIssuanceLockTTL sets how long the cluster-wide issuance lock for a domain is held
// for. Defaults to 2 minutes; increase it if your CertIssuer can take longer than that.
func WithIssuanceLockTTL(d time.Duration) ManagerOption {
	return func(m *Manager) { m.lockTTL = d }
}

// WithRenewInterval sets the cron expression the renewal schedule uses to check for
// expiring certificates. Defaults to "@every 1h". Pass an empty string to disable
// automatic renewal entirely (EnsureCertificate/GetCertificate will still re-issue
// on-demand once a served certificate has actually expired).
func WithRenewInterval(cronExpression string) ManagerOption {
	return func(m *Manager) { m.renewInterval = cronExpression }
}

// NewManager creates a Manager backed by system. Call Start before serving traffic so
// the renewal schedule is registered, and Stop during shutdown to release it.
func NewManager(system actor.ActorSystem, logger log.Logger, opts ...ManagerOption) *Manager {
	if logger == nil {
		logger = log.DiscardLogger
	}
	m := &Manager{
		system:        system,
		logger:        logger,
		store:         NewMemoryCertStore(),
		renewBefore:   defaultRenewBefore,
		lockTTL:       defaultLockTTL,
		renewInterval: defaultRenewInterval,
		certs:         make(map[string]*cachedCert),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Start registers the cluster-single-fire renewal schedule. It is a no-op if renewal was
// disabled via WithRenewInterval("").
func (m *Manager) Start(ctx context.Context) error {
	if m.renewInterval == "" {
		return nil
	}

	pid, err := m.system.Spawn(ctx, certRenewalActorName, &certRenewalActor{manager: m}, actor.WithLongLived())
	if err != nil {
		return fmt.Errorf("gateway: failed to start certificate renewal actor: %w", err)
	}
	m.renewalPID = pid

	if err := m.system.ScheduleWithCron(ctx, &emptypb.Empty{}, pid, m.renewInterval,
		actor.WithClusterSingleFire(),
		actor.WithReference(certRenewalReference),
	); err != nil {
		return fmt.Errorf("gateway: failed to register certificate renewal schedule: %w", err)
	}
	return nil
}

// Stop cancels the renewal schedule and stops its backing actor. It is a no-op if Start
// was never called or renewal was disabled.
func (m *Manager) Stop(ctx context.Context) error {
	if m.renewalPID == nil {
		return nil
	}
	_ = m.system.CancelSchedule(certRenewalReference)
	err := m.renewalPID.Shutdown(ctx)
	m.renewalPID = nil
	return err
}

// GetCertificate implements the tls.Config.GetCertificate signature for SNI-based
// dynamic certificate lookup: assign it directly, e.g.
// tls.Config{GetCertificate: manager.GetCertificate}.
func (m *Manager) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	domain := hello.ServerName
	if domain == "" {
		return nil, fmt.Errorf("gateway: SNI server name required")
	}
	return m.EnsureCertificate(hello.Context(), domain)
}

// TLSConfig returns a *tls.Config wired to serve certificates dynamically by SNI through
// this Manager.
func (m *Manager) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: m.GetCertificate,
	}
}

// EnsureCertificate returns a ready-to-serve certificate for domain, issuing (or
// fetching from another node's issuance) one if necessary. Concurrent calls for the
// same domain on this node are deduplicated; concurrent calls across the cluster are
// arbitrated so only one node calls the configured CertIssuer (see Manager
// documentation).
//
// It returns ErrDomainNotAllowed if WithAllowedDomains was set and domain is not in it,
// and ErrNoIssuer if issuance is required but no CertIssuer was configured.
func (m *Manager) EnsureCertificate(ctx context.Context, domain string) (*tls.Certificate, error) {
	if m.allowedDomains != nil {
		if _, ok := m.allowedDomains[domain]; !ok {
			return nil, ErrDomainNotAllowed
		}
	}

	if cert, ok := m.hot(domain); ok {
		return cert, nil
	}

	v, err, _ := m.g.Do(domain, func() (any, error) {
		return m.doEnsure(ctx, domain, false)
	})
	if err != nil {
		return nil, err
	}
	return v.(*tls.Certificate), nil
}

// renewCertificate forces a fresh issuance for domain, bypassing the hot-cache/"already
// valid" fast path EnsureCertificate takes. It is only called by the renewal schedule.
func (m *Manager) renewCertificate(ctx context.Context, domain string) (*tls.Certificate, error) {
	v, err, _ := m.g.Do(domain, func() (any, error) {
		return m.doEnsure(ctx, domain, true)
	})
	if err != nil {
		return nil, err
	}
	return v.(*tls.Certificate), nil
}

// hot returns the cached certificate for domain if present and not within renewBefore
// of expiring.
func (m *Manager) hot(domain string) (*tls.Certificate, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cc, ok := m.certs[domain]
	if !ok || time.Until(cc.notAfter) < m.renewBefore {
		return nil, false
	}
	return cc.tlsCert, true
}

// cache stores cert in the hot in-memory cache and records its expiry for renewal
// bookkeeping.
func (m *Manager) cache(cert *Certificate, tlsCert *tls.Certificate) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.certs[cert.Domain] = &cachedCert{tlsCert: tlsCert, notAfter: cert.NotAfter}
}

// doEnsure performs the actual lookup/issuance for domain. It runs inside the
// per-domain singleflight group, so at most one goroutine per domain executes it on
// this node at a time. When force is true, an already-valid cached/stored certificate is
// not treated as sufficient - a fresh one is still fetched/issued (used by renewal).
func (m *Manager) doEnsure(ctx context.Context, domain string, force bool) (*tls.Certificate, error) {
	if !force {
		if cert, ok := m.hot(domain); ok {
			return cert, nil
		}
		if cert, ok := m.fromStore(ctx, domain); ok {
			return cert, nil
		}
	}

	if !m.system.InCluster() {
		return m.issueAndPersist(ctx, domain)
	}

	store, err := m.system.KV()
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to reach cluster KV store: %w", err)
	}

	if !force {
		if cert, ok := m.fromKV(ctx, store, domain); ok {
			return cert, nil
		}
	}

	lock, err := store.TryLock(ctx, certLockKeyPrefix+domain, m.lockTTL)
	if err != nil {
		if errors.Is(err, kv.ErrLockNotAcquired) {
			return m.waitForClusterCert(ctx, store, domain)
		}
		return nil, fmt.Errorf("gateway: failed to acquire cluster issuance lock for %q: %w", domain, err)
	}
	defer func() { _ = lock.Unlock(context.WithoutCancel(ctx)) }()

	// Someone may have finished issuing between our pre-lock KV read and acquiring the
	// lock; re-check once more now that we are the exclusive issuer for this domain.
	if !force {
		if cert, ok := m.fromKV(ctx, store, domain); ok {
			return cert, nil
		}
	}

	tlsCert, cert, err := m.issue(ctx, domain)
	if err != nil {
		return nil, err
	}

	if err := m.store.Put(ctx, cert); err != nil {
		m.logger.Warnf("gateway: failed to persist certificate for %q to local store: %v", domain, err)
	}

	record, err := encodeCertRecord(cert)
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to encode certificate for %q: %w", domain, err)
	}
	if err := store.Put(ctx, certKVKeyPrefix+domain, record); err != nil {
		m.logger.Warnf("gateway: failed to distribute certificate for %q via cluster KV: %v", domain, err)
	}

	m.cache(cert, tlsCert)
	return tlsCert, nil
}

// fromStore attempts to serve domain from the persistent CertStore layer.
func (m *Manager) fromStore(ctx context.Context, domain string) (*tls.Certificate, bool) {
	cert, err := m.store.Get(ctx, domain)
	if err != nil {
		return nil, false
	}
	if time.Until(cert.NotAfter) < m.renewBefore {
		return nil, false
	}
	tlsCert, err := parseCertificate(cert)
	if err != nil {
		m.logger.Warnf("gateway: stored certificate for %q is invalid: %v", domain, err)
		return nil, false
	}
	m.cache(cert, tlsCert)
	return tlsCert, true
}

// fromKV attempts to serve domain from the cluster-shared KV layer, additionally
// persisting a local copy so a subsequent single-node restart does not need the cluster.
func (m *Manager) fromKV(ctx context.Context, store kv.Store, domain string) (*tls.Certificate, bool) {
	data, err := store.Get(ctx, certKVKeyPrefix+domain)
	if err != nil {
		return nil, false
	}
	cert, err := decodeCertRecord(data)
	if err != nil {
		m.logger.Warnf("gateway: certificate record for %q in cluster KV is invalid: %v", domain, err)
		return nil, false
	}
	if time.Until(cert.NotAfter) < m.renewBefore {
		return nil, false
	}
	tlsCert, err := parseCertificate(cert)
	if err != nil {
		m.logger.Warnf("gateway: certificate for %q fetched from cluster KV is invalid: %v", domain, err)
		return nil, false
	}
	if err := m.store.Put(ctx, cert); err != nil {
		m.logger.Warnf("gateway: failed to persist certificate for %q fetched from cluster KV: %v", domain, err)
	}
	m.cache(cert, tlsCert)
	return tlsCert, true
}

// waitForClusterCert is taken by every node that lost the TryLock race for domain: it
// polls the cluster KV store until the winning node publishes a certificate, or gives up
// once the issuing node's lock would have expired anyway.
func (m *Manager) waitForClusterCert(ctx context.Context, store kv.Store, domain string) (*tls.Certificate, error) {
	deadline := time.Now().Add(m.lockTTL)
	ticker := time.NewTicker(defaultWaitPollPeriod)
	defer ticker.Stop()

	for {
		if cert, ok := m.fromKV(ctx, store, domain); ok {
			return cert, nil
		}
		if time.Now().After(deadline) {
			return nil, ErrIssuanceTimeout
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// issueAndPersist calls the issuer and stores the result locally only - used outside
// cluster mode, where there is no KV store to distribute to.
func (m *Manager) issueAndPersist(ctx context.Context, domain string) (*tls.Certificate, error) {
	tlsCert, cert, err := m.issue(ctx, domain)
	if err != nil {
		return nil, err
	}
	if err := m.store.Put(ctx, cert); err != nil {
		m.logger.Warnf("gateway: failed to persist certificate for %q to local store: %v", domain, err)
	}
	m.cache(cert, tlsCert)
	return tlsCert, nil
}

// issue calls the configured CertIssuer and parses the result into a *tls.Certificate.
func (m *Manager) issue(ctx context.Context, domain string) (*tls.Certificate, *Certificate, error) {
	if m.issuer == nil {
		return nil, nil, ErrNoIssuer
	}
	cert, err := m.issuer.Issue(ctx, domain)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: failed to issue certificate for %q: %w", domain, err)
	}
	tlsCert, err := parseCertificate(cert)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: issuer returned an invalid certificate for %q: %w", domain, err)
	}
	return tlsCert, cert, nil
}

// renewAll checks every domain this node has ever served for imminent expiry and renews
// it. It is invoked by certRenewalActor on the cluster-single-fire schedule tick.
func (m *Manager) renewAll(ctx context.Context) {
	m.mu.RLock()
	domains := make([]string, 0, len(m.certs))
	for domain, cc := range m.certs {
		if time.Until(cc.notAfter) < m.renewBefore {
			domains = append(domains, domain)
		}
	}
	m.mu.RUnlock()

	for _, domain := range domains {
		if _, err := m.renewCertificate(ctx, domain); err != nil {
			m.logger.Warnf("gateway: failed to renew certificate for %q: %v", domain, err)
		}
	}
}

// parseCertificate parses a Certificate's PEM material into a *tls.Certificate.
func parseCertificate(cert *Certificate) (*tls.Certificate, error) {
	tlsCert, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
	if err != nil {
		return nil, err
	}
	if len(tlsCert.Certificate) > 0 {
		leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
		if err == nil {
			tlsCert.Leaf = leaf
		}
	}
	return &tlsCert, nil
}

// certRenewalActor receives the renewal schedule's cron tick and delegates to
// Manager.renewAll. It exists only because ScheduleWithCron delivers to an actor.
type certRenewalActor struct {
	manager *Manager
}

// enforce compilation error
var _ actor.Actor = (*certRenewalActor)(nil)

// PreStart is called before the actor starts.
func (a *certRenewalActor) PreStart(*actor.Context) error { return nil }

// Receive handles the renewal schedule's cron tick.
func (a *certRenewalActor) Receive(ctx *actor.ReceiveContext) {
	switch ctx.Message().(type) {
	case *emptypb.Empty:
		a.manager.renewAll(ctx.Context())
	default:
		ctx.Unhandled()
	}
}

// PostStop is called when the actor is stopped.
func (a *certRenewalActor) PostStop(*actor.Context) error { return nil }
