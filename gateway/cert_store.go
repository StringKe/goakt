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
	"encoding/json"
	"sync"
	"time"
)

// CertStore persists issued certificates so that a cluster cold start does not have to
// re-issue one for every hosted domain. Manager always keeps a hot, cluster-shared copy
// in the actor system's KV store (see kv.Store) for fast SNI lookups across nodes; a
// CertStore is the layer behind that - by default an in-memory MemoryCertStore, but
// applications facing a full-cluster restart should plug in a persistent implementation
// (e.g. backed by a database or object storage) to avoid depending on the issuer being
// reachable/willing to re-issue on every cold start.
//
// Implementations must be safe for concurrent use.
type CertStore interface {
	// Get returns the certificate stored for domain. It returns ErrCertificateNotFound
	// if none is stored.
	Get(ctx context.Context, domain string) (*Certificate, error)
	// Put stores cert, overwriting any previous certificate for the same domain.
	Put(ctx context.Context, cert *Certificate) error
	// Delete removes the certificate stored for domain, if any.
	Delete(ctx context.Context, domain string) error
}

// MemoryCertStore is the default, in-process CertStore. It does not survive a process
// restart, which is why Manager also always keeps a copy in the cluster KV store: on a
// single node restart the certificate is fetched back from the cluster, and on a full
// cluster cold start it is re-issued.
type MemoryCertStore struct {
	mu    sync.RWMutex
	certs map[string]*Certificate
}

// enforce compilation error
var _ CertStore = (*MemoryCertStore)(nil)

// NewMemoryCertStore creates an empty MemoryCertStore.
func NewMemoryCertStore() *MemoryCertStore {
	return &MemoryCertStore{certs: make(map[string]*Certificate)}
}

// Get implements CertStore.
func (m *MemoryCertStore) Get(_ context.Context, domain string) (*Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cert, ok := m.certs[domain]
	if !ok {
		return nil, ErrCertificateNotFound
	}
	return cert, nil
}

// Put implements CertStore.
func (m *MemoryCertStore) Put(_ context.Context, cert *Certificate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.certs[cert.Domain] = cert
	return nil
}

// Delete implements CertStore.
func (m *MemoryCertStore) Delete(_ context.Context, domain string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.certs, domain)
	return nil
}

// certRecord is the JSON wire format Manager uses to distribute a Certificate through
// the cluster KV store. Plain JSON keeps the cluster-shared certificate distribution
// path independent from GoAkt's proto-based remoting pipeline, since a CertStore/KV
// value is opaque bytes rather than an actor message.
type certRecord struct {
	Domain   string `json:"domain"`
	CertPEM  []byte `json:"cert_pem"`
	KeyPEM   []byte `json:"key_pem"`
	NotAfter int64  `json:"not_after_unix"`
}

func encodeCertRecord(cert *Certificate) ([]byte, error) {
	return json.Marshal(certRecord{
		Domain:   cert.Domain,
		CertPEM:  cert.CertPEM,
		KeyPEM:   cert.KeyPEM,
		NotAfter: cert.NotAfter.Unix(),
	})
}

func decodeCertRecord(data []byte) (*Certificate, error) {
	var record certRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	return &Certificate{
		Domain:   record.Domain,
		CertPEM:  record.CertPEM,
		KeyPEM:   record.KeyPEM,
		NotAfter: time.Unix(record.NotAfter, 0).UTC(),
	}, nil
}
