package rpc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// certBundle is a generated CA plus a server and client leaf cert, written to
// temp files so tests exercise the file-based mTLS load path.
type certBundle struct {
	caCertPEM      []byte
	serverCert     tls.Certificate
	clientCertPath string
	clientKeyPath  string
	caCertPath     string
	serverName     string
}

// genCerts builds a self-signed CA and issues a server cert (for the given DNS
// name / loopback IP) and a client cert, persisting the client material and CA
// to disk under t.TempDir().
func genCerts(t *testing.T) *certBundle {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "evm-tools-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	const serverName = "localhost"
	serverCert := issueLeaf(t, caCert, caKey, serverName, true)
	clientCert := issueLeaf(t, caCert, caKey, "evm-tools-test-client", false)

	caCertPath := filepath.Join(dir, "ca.crt")
	mustWrite(t, caCertPath, caCertPEM, 0o644)

	clientCertPath := filepath.Join(dir, "client.crt")
	clientKeyPath := filepath.Join(dir, "client.key")
	mustWrite(t, clientCertPath, certPEM(clientCert), 0o644)
	mustWrite(t, clientKeyPath, keyPEM(t, clientCert), 0o600)

	return &certBundle{
		caCertPEM:      caCertPEM,
		serverCert:     serverCert.tlsCert,
		clientCertPath: clientCertPath,
		clientKeyPath:  clientKeyPath,
		caCertPath:     caCertPath,
		serverName:     serverName,
	}
}

type leaf struct {
	tlsCert tls.Certificate
	key     *ecdsa.PrivateKey
	der     []byte
}

func issueLeaf(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, server bool) leaf {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{cn}
		tmpl.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	tlsCert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	return leaf{tlsCert: tlsCert, key: key, der: der}
}

func certPEM(l leaf) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: l.der})
}

func keyPEM(t *testing.T, l leaf) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(l.key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func mustWrite(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// clientCAPool returns a pool trusting the bundle's CA, for the test server's
// ClientCAs (so it accepts the generated client cert).
func (b *certBundle) clientCAPool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(b.caCertPEM)
	return pool
}
