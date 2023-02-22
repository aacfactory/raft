package raft

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
)

type TLS interface {
	Config() (server *tls.Config, client *tls.Config, err error)
}

func TLSFiles(ca, serverCert, serverKey, clientCert, clientKey string) TLS {
	return &localTLS{
		caFilepath:         strings.TrimSpace(ca),
		serverCertFilepath: strings.TrimSpace(serverCert),
		serverKeyFilepath:  strings.TrimSpace(serverKey),
		clientCertFilepath: strings.TrimSpace(clientCert),
		clientKeyFilepath:  strings.TrimSpace(clientKey),
	}
}

type localTLS struct {
	caFilepath         string
	serverCertFilepath string
	serverKeyFilepath  string
	clientCertFilepath string
	clientKeyFilepath  string
}

func (l *localTLS) Config() (server *tls.Config, client *tls.Config, err error) {
	// ca
	cas := x509.NewCertPool()
	if l.caFilepath != "" {
		pem, readErr := os.ReadFile(l.caFilepath)
		if readErr != nil {
			err = errors.Join(errors.New("read ca file failed"), readErr)
			return
		}
		if !cas.AppendCertsFromPEM(pem) {
			err = errors.New("append ca into cert pool failed")
			return
		}
	}
	// server
	if l.serverKeyFilepath != "" && l.serverCertFilepath != "" {
		certPEM, readCertErr := os.ReadFile(l.serverCertFilepath)
		if readCertErr != nil {
			err = errors.Join(errors.New("read server cert pem file failed"), readCertErr)
			return
		}
		keyPEM, readKeyErr := os.ReadFile(l.serverKeyFilepath)
		if readKeyErr != nil {
			err = errors.Join(errors.New("read server key pem file failed"), readKeyErr)
			return
		}
		cert, certErr := tls.X509KeyPair(certPEM, keyPEM)
		if certErr != nil {
			err = errors.Join(errors.New("build server x509 key pair failed"), certErr)
			return
		}
		server = &tls.Config{
			ClientCAs:    cas,
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}
	}
	// client
	if l.clientCertFilepath != "" && l.clientKeyFilepath != "" {
		certPEM, readCertErr := os.ReadFile(l.clientCertFilepath)
		if readCertErr != nil {
			err = errors.Join(errors.New("read client cert pem file failed"), readCertErr)
			return
		}
		keyPEM, readKeyErr := os.ReadFile(l.clientKeyFilepath)
		if readKeyErr != nil {
			err = errors.Join(errors.New("read client key pem file failed"), readKeyErr)
			return
		}
		cert, certErr := tls.X509KeyPair(certPEM, keyPEM)
		if certErr != nil {
			err = errors.Join(errors.New("build client x509 key pair failed"), certErr)
			return
		}
		client = &tls.Config{
			RootCAs:            cas,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}
	}
	if server == nil {
		err = errors.New("no server tls config was built")
		return
	}
	return
}
