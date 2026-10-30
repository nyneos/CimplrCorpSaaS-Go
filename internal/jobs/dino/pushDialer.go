package jobs

import (
	"errors"
	"net"
	"net/http"
	"syscall"
	"time"
)

var errPushPrivateAddress = errors.New("push endpoint resolves to a non-public address")

// pushHTTPClient sends Web Push requests. Subscription endpoints come from browsers, so
// the dialer refuses loopback, private, link-local and other non-public addresses after
// DNS resolution, which also covers names that resolve to internal hosts.
var pushHTTPClient = &http.Client{
	Timeout: bpwSendTimeout,
	Transport: &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
			Control: refuseNonPublicDial,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: bpwSendTimeout,
		MaxIdleConns:          20,
		IdleConnTimeout:       90 * time.Second,
	},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func refuseNonPublicDial(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || !isPublicIP(ip) {
		return errPushPrivateAddress
	}
	return nil
}

func isPublicIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		// 100.64.0.0/10 carrier-grade NAT and 0.0.0.0/8.
		if v4[0] == 100 && v4[1]&0xC0 == 64 || v4[0] == 0 {
			return false
		}
	}
	return true
}
