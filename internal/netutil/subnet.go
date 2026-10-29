package netutil

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

func IsOnLocalSubnet(remoteIP net.IP) (bool, error) {
	if remoteIP == nil {
		return false, nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, err
	}
	return containsIP(addrs, remoteIP), nil
}

// IsLocalAddress reports whether remote is a loopback address or an exact
// address assigned to one of this machine's network interfaces.
func IsLocalAddress(remote net.Addr) (bool, error) {
	remoteIP := ipFromAddr(remote)
	if remoteIP == nil {
		return false, nil
	}
	if remoteIP.IsLoopback() {
		return true, nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, err
	}
	return isLocalAddress(addrs, remoteIP), nil
}

func containsIP(addrs []net.Addr, remoteIP net.IP) bool {
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && ipNet.Contains(remoteIP) {
			return true
		}
	}
	return false
}

func isLocalAddress(addrs []net.Addr, remoteIP net.IP) bool {
	if remoteIP == nil || remoteIP.IsLoopback() {
		return remoteIP != nil
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.Equal(remoteIP) {
			return true
		}
	}
	return false
}

func URLOnLocalSubnet(rawURL string) (bool, error) {
	return urlOnLocalSubnet(rawURL, net.DefaultResolver.LookupIPAddr, net.InterfaceAddrs)
}

func urlOnLocalSubnet(
	rawURL string,
	lookupIPAddr func(context.Context, string) ([]net.IPAddr, error),
	interfaceAddrs func() ([]net.Addr, error),
) (bool, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false, err
	}
	host := parsed.Hostname()
	if host == "" {
		return false, nil
	}

	remoteIPs := []net.IP{parseIP(host)}
	if remoteIPs[0] == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resolved, err := lookupIPAddr(ctx, host)
		if err != nil {
			return false, fmt.Errorf("resolve URL host %q: %w", host, err)
		}
		remoteIPs = make([]net.IP, 0, len(resolved))
		for _, addr := range resolved {
			remoteIPs = append(remoteIPs, addr.IP)
		}
	}

	if len(remoteIPs) == 0 {
		return false, nil
	}
	addrs, err := interfaceAddrs()
	if err != nil {
		return false, err
	}
	for _, remoteIP := range remoteIPs {
		if containsIP(addrs, remoteIP) {
			return true, nil
		}
	}
	return false, nil
}

func AreOnSameSubnet(local, remote net.Addr) (bool, error) {
	localIP := ipFromAddr(local)
	remoteIP := ipFromAddr(remote)
	if localIP == nil || remoteIP == nil {
		return false, nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, err
	}
	return sameSubnet(addrs, localIP, remoteIP), nil
}

func sameSubnet(addrs []net.Addr, localIP, remoteIP net.IP) bool {
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || !ipNet.IP.Equal(localIP) {
			continue
		}
		if ipNet.Contains(remoteIP) {
			return true
		}
	}
	return false
}

func ipFromAddr(addr net.Addr) net.IP {
	if addr == nil {
		return nil
	}
	switch value := addr.(type) {
	case *net.TCPAddr:
		return value.IP
	case *net.UDPAddr:
		return value.IP
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return nil
		}
		return parseIP(host)
	}
}

func parseIP(host string) net.IP {
	if zone := strings.LastIndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	return net.ParseIP(strings.Trim(host, "[]"))
}
