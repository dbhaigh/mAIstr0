package netutil

import (
	"context"
	"net"
	"testing"
)

func TestSameSubnetRequiresMatchingLocalInterfaceAndPrefix(t *testing.T) {
	tests := []struct {
		name     string
		local    net.IP
		remote   net.IP
		expected bool
	}{
		{name: "same IPv4 prefix", local: net.ParseIP("192.168.10.2"), remote: net.ParseIP("192.168.10.99"), expected: true},
		{name: "different IPv4 prefix", local: net.ParseIP("192.168.10.2"), remote: net.ParseIP("192.168.11.2")},
		{name: "different local interface", local: net.ParseIP("192.168.20.2"), remote: net.ParseIP("192.168.10.99")},
		{name: "same IPv6 prefix", local: net.ParseIP("2001:db8:1::2"), remote: net.ParseIP("2001:db8:1::99"), expected: true},
		{name: "different IPv6 prefix", local: net.ParseIP("2001:db8:1::2"), remote: net.ParseIP("2001:db8:2::2")},
	}
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("192.168.10.2").To4(), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("192.168.20.2").To4(), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("2001:db8:1::2"), Mask: net.CIDRMask(64, 128)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameSubnet(addrs, tt.local, tt.remote); got != tt.expected {
				t.Fatalf("sameSubnet() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestLocalAddressRequiresExactInterfaceMatch(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("192.168.10.2").To4(), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("2001:db8:1::2"), Mask: net.CIDRMask(64, 128)},
	}
	tests := []struct {
		name     string
		remote   net.IP
		expected bool
	}{
		{name: "exact IPv4 interface address", remote: net.ParseIP("192.168.10.2"), expected: true},
		{name: "same subnet but another host", remote: net.ParseIP("192.168.10.99")},
		{name: "exact IPv6 interface address", remote: net.ParseIP("2001:db8:1::2"), expected: true},
		{name: "loopback IPv4", remote: net.ParseIP("127.0.0.1"), expected: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isLocalAddress(addrs, tt.remote)
			if got != tt.expected {
				t.Fatalf("local address match = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestURLOnLocalSubnetResolvesHostnamesAgainstLocalPrefixes(t *testing.T) {
	localAddrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("192.168.10.2").To4(), Mask: net.CIDRMask(24, 32)},
	}
	tests := []struct {
		name     string
		resolved net.IP
		expected bool
	}{
		{name: "same local subnet", resolved: net.ParseIP("192.168.10.99"), expected: true},
		{name: "different subnet", resolved: net.ParseIP("192.168.11.99"), expected: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := urlOnLocalSubnet(
				"https://coordinator.example:7450",
				func(_ context.Context, host string) ([]net.IPAddr, error) {
					if host != "coordinator.example" {
						t.Fatalf("resolver host = %q, want coordinator.example", host)
					}
					return []net.IPAddr{{IP: tt.resolved}}, nil
				},
				func() ([]net.Addr, error) { return localAddrs, nil },
			)
			if err != nil || got != tt.expected {
				t.Fatalf("urlOnLocalSubnet() = %v, %v, want %v, nil", got, err, tt.expected)
			}
		})
	}
}

func TestURLOnLocalSubnetRejectsMalformedURL(t *testing.T) {
	if _, err := URLOnLocalSubnet("https://[::1"); err == nil {
		t.Fatal("URLOnLocalSubnet accepted a malformed URL")
	}
}
