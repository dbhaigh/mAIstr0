package supervisor

import (
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
)

type staticNetworkAddressSource []NetworkAddress

func (source staticNetworkAddressSource) List() ([]NetworkAddress, error) {
	return append([]NetworkAddress(nil), source...), nil
}

func TestSelectVLLMHostIPPreflight(t *testing.T) {
	tests := []struct {
		name          string
		requested     string
		trusted       []string
		allowInsecure bool
		addresses     []NetworkAddress
		wantIP        string
		wantInsecure  bool
		wantError     string
	}{
		{
			name:      "trusted RFC1918 interface",
			trusted:   []string{"wg0"},
			addresses: []NetworkAddress{{Interface: "wg0", IP: net.ParseIP("10.40.0.2")}},
			wantIP:    "10.40.0.2",
		},
		{
			name:      "trusted IPv6 unique local address",
			trusted:   []string{"wg0"},
			addresses: []NetworkAddress{{Interface: "wg0", IP: net.ParseIP("fd12:3456::2")}},
			wantIP:    "fd12:3456::2",
		},
		{
			name:      "untrusted private interface refused",
			addresses: []NetworkAddress{{Interface: "eth0", IP: net.ParseIP("192.168.1.20")}},
			wantError: "trusted_network_interfaces",
		},
		{
			name:          "explicit insecure opt-in",
			allowInsecure: true,
			addresses:     []NetworkAddress{{Interface: "eth0", IP: net.ParseIP("192.168.1.20")}},
			wantIP:        "192.168.1.20", wantInsecure: true,
		},
		{
			name:      "public trusted address is not accepted securely",
			trusted:   []string{"eth0"},
			addresses: []NetworkAddress{{Interface: "eth0", IP: net.ParseIP("203.0.113.20")}},
			wantError: "private",
		},
		{
			name:      "explicit address must belong to an active interface",
			requested: "10.40.0.3", trusted: []string{"wg0"}, allowInsecure: true,
			addresses: []NetworkAddress{{Interface: "wg0", IP: net.ParseIP("10.40.0.2")}},
			wantError: "not assigned",
		},
		{
			name:    "trusted private address selected over untrusted address",
			trusted: []string{"wg0"},
			addresses: []NetworkAddress{
				{Interface: "eth0", IP: net.ParseIP("192.168.1.20")},
				{Interface: "wg0", IP: net.ParseIP("10.40.0.2")},
			},
			wantIP: "10.40.0.2",
		},
		{
			name:          "loopback is not a distributed address",
			allowInsecure: true,
			addresses:     []NetworkAddress{{Interface: "lo", IP: net.ParseIP("127.0.0.1")}},
			wantError:     "no active non-loopback",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ip, insecure, err := selectVLLMHostIP(
				test.requested, test.trusted, test.allowInsecure,
				staticNetworkAddressSource(test.addresses),
			)
			if test.wantError != "" {
				if err == nil || !containsTestString(err.Error(), test.wantError) {
					t.Fatalf("selectVLLMHostIP() error = %v, want substring %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("selectVLLMHostIP() error = %v", err)
			}
			if ip != test.wantIP || insecure != test.wantInsecure {
				t.Fatalf("selectVLLMHostIP() = %q, insecure=%t; want %q, insecure=%t",
					ip, insecure, test.wantIP, test.wantInsecure)
			}
		})
	}
}

func TestVLLMEnvironmentReplacesInheritedValue(t *testing.T) {
	configured := configForVLLMHostIP("10.40.0.2")
	environment, insecure, err := vllmChildEnvironment(
		configured, []string{"wg0"}, false,
		staticNetworkAddressSource{{Interface: "wg0", IP: net.ParseIP("10.40.0.2")}},
		[]string{"VLLM_HOST_IP=public", "PATH=bin"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if insecure || !reflect.DeepEqual(environment, []string{"PATH=bin", "VLLM_HOST_IP=10.40.0.2"}) {
		t.Fatalf("vLLM environment = %#v, insecure=%t", environment, insecure)
	}
}

func TestVLLMPreflightFailsIfInterfaceInventoryFails(t *testing.T) {
	_, _, err := selectVLLMHostIP("", nil, false, failingNetworkAddressSource{})
	if err == nil || !errors.Is(err, errNetworkInventory) {
		t.Fatalf("selectVLLMHostIP() error = %v, want network inventory error", err)
	}
}

func containsTestString(value, part string) bool {
	return len(part) == 0 || (len(value) >= len(part) && strings.Contains(value, part))
}

func configForVLLMHostIP(hostIP string) config.ManagedEngineConfig {
	return config.ManagedEngineConfig{
		Kind:    config.ManagedEngineVLLM,
		Options: config.ManagedEngineOptions{VLLM: &config.VLLMOptions{HostIP: hostIP}},
	}
}

var errNetworkInventory = errors.New("network inventory failed")

type failingNetworkAddressSource struct{}

func (failingNetworkAddressSource) List() ([]NetworkAddress, error) {
	return nil, errNetworkInventory
}
