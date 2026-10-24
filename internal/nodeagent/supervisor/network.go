package supervisor

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/config"
)

type NetworkAddress struct {
	Interface string
	IP        net.IP
}

type NetworkAddressSource interface {
	List() ([]NetworkAddress, error)
}

type systemNetworkAddressSource struct{}

func (systemNetworkAddressSource) List() ([]NetworkAddress, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list network interfaces: %w", err)
	}
	var result []NetworkAddress
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("list addresses for interface %q: %w", iface.Name, err)
		}
		for _, address := range addresses {
			var ip net.IP
			switch value := address.(type) {
			case *net.IPNet:
				ip = value.IP
			case *net.IPAddr:
				ip = value.IP
			}
			if ip != nil {
				result = append(result, NetworkAddress{Interface: iface.Name, IP: append(net.IP(nil), ip...)})
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Interface != result[j].Interface {
			return result[i].Interface < result[j].Interface
		}
		return result[i].IP.String() < result[j].IP.String()
	})
	return result, nil
}

func selectVLLMHostIP(
	requested string,
	trustedInterfaces []string,
	allowInsecure bool,
	source NetworkAddressSource,
) (string, bool, error) {
	if source == nil {
		return "", false, errors.New("network interface inventory is unavailable")
	}
	addresses, err := source.List()
	if err != nil {
		return "", false, err
	}
	trusted := make(map[string]struct{}, len(trustedInterfaces))
	for _, name := range trustedInterfaces {
		trusted[strings.TrimSpace(name)] = struct{}{}
	}
	var requestedIP net.IP
	if requested != "" {
		requestedIP = net.ParseIP(strings.Trim(requested, "[]"))
		if requestedIP == nil {
			return "", false, errors.New("configured vllm host_ip is not an IP address")
		}
	}
	var usable []NetworkAddress
	for _, address := range addresses {
		if address.IP == nil || !usableVLLMAddress(address.IP) {
			continue
		}
		if requestedIP != nil && !address.IP.Equal(requestedIP) {
			continue
		}
		usable = append(usable, address)
	}
	sort.Slice(usable, func(i, j int) bool {
		if usable[i].Interface != usable[j].Interface {
			return usable[i].Interface < usable[j].Interface
		}
		return usable[i].IP.String() < usable[j].IP.String()
	})
	if len(usable) == 0 {
		if requested != "" {
			return "", false, fmt.Errorf("configured VLLM_HOST_IP %q is not assigned to an active non-loopback interface", requested)
		}
		return "", false, errors.New("no active non-loopback interface has a usable IP address for vLLM")
	}
	for _, address := range usable {
		_, interfaceTrusted := trusted[address.Interface]
		if interfaceTrusted && address.IP.IsPrivate() {
			return address.IP.String(), false, nil
		}
	}
	if allowInsecure {
		return usable[0].IP.String(), true, nil
	}
	if requested != "" {
		return "", false, fmt.Errorf(
			"VLLM_HOST_IP %q must be private and assigned to an interface listed in trusted_network_interfaces",
			requested,
		)
	}
	return "", false, errors.New(
		"vLLM distributed traffic requires a private IP on an interface listed in trusted_network_interfaces; use WireGuard or an isolated VLAN, or explicitly set allow_insecure_shard_network",
	)
}

func usableVLLMAddress(ip net.IP) bool {
	return ip != nil && !ip.IsUnspecified() && !ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsMulticast()
}

func setEnvironmentValue(environment []string, key, value string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		name, _, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(name, key) {
			continue
		}
		result = append(result, entry)
	}
	return append(result, key+"="+value)
}

func containsWarning(warnings []string, warning string) bool {
	for _, existing := range warnings {
		if existing == warning {
			return true
		}
	}
	return false
}

func configuredRPCRelayEndpoint(cfg config.NodeConfig) string {
	listenAddr := cfg.RPCRelayListenAddr
	if listenAddr == "" {
		listenAddr = ":7452"
	}
	bindHost, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return ""
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return ""
	}
	host := strings.Trim(bindHost, "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		advertised, err := url.Parse(cfg.AdvertiseAddr)
		if err != nil || advertised.Hostname() == "" {
			return ""
		}
		host = advertised.Hostname()
	}
	return net.JoinHostPort(host, port)
}

func splitEndpointHostPort(endpoint string) (string, string, bool) {
	host, port, err := net.SplitHostPort(endpoint)
	return host, port, err == nil && host != "" && port != ""
}

func vllmChildEnvironment(configured config.ManagedEngineConfig, interfaces []string, allowInsecure bool, source NetworkAddressSource, parent []string) ([]string, bool, error) {
	if configured.Kind != config.ManagedEngineVLLM {
		return parent, false, nil
	}
	requested := ""
	if configured.Options.VLLM != nil {
		requested = configured.Options.VLLM.HostIP
	}
	hostIP, insecure, err := selectVLLMHostIP(requested, interfaces, allowInsecure, source)
	if err != nil {
		return nil, false, err
	}
	return setEnvironmentValue(parent, "VLLM_HOST_IP", hostIP), insecure, nil
}
