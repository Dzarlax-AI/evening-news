package managedsetup

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

type renderedBrowserDocument struct {
	Services map[string]renderedBrowserService `json:"services"`
	Networks map[string]struct {
		Name     string `json:"name"`
		External any    `json:"external"`
		Internal bool   `json:"internal"`
		IPAM     struct {
			Config []struct {
				Subnet string `json:"subnet"`
			} `json:"config"`
		} `json:"ipam"`
	} `json:"networks"`
}

type renderedBrowserService struct {
	Networks map[string]struct {
		IPv4Address string `json:"ipv4_address"`
	} `json:"networks"`
	Init            bool     `json:"init"`
	ReadOnly        bool     `json:"read_only"`
	CapDrop         []string `json:"cap_drop"`
	SecurityOpt     []string `json:"security_opt"`
	PidsLimit       int      `json:"pids_limit"`
	MemLimit        string   `json:"mem_limit"`
	CPUs            float64  `json:"cpus"`
	StopGracePeriod string   `json:"stop_grace_period"`
}

func (s renderedBrowserService) limits() ContainerLimits {
	return ContainerLimits{Init: s.Init, ReadOnly: s.ReadOnly, CapDrop: s.CapDrop, SecurityOpt: s.SecurityOpt, PidsLimit: s.PidsLimit, MemLimit: s.MemLimit, CPUs: s.CPUs, StopGracePeriod: s.StopGracePeriod}
}

// ReadNativeBrowserOptions imports the checked-in native extractor and egress
// contract from a protected `docker compose config --format json` file.
func ReadNativeBrowserOptions(path string) (NativeBrowserOptions, error) {
	compose, err := ReadRenderedCompose(path)
	if err != nil {
		return NativeBrowserOptions{}, err
	}
	return NativeBrowserOptionsFromCompose(compose)
}

// NativeBrowserOptionsFromCompose validates and extracts the native-browser
// contract from an already protected rendered Compose document.
func NativeBrowserOptionsFromCompose(compose RenderedCompose) (NativeBrowserOptions, error) {
	var doc renderedBrowserDocument
	if len(compose.raw) == 0 || json.Unmarshal(compose.raw, &doc) != nil || len(doc.Services["extractor"].Networks) == 0 || len(doc.Services["egress"].Networks) == 0 {
		return NativeBrowserOptions{}, errors.New("native browser topology is missing from rendered Compose")
	}
	extractor := compose.Services["extractor"]
	egress := compose.Services["egress"]
	values := NativeBrowserOptions{ExtractorValues: map[string]string{}, EgressValues: map[string]string{}}
	values.ExtractorLimits = doc.Services["extractor"].limits()
	values.EgressLimits = doc.Services["egress"].limits()
	for _, key := range []string{"LISTEN_ADDR", "EXTRACTOR_MAX_CONCURRENT", "EXTRACTOR_BROWSER_CONCURRENT", "CHROME_ENDPOINT", "EXTRACTOR_HTTP_ONLY", "EXTRACTOR_BROWSER_BACKEND", "EXTRACTOR_NATIVE_PROXY_URL", "EXTRACTOR_NATIVE_PROXY_TOKEN", "EXTRACTOR_NATIVE_NETWORK_ISOLATED", "EXTRACTOR_NATIVE_READY_SELECTOR", "EXTRACTOR_NATIVE_DIAGNOSTICS"} {
		if value, ok := extractor[key]; ok {
			values.ExtractorValues[key] = value
		}
	}
	for _, key := range []string{"EGRESS_LISTEN_ADDR", "EGRESS_TOKEN", "EGRESS_ALLOW_WILDCARD_BIND", "EGRESS_MAX_CONCURRENT", "EGRESS_MAX_TUNNEL_BYTES", "EGRESS_MAX_LIFETIME_SECONDS", "EGRESS_TEST_MODE", "EGRESS_TEST_ALLOWLIST"} {
		if value, ok := egress[key]; ok {
			values.EgressValues[key] = value
		}
	}
	if values.ExtractorValues["EXTRACTOR_HTTP_ONLY"] != "false" || values.ExtractorValues["EXTRACTOR_BROWSER_BACKEND"] != "native" || values.ExtractorValues["EXTRACTOR_NATIVE_NETWORK_ISOLATED"] != "true" {
		return NativeBrowserOptions{}, errors.New("rendered Compose does not use the isolated native extractor")
	}
	control, outbound, err := browserNetworks(doc)
	if err != nil {
		return NativeBrowserOptions{}, err
	}
	values.ControlNetwork, values.ControlName = control[0], control[1]
	values.OutboundNetwork, values.OutboundName = outbound.name, outbound.nameValue
	values.ExtractorIP = doc.Services["extractor"].Networks[values.ControlNetwork].IPv4Address
	values.EgressIP = doc.Services["egress"].Networks[values.ControlNetwork].IPv4Address
	if err = values.Validate(); err != nil {
		return NativeBrowserOptions{}, err
	}
	if err = validateBrowserEndpointParity(values); err != nil {
		return NativeBrowserOptions{}, err
	}
	return values, nil
}

type outboundNetwork struct {
	name, nameValue string
}

func browserNetworks(doc renderedBrowserDocument) ([2]string, outboundNetwork, error) {
	var control [2]string
	var outbound outboundNetwork
	controlCount, outboundCount := 0, 0
	for name, attached := range doc.Services["extractor"].Networks {
		other, shared := doc.Services["egress"].Networks[name]
		if !shared {
			continue
		}
		definition, exists := doc.Networks[name]
		if !exists || definition.Name == "" {
			continue
		}
		var external bool
		switch v := definition.External.(type) {
		case bool:
			external = v
		case map[string]any:
			external = v["external"] == true
		}
		if external {
			if attached.IPv4Address != "" && other.IPv4Address != "" {
				control = [2]string{name, definition.Name}
				controlCount++
			}
			continue
		}
		if definition.Name != "" {
			outbound = outboundNetwork{name: name, nameValue: definition.Name}
			outboundCount++
		}
	}
	if controlCount != 1 || outboundCount != 1 {
		return control, outbound, errors.New("native browser requires one existing private control network and one dedicated outbound network")
	}
	if _, ok := doc.Services["extractor"].Networks[control[0]]; !ok {
		return control, outbound, errors.New("extractor is not attached to the private control network")
	}
	if _, ok := doc.Services["extractor"].Networks[outbound.name]; !ok {
		return control, outbound, errors.New("extractor is not attached to the dedicated outbound network")
	}
	if _, ok := doc.Services["egress"].Networks[control[0]]; !ok {
		return control, outbound, errors.New("egress is not attached to the private control network")
	}
	if _, ok := doc.Services["egress"].Networks[outbound.name]; !ok {
		return control, outbound, errors.New("egress is not attached to the dedicated outbound network")
	}
	for _, ip := range []string{doc.Services["extractor"].Networks[control[0]].IPv4Address, doc.Services["egress"].Networks[control[0]].IPv4Address} {
		parsed, err := netip.ParseAddr(ip)
		if err != nil || !parsed.IsPrivate() || parsed.Is4In6() {
			return control, outbound, errors.New("native browser control network addresses invalid")
		}
	}
	return control, outbound, nil
}

func validateBrowserEndpointParity(values NativeBrowserOptions) error {
	proxyURL, err := url.Parse(values.ExtractorValues["EXTRACTOR_NATIVE_PROXY_URL"])
	if err != nil || proxyURL.Scheme != "http" || proxyURL.User != nil || proxyURL.Hostname() == "" {
		return errors.New("native browser proxy endpoint invalid")
	}
	proxyHost, proxyPort, err := net.SplitHostPort(values.EgressValues["EGRESS_LISTEN_ADDR"])
	if err != nil || proxyHost != values.EgressIP || proxyURL.Hostname() != proxyHost || proxyURL.Port() != proxyPort {
		return errors.New("native browser proxy endpoint does not match the egress listener")
	}
	for _, key := range []string{"CHROME_ENDPOINT"} {
		endpoint, err := url.Parse(values.ExtractorValues[key])
		if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.Hostname() == "" {
			return fmt.Errorf("native browser endpoint %s invalid", key)
		}
	}
	return nil
}

// NativeBrowserComposeOverride adds only the two managed News services to the
// exact external-control and dedicated-outbound networks imported above.
func NativeBrowserComposeOverride(options NativeBrowserOptions) ([]byte, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf(`services:
  executor:
    networks:
      default: {}
      extraction-call: {}
  background:
    networks:
      default: {}
      extraction-call: {}
  extractor:
    init: %t
    read_only: %t
    cap_drop: %s
    security_opt: %s
    pids_limit: %d
    mem_limit: %s
    cpus: %g
    stop_grace_period: %s
    depends_on:
      egress:
        condition: service_healthy
    networks: !override
      extraction-call: {}
      %s:
        ipv4_address: %s
      %s: {}
  egress:
    init: %t
    read_only: %t
    cap_drop: %s
    security_opt: %s
    pids_limit: %d
    mem_limit: %s
    cpus: %g
    stop_grace_period: %s
    profiles: !override []
    networks: !override
      %s:
        ipv4_address: %s
      %s: {}
networks:
  extraction-call:
    internal: true
  %s:
    external: true
    name: %s
  %s:
    external: true
    name: %s
`, options.ExtractorLimits.Init, options.ExtractorLimits.ReadOnly, yamlStringList(options.ExtractorLimits.CapDrop), yamlStringList(options.ExtractorLimits.SecurityOpt), options.ExtractorLimits.PidsLimit, options.ExtractorLimits.MemLimit, options.ExtractorLimits.CPUs, options.ExtractorLimits.StopGracePeriod, options.ControlNetwork, options.ExtractorIP, options.OutboundNetwork, options.EgressLimits.Init, options.EgressLimits.ReadOnly, yamlStringList(options.EgressLimits.CapDrop), yamlStringList(options.EgressLimits.SecurityOpt), options.EgressLimits.PidsLimit, options.EgressLimits.MemLimit, options.EgressLimits.CPUs, options.EgressLimits.StopGracePeriod, options.ControlNetwork, options.EgressIP, options.OutboundNetwork, options.ControlNetwork, options.ControlName, options.OutboundNetwork, options.OutboundName)), nil
}

func yamlStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, fmt.Sprintf("%q", value))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
