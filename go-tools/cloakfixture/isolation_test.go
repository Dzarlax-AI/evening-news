package cloakfixture

import (
	"strings"
	"testing"
)

func TestNamespacePoliciesFailClosedAndUseExactPeers(t *testing.T) {
	ips := map[string]string{"browser": "172.29.20.2", "worker": "172.29.20.3", "observer": "172.29.20.4", "gateway": "172.29.20.5", "origin": "11.203.254.242"}
	for _, role := range []string{"browser", "worker", "observer", "gateway", "origin"} {
		policy, err := nftPolicy(role, ips)
		if err != nil {
			t.Fatal(err)
		}
		for _, part := range []string{"policy drop", "meta nfproto ipv6 counter drop", "ct state invalid counter drop", "counter drop"} {
			if !strings.Contains(policy, part) {
				t.Errorf("%s missing %s", role, part)
			}
		}
		if strings.Contains(policy, "169.254.169.254") || strings.Contains(policy, " dport 80 ") {
			t.Errorf("%s allowed forbidden traffic", role)
		}
	}
	ips["browser"] = "1.2.3.4; accept"
	if _, err := nftPolicy("browser", ips); err == nil {
		t.Fatal("policy injection accepted")
	}
}
func TestDropCounterRequiresActualCountedDenial(t *testing.T) {
	s := "counter packets 3 bytes 120 drop\ncounter packets 7 bytes 280 drop"
	m := dropCounter.FindAllStringSubmatch(s, -1)
	if len(m) != 2 || m[0][1] != "3" || m[1][1] != "7" {
		t.Fatalf("invalid counters: %v", m)
	}
}

func TestProbeRequiresGatewayCountAndOnlyAcceptsExplicitPrivateNoRoute(t *testing.T) {
	cases := []struct {
		target, result string
		before, after  uint64
		want           bool
	}{{"172.29.20.1:80", "blocked", 3, 4, true}, {"172.29.20.1:80", "network_unreachable", 3, 3, false}, {"172.29.20.1:80", "blocked", 3, 3, false}, {"10.254.254.254:80", "network_unreachable", 3, 3, true}, {"10.254.254.254:80", "blocked", 3, 3, false}, {"10.254.254.254:80", "blocked", 3, 4, true}, {"127.0.0.1:5100", "network_unreachable", 3, 3, false}, {"10.254.254.254:80", "unexpected", 3, 4, false}}
	for _, c := range cases {
		if got := probeAccepted(c.target, c.result, c.before, c.after); got != c.want {
			t.Errorf("%+v got %v", c, got)
		}
	}
}
func TestDropCountRequiresOutputDefaultDropPolicy(t *testing.T) {
	valid := "chain output {\n type filter hook output priority -150; policy drop;\n counter packets 3 bytes 120 drop\n}"
	if n, e := parseDropCount(valid); e != nil || n != 3 {
		t.Fatalf("%d %v", n, e)
	}
	if _, e := parseDropCount(strings.Replace(valid, "policy drop", "policy accept", 1)); e == nil {
		t.Fatal("accepted non-drop policy")
	}
	if _, e := parseDropCount(strings.Replace(valid, "counter packets 3 bytes 120 drop", "", 1)); e == nil {
		t.Fatal("accepted missing drop counter")
	}
}
