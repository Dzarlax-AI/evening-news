package cloakfixture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var safeIPv4 = regexp.MustCompile(`^\d{1,3}(?:\.\d{1,3}){3}$`)

func nftPolicy(role string, ips map[string]string) (string, error) {
	for _, v := range ips {
		ip := net.ParseIP(v)
		if !safeIPv4.MatchString(v) || ip == nil || ip.To4() == nil {
			return "", errors.New("policy_address_invalid")
		}
	}
	b, w, o, g, x := ips["browser"], ips["worker"], ips["observer"], ips["gateway"], ips["origin"]
	var out, in string
	switch role {
	case "browser":
		out = fmt.Sprintf("ip daddr %s tcp dport 8081 ct state { new, established } accept\nip daddr { %s, %s } tcp sport 9222 ct state established accept\n", g, w, o)
		in = fmt.Sprintf("ip saddr %s tcp sport 8081 ct state established accept\nip saddr { %s, %s } tcp dport 9222 ct state { new, established } accept\n", g, w, o)
		out += "meta skuid 65534 oifname \"lo\" ip daddr 127.0.0.1 tcp dport 5100 ct state { new, established } accept\nmeta skuid 0 oifname \"lo\" ip saddr 127.0.0.1 tcp sport 5100 ct state established accept\n"
		in += "iifname \"lo\" ip daddr 127.0.0.1 tcp dport 5100 accept\niifname \"lo\" ip saddr 127.0.0.1 tcp sport 5100 ct state established accept\n"
	case "worker":
		out = fmt.Sprintf("ip daddr %s tcp dport 9222 ct state { new, established } accept\nip daddr %s tcp dport 8081 ct state { new, established } accept\nip daddr %s tcp sport 8080 ct state established accept\n", b, g, o)
		in = fmt.Sprintf("ip saddr %s tcp sport 9222 ct state established accept\nip saddr %s tcp sport 8081 ct state established accept\nip saddr %s tcp dport 8080 ct state { new, established } accept\n", b, g, o)
		out += "oifname \"lo\" ip daddr 127.0.0.1 tcp dport 8080 accept\noifname \"lo\" ip saddr 127.0.0.1 tcp sport 8080 ct state established accept\n"
		in += "iifname \"lo\" ip daddr 127.0.0.1 tcp dport 8080 accept\niifname \"lo\" ip saddr 127.0.0.1 tcp sport 8080 ct state established accept\n"
	case "observer":
		out = fmt.Sprintf("ip daddr %s tcp dport 8080 ct state { new, established } accept\nip daddr %s tcp dport 9222 ct state { new, established } accept\n", w, b)
		in = fmt.Sprintf("ip saddr %s tcp sport 8080 ct state established accept\nip saddr %s tcp sport 9222 ct state established accept\n", w, b)
	case "gateway":
		out = fmt.Sprintf("ip daddr { %s, %s } tcp sport 8081 ct state established accept\nip daddr %s tcp dport 443 ct state { new, established } accept\n", b, w, x)
		in = fmt.Sprintf("ip saddr { %s, %s } tcp dport 8081 ct state { new, established } accept\nip saddr %s tcp sport 443 ct state established accept\n", b, w, x)
		out += "oifname \"lo\" ip daddr 127.0.0.1 tcp dport 8081 accept\noifname \"lo\" ip saddr 127.0.0.1 tcp sport 8081 ct state established accept\n"
		in += "iifname \"lo\" ip daddr 127.0.0.1 tcp dport 8081 accept\niifname \"lo\" ip saddr 127.0.0.1 tcp sport 8081 ct state established accept\n"
		out += fmt.Sprintf("oifname \"lo\" ip daddr %s tcp dport 8081 accept\noifname \"lo\" ip saddr %s tcp sport 8081 ct state established accept\n", g, g)
		in += fmt.Sprintf("iifname \"lo\" ip daddr %s tcp dport 8081 accept\niifname \"lo\" ip saddr %s tcp sport 8081 ct state established accept\n", g, g)
	case "origin":
		out = fmt.Sprintf("ip daddr 11.203.254.243 tcp sport 443 ct state established accept\n")
		in = fmt.Sprintf("ip saddr 11.203.254.243 tcp dport 443 ct state { new, established } accept\n")
		out += "oifname \"lo\" ip daddr 127.0.0.1 tcp dport 443 accept\noifname \"lo\" ip saddr 127.0.0.1 tcp sport 443 ct state established accept\n"
		in += "iifname \"lo\" ip daddr 127.0.0.1 tcp dport 443 accept\niifname \"lo\" ip saddr 127.0.0.1 tcp sport 443 ct state established accept\n"
	default:
		return "", errors.New("policy_role_invalid")
	}
	return "table inet news_fixture {\nchain output { type filter hook output priority -150; policy drop;\nmeta nfproto ipv6 counter drop\nct state invalid counter drop\n" + out + "counter drop\n}\nchain input { type filter hook input priority -150; policy drop;\nmeta nfproto ipv6 counter drop\nct state invalid counter drop\n" + in + "counter drop\n}\nchain forward { type filter hook forward priority -150; policy drop; }\n}\n", nil
}
func (g *dockerGate) namespace(ctx context.Context, name string, input string, args ...string) (string, error) {
	if e := g.verify(ctx, name, false); e != nil {
		return "", e
	}
	pid, e := g.docker.Run(ctx, "inspect", "--format", "{{.State.Pid}}", name)
	n, e2 := strconv.Atoi(pid)
	if e != nil || e2 != nil || n <= 1 {
		return "", errors.New("namespace_invalid")
	}
	f, e := os.Open(filepath.Join("/proc", pid, "ns/net"))
	if e != nil {
		return "", errors.New("namespace_invalid")
	}
	defer f.Close()
	own, e := os.Stat("/proc/self/ns/net")
	if e != nil {
		return "", errors.New("namespace_invalid")
	}
	actual, e := f.Stat()
	if e != nil || os.SameFile(own, actual) {
		return "", errors.New("namespace_invalid")
	}
	again, e := g.docker.Run(ctx, "inspect", "--format", "{{.State.Pid}}", name)
	if e != nil || again != pid {
		return "", errors.New("namespace_invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	argv := append([]string{"--net=/proc/self/fd/3", "--"}, args...)
	cmd := exec.CommandContext(ctx, "nsenter", argv...)
	cmd.ExtraFiles = []*os.File{f}
	cmd.Stdin = strings.NewReader(input)
	var b bytes.Buffer
	cmd.Stdout = &limitedWriter{writer: &b, left: 65536}
	cmd.Stderr = nil
	if cmd.Run() != nil {
		return "", errors.New("namespace_operation_failed")
	}
	return b.String(), nil
}

var dropCounter = regexp.MustCompile(`counter packets ([0-9]+) bytes [0-9]+ drop`)
var outputDefaultDrop = regexp.MustCompile(`(?m)type filter hook output[^\n]*policy drop`)

func parseDropCount(out string) (uint64, error) {
	if !outputDefaultDrop.MatchString(out) {
		return 0, errors.New("isolation_policy_invalid")
	}
	matches := dropCounter.FindAllStringSubmatch(out, -1)
	if len(matches) < 1 {
		return 0, errors.New("isolation_counter_unavailable")
	}
	var total uint64
	for _, row := range matches {
		n, e := strconv.ParseUint(row[1], 10, 64)
		if e != nil {
			return 0, errors.New("isolation_counter_unavailable")
		}
		total += n
	}
	return total, nil
}

func (g *dockerGate) dropCount(ctx context.Context, name string) (uint64, error) {
	out, e := g.namespace(ctx, name, "", "nft", "list", "chain", "inet", "news_fixture", "output")
	if e != nil {
		return 0, e
	}
	return parseDropCount(out)
}
func probeAccepted(target, result string, before, after uint64) bool {
	if result != "blocked" && result != "network_unreachable" {
		return false
	}
	if after > before {
		return true
	}
	return target == "10.254.254.254:80" && result == "network_unreachable" && after == before
}
func (g *dockerGate) probeDrops(ctx context.Context, role string, targets []string) error {
	name := g.prefix + "-" + role
	for _, target := range targets {
		before, e := g.dropCount(ctx, name)
		if e != nil {
			return e
		}
		result, e := g.docker.Run(ctx, "exec", name, "/fixture", "netprobe", "--target", target)
		if e != nil {
			return errors.New("namespace_bypass_gate_failed")
		}
		after, e := g.dropCount(ctx, name)
		if e != nil || !probeAccepted(target, result, before, after) {
			return errors.New("namespace_bypass_gate_failed")
		}
	}
	return nil
}
