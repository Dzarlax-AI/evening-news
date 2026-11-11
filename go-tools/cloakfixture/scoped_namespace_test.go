package cloakfixture

import (
	"context"
	"strings"
	"testing"
)

type scopedDocker struct {
	fakeDocker
	identity string
	inputs   int
}

func (d *scopedDocker) Run(ctx context.Context, args ...string) (string, error) {
	if len(args) > 2 && args[0] == "inspect" && args[2] == "{{.Id}}" {
		return d.identity, nil
	}
	return d.fakeDocker.Run(ctx, args...)
}

func (d *scopedDocker) RunInput(ctx context.Context, input string, args ...string) (string, error) {
	d.inputs++
	for i, arg := range args {
		if arg == "--name" {
			d.labels[args[i+1]] = "right"
		}
	}
	return d.fakeDocker.Run(ctx, args...)
}

func TestScopedNamespaceHasOnlyOwnedNetworkAdministration(t *testing.T) {
	id := strings.Repeat("a", 64)
	d := &scopedDocker{fakeDocker: fakeDocker{labels: map[string]string{"anchor": "right", id: "right"}}, identity: id}
	g := &dockerGate{docker: d, id: "right", prefix: "owned", namespaceImage: "fixture-nft:local"}
	if _, err := g.namespace(context.Background(), "anchor", "table inet news_fixture {}", "nft", "-f", "-"); err != nil {
		t.Fatal(err)
	}
	if d.inputs != 1 || len(g.containers) != 0 {
		t.Fatal("namespace helper was not removed")
	}
	for _, args := range d.calls {
		if args[0] != "run" {
			continue
		}
		joined := strings.Join(args, " ")
		for _, forbidden := range []string{"--privileged", "--pid ", "--pid=", "--mount", " -v ", "docker.sock", "SYS_ADMIN"} {
			if strings.Contains(joined, forbidden) {
				t.Fatalf("namespace helper gained broad access: %s", forbidden)
			}
		}
		for _, required := range []string{"--network container:" + id, "--cap-drop=ALL", "--cap-add=NET_ADMIN", "--read-only"} {
			if !strings.Contains(joined, required) {
				t.Fatalf("namespace helper lost boundary: %s", required)
			}
		}
	}
}

func TestScopedNamespaceRejectsForeignIdentityAndArbitraryCommands(t *testing.T) {
	id := strings.Repeat("a", 64)
	d := &scopedDocker{fakeDocker: fakeDocker{labels: map[string]string{"anchor": "right", id: "foreign"}}, identity: id}
	g := &dockerGate{docker: d, id: "right", prefix: "owned", namespaceImage: "fixture-nft:local"}
	if _, err := g.namespace(context.Background(), "anchor", "", "nft", "list", "chain", "inet", "news_fixture", "output"); err == nil || d.inputs != 0 {
		t.Fatal("foreign namespace accepted")
	}
	d.labels[id] = "right"
	if _, err := g.namespace(context.Background(), "anchor", "", "sh", "-c", "untrusted"); err == nil || d.inputs != 0 {
		t.Fatal("arbitrary namespace command accepted")
	}
}
