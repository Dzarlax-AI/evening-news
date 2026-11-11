package cloakfixture

import (
	"context"
	"strings"
	"testing"
	"time"
)

type fakeDocker struct {
	calls  [][]string
	labels map[string]string
	oom    bool
}

func (f *fakeDocker) Run(_ context.Context, args ...string) (string, error) {
	f.calls = append(f.calls, append([]string{}, args...))
	switch {
	case len(args) > 0 && args[0] == "inspect":
		name := args[len(args)-1]
		if strings.Contains(strings.Join(args, " "), "Labels") {
			return f.labels[name], nil
		}
		return "true 0 false", nil
	case len(args) > 1 && args[0] == "network" && args[1] == "inspect":
		return f.labels[args[len(args)-1]], nil
	case args[0] == "exec":
		if f.oom {
			return "oom 1\noom_kill 0", nil
		}
		return "oom 0\noom_kill 0", nil
	default:
		return "", nil
	}
}
func TestCleanupRequiresExactRunOwnership(t *testing.T) {
	f := &fakeDocker{labels: map[string]string{"owned": "right", "foreign": "other", "network": "right"}}
	g := &dockerGate{docker: f, id: "right", containers: []string{"owned", "foreign"}, networks: []string{"network"}}
	if e := g.cleanup(context.Background()); e == nil {
		t.Fatal("foreign ownership accepted")
	}
	for _, c := range f.calls {
		if len(c) >= 3 && c[0] == "rm" && c[len(c)-1] == "foreign" {
			t.Fatal("removed foreign container")
		}
	}
}
func TestAliveRequiresOOMMetricsAndZeroEvents(t *testing.T) {
	f := &fakeDocker{labels: map[string]string{"owned": "right"}}
	g := &dockerGate{docker: f, id: "right", containers: []string{"owned"}}
	if e := g.alive(context.Background()); e != nil {
		t.Fatal(e)
	}
	f.oom = true
	if e := g.alive(context.Background()); e == nil || e.Error() != "container_oom_event_detected" {
		t.Fatal("OOM accepted")
	}
}
func TestContainerAppliesCapsAndNeverPulls(t *testing.T) {
	f := &fakeDocker{labels: map[string]string{"run-worker": "right"}}
	g := &dockerGate{docker: f, id: "right", prefix: "run"}
	if e := g.container(context.Background(), "worker", alpineImage, "control", "172.29.20.3", "256m", "1", "128", nil, nil, nil, []string{"/worker"}); e != nil {
		t.Fatal(e)
	}
	args := strings.Join(f.calls[0], " ")
	for _, required := range []string{"--pull=never", "--memory 256m", "--memory-swap 256m", "--cpus 1", "--pids-limit 128", "--cap-drop=ALL", "--read-only", "--network run-control", ownerKey + "=right"} {
		if !strings.Contains(args, required) {
			t.Errorf("missing %s", required)
		}
	}
}

func TestWorkerAnchorPublishesPublicFixtureAdmissionHost(t *testing.T) {
	f := &fakeDocker{labels: map[string]string{"run-worker-anchor": "right"}}
	g := &dockerGate{docker: f, id: "right", prefix: "run"}
	if err := g.container(context.Background(), "worker-anchor", alpineImage, "control", "172.29.20.3", "32m", "0.125", "32", nil, nil, workerAnchorExtra(), []string{"sleep", "300"}); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(f.calls[0], " ")
	if !strings.Contains(args, "--add-host native-fixture.test:11.203.254.242") || strings.Contains(args, "1.1.1.1") {
		t.Fatalf("worker anchor missing public fixture host entry: %s", args)
	}
}

type cancelAwareDocker struct{ fakeDocker }

func (f *cancelAwareDocker) Run(ctx context.Context, args ...string) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	return f.fakeDocker.Run(ctx, args...)
}
func TestCleanupUsesFreshContextAfterCancellation(t *testing.T) {
	f := &cancelAwareDocker{fakeDocker{labels: map[string]string{"owned": "id", "network": "id"}}}
	g := &dockerGate{docker: f, id: "id", containers: []string{"owned"}, networks: []string{"network"}}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.cleanup(cancelled); err == nil {
		t.Fatal("cancelled cleanup succeeded")
	}
	fresh, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := g.cleanup(fresh); err != nil {
		t.Fatal(err)
	}
	var actions []string
	for _, c := range f.calls {
		if len(c) > 0 && (c[0] == "rm" || (len(c) > 1 && c[0] == "network" && c[1] == "rm")) {
			actions = append(actions, strings.Join(c, " "))
		}
	}
	if len(actions) != 2 || actions[0] != "rm -f owned" || actions[1] != "network rm network" {
		t.Fatalf("cleanup ordering: %v", actions)
	}
}

type runtimeDocker struct{ fakeDocker }

// Run returns deterministic lifecycle state for owned-container evidence tests.
func (f *runtimeDocker) Run(ctx context.Context, args ...string) (string, error) {
	if len(args) > 2 && args[0] == "inspect" && strings.Contains(args[2], "ExitCode") {
		f.calls = append(f.calls, args)
		return "false 0 true 137", nil
	}
	return f.fakeDocker.Run(ctx, args...)
}

// TestRuntimeStatesAreFiniteAndOwned excludes foreign containers while retaining owned OOM and
// exit evidence.
func TestRuntimeStatesAreFiniteAndOwned(t *testing.T) {
	f := &runtimeDocker{fakeDocker{labels: map[string]string{"run-origin": "right", "run-foreign": "other"}}}
	g := &dockerGate{docker: f, id: "right", prefix: "run", containers: []string{"run-origin", "run-foreign"}}
	states := g.runtimeStates(context.Background())
	if len(states) != 1 || states[0].Role != "origin" || !states[0].OOMKilled || states[0].Running || states[0].ExitCode != 137 {
		t.Fatalf("invalid bounded OOM receipt: %+v", states)
	}
	for _, args := range f.calls {
		if len(args) > 2 && strings.Contains(args[2], "ExitCode") && args[len(args)-1] == "run-foreign" {
			t.Fatal("inspected foreign runtime")
		}
	}
}

// TestDesktopContainerPreservesIsolationWithoutMemoryOrCPUCaps checks that removing local
// compute caps preserves namespace and process isolation.
func TestDesktopContainerPreservesIsolationWithoutMemoryOrCPUCaps(t *testing.T) {
	f := &fakeDocker{labels: map[string]string{"run-cloak": "right"}}
	g := &dockerGate{docker: f, id: "right", prefix: "run", namespaceImage: "fixture-nft:local"}
	if err := g.container(context.Background(), "cloak", cloakImage, "control", "172.29.20.2", "768m", "0.5", "256", nil, nil, nil, []string{"/browser"}); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(f.calls[0], " ")
	for _, forbidden := range []string{"--memory", "--memory-swap", "--cpus"} {
		if strings.Contains(args, forbidden) {
			t.Fatalf("local cap retained: %s", forbidden)
		}
	}
	for _, required := range []string{"--network run-control", "--cap-drop=ALL", "--read-only", "--security-opt=no-new-privileges:true", "--pids-limit 256", ownerKey + "=right"} {
		if !strings.Contains(args, required) {
			t.Fatalf("isolation boundary lost: %s", required)
		}
	}
}
