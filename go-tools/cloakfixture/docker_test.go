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
