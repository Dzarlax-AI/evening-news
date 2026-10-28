package cloakfixture

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

const ownerKey = "com.dzarlax.native-cloak-concurrency.run"
const ownerClass = "com.dzarlax.native-cloak-concurrency=v1"
const cloakImage = "cloakhq/cloakbrowser:0.5.2@sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4"
const alpineImage = "alpine:3.21"

type DockerRunner interface {
	Run(context.Context, ...string) (string, error)
}
type systemDocker struct{}

func (systemDocker) Run(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	var b bytes.Buffer
	cmd.Stdout = &limitedWriter{writer: &b, left: 65536}
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return "", errors.New("docker_operation_failed")
	}
	return strings.TrimSpace(b.String()), nil
}

type limitedWriter struct {
	writer io.Writer
	left   int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) > w.left {
		p = p[:w.left]
	}
	if len(p) > 0 {
		_, _ = w.writer.Write(p)
		w.left -= len(p)
	}
	return n, nil
}

type dockerGate struct {
	docker               DockerRunner
	id, prefix           string
	containers, networks []string
}

func newDockerGate(d DockerRunner) (*dockerGate, error) {
	var id [8]byte
	if _, e := rand.Read(id[:]); e != nil {
		return nil, errors.New("gate_entropy_failed")
	}
	s := hex.EncodeToString(id[:])
	return &dockerGate{docker: d, id: s, prefix: "news-cloak-concurrency-" + s}, nil
}
func (g *dockerGate) label() string { return ownerKey + "=" + g.id }
func (g *dockerGate) network(ctx context.Context, suffix, subnet string) error {
	name := g.prefix + "-" + suffix
	g.networks = append(g.networks, name)
	_, e := g.docker.Run(ctx, "network", "create", "--internal", "--subnet", subnet, "--label", ownerClass, "--label", g.label(), name)
	return e
}
func (g *dockerGate) verify(ctx context.Context, name string, network bool) error {
	args := []string{"inspect", "--format", `{{index .Config.Labels "` + ownerKey + `"}}`, name}
	if network {
		args = []string{"network", "inspect", "--format", `{{index .Labels "` + ownerKey + `"}}`, name}
	}
	out, e := g.docker.Run(ctx, args...)
	if e != nil {
		return e
	}
	if out != g.id {
		return errors.New("ownership_mismatch")
	}
	return nil
}
func (g *dockerGate) container(ctx context.Context, role, image, network, ip, memory, cpu, pids string, mounts, env, extra, command []string) error {
	name := g.prefix + "-" + role
	g.containers = append(g.containers, name)
	args := []string{"run", "--pull=never", "-d", "--name", name, "--label", ownerClass, "--label", g.label(), "--restart=no", "--cap-drop=ALL", "--security-opt=no-new-privileges:true", "--platform=linux/amd64", "--memory", memory, "--memory-swap", memory, "--cpus", cpu, "--pids-limit", pids, "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=256m"}
	if strings.HasPrefix(network, "@") {
		args = append(args, "--network", "container:"+g.prefix+"-"+strings.TrimPrefix(network, "@"))
	} else {
		args = append(args, "--network", g.prefix+"-"+network, "--ip", ip, "--dns", "127.0.0.1")
	}
	for _, m := range mounts {
		args = append(args, "-v", m)
	}
	for _, v := range env {
		args = append(args, "-e", v)
	}
	args = append(args, extra...)
	args = append(args, image)
	args = append(args, command...)
	if _, e := g.docker.Run(ctx, args...); e != nil {
		return e
	}
	return g.verify(ctx, name, false)
}
func (g *dockerGate) alive(ctx context.Context) error {
	for _, name := range g.containers {
		if e := g.verify(ctx, name, false); e != nil {
			return e
		}
		out, e := g.docker.Run(ctx, "inspect", "--format", "{{.State.Running}} {{.RestartCount}} {{.State.OOMKilled}}", name)
		if e != nil || out != "true 0 false" {
			return errors.New("runtime_stopped_restarted_or_oom")
		}
		out, e = g.docker.Run(ctx, "exec", name, "sh", "-c", `if test -r /sys/fs/cgroup/memory.events; then cat /sys/fs/cgroup/memory.events; else exit 1; fi`)
		if e != nil {
			return errors.New("oom_metrics_unavailable")
		}
		found := map[string]bool{}
		for _, line := range strings.Split(out, "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && (f[0] == "oom" || f[0] == "oom_kill") {
				found[f[0]] = true
				if f[1] != "0" {
					return errors.New("container_oom_event_detected")
				}
			}
		}
		if !found["oom"] || !found["oom_kill"] {
			return errors.New("oom_metrics_unavailable")
		}
	}
	return nil
}
func (g *dockerGate) cleanup(ctx context.Context) error {
	var failures int
	for i := len(g.containers) - 1; i >= 0; i-- {
		n := g.containers[i]
		if g.verify(ctx, n, false) != nil {
			failures++
			continue
		}
		if _, e := g.docker.Run(ctx, "rm", "-f", n); e != nil {
			failures++
		}
	}
	for i := len(g.networks) - 1; i >= 0; i-- {
		n := g.networks[i]
		if g.verify(ctx, n, true) != nil {
			failures++
			continue
		}
		if _, e := g.docker.Run(ctx, "network", "rm", n); e != nil {
			failures++
		}
	}
	if failures > 0 {
		return fmt.Errorf("cleanup_failed")
	}
	return nil
}
