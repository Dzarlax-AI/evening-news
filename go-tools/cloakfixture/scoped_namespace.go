package cloakfixture

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"
)

var containerIdentity = regexp.MustCompile("^[0-9a-f]{64}$")

// scopedNamespace gives nft NET_ADMIN only in an immutable, verified owned
// container's network namespace. The helper has no host PID, mounts, Docker
// socket, SYS_ADMIN, or privileged mode, including on Docker Desktop.
func (g *dockerGate) scopedNamespace(ctx context.Context, name, input string, args ...string) (string, error) {
	if !slices.Equal(args, []string{"nft", "-f", "-"}) &&
		!slices.Equal(args, []string{"nft", "list", "chain", "inet", "news_fixture", "output"}) {
		return "", errors.New("namespace_arguments_invalid")
	}
	if len(input) > 16384 {
		return "", errors.New("namespace_arguments_invalid")
	}
	runner, ok := g.docker.(interface {
		RunInput(context.Context, string, ...string) (string, error)
	})
	if !ok {
		return "", errors.New("namespace_input_unavailable")
	}
	if err := g.verify(ctx, name, false); err != nil {
		return "", err
	}
	id, err := g.docker.Run(ctx, "inspect", "--format", "{{.Id}}", name)
	if err != nil || !containerIdentity.MatchString(id) {
		return "", errors.New("namespace_invalid")
	}
	if err := g.verify(ctx, id, false); err != nil {
		return "", err
	}
	g.namespaceSequence++
	helper := fmt.Sprintf("%s-namespace-%d", g.prefix, g.namespaceSequence)
	g.containers = append(g.containers, helper)
	argv := []string{"run", "--name", helper, "--label", ownerClass, "--label", g.label(),
		"--network", "container:" + id, "--cap-drop=ALL", "--cap-add=NET_ADMIN",
		"--security-opt=no-new-privileges:true", "--read-only",
		"--pids-limit", "32", "-i", "--entrypoint", "nft", g.namespaceImage}
	argv = append(argv, args[1:]...)
	runCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	out, runErr := runner.RunInput(runCtx, input, argv...)
	cancel()
	cleanCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	cleanErr := g.stopOwnedContainer(cleanCtx, helper)
	if runErr != nil || cleanErr != nil {
		return "", errors.New("namespace_operation_failed")
	}
	return out, nil
}
