package cluster

// FluxDriver dispatches flux-uri clusters to a real flux instance via the `flux`
// CLI, instead of the emulator. With an empty URI it targets the ambient broker
// (the instance the fluxq server runs inside — `flux start`); with a URI it
// wraps calls in `flux proxy <uri> …` to reach a remote instance.
//
// It consumes the same Content the flux-uri transform emits (a `flux submit`
// command line). State is read from the job eventlog rather than `flux jobs
// --format`, whose template engine is unreliable across flux builds:
//   finish status=N  -> N==0 Completed, else Failed (exit N>>8 / signal N&0x7f)
//   start | alloc    -> Running
//   (otherwise)      -> Running (submitted, awaiting resources)

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/queue"
)

type FluxDriver struct {
	Timeout time.Duration // per-command timeout (default 15s)
}

func NewFluxDriver() *FluxDriver { return &FluxDriver{Timeout: 15 * time.Second} }

// fluxURI reads the per-cluster endpoint from dispatch config. "local" (or
// empty) means the ambient broker this server runs inside; anything else is a
// flux URI reached via `flux proxy <uri>` (e.g. ssh://host/run/flux/local).
func fluxURI(target graph.ClusterGraph) string {
	if u := target.Cfg("uri"); u != "local" {
		return u
	}
	return ""
}

func (d *FluxDriver) Type() graph.ManagerType { return graph.FluxURI }

func (d *FluxDriver) timeout() time.Duration {
	if d.Timeout <= 0 {
		return 15 * time.Second
	}
	return d.Timeout
}

// shellCmd runs a full command line (the transform's `flux submit …` payload),
// under `flux proxy` when a URI is set (proxy just sets FLUX_URI for the child).
func shellCmd(ctx context.Context, uri, line string) *exec.Cmd {
	if uri != "" {
		line = "flux proxy " + shquote(uri) + " " + line
	}
	return exec.CommandContext(ctx, "sh", "-c", line)
}

// fluxCmd runs `flux <args>` (proxied when a URI is set).
func fluxCmd(ctx context.Context, uri string, args ...string) *exec.Cmd {
	if uri != "" {
		args = append([]string{"proxy", uri, "flux"}, args...)
	}
	return exec.CommandContext(ctx, "flux", args...)
}

func run(cmd *exec.Cmd) (string, error) {
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if msg == "" {
			msg = err.Error()
		}
		return strings.TrimSpace(out.String()), fmt.Errorf("%s", msg)
	}
	return strings.TrimSpace(out.String()), nil
}

func (d *FluxDriver) Submit(target graph.ClusterGraph, c Content) (string, error) {
	if c.Kind != "command" {
		return "", fmt.Errorf("flux-uri expects a command, got %q", c.Kind)
	}
	if !strings.Contains(c.Payload, "flux submit") {
		return "", fmt.Errorf("not a 'flux submit' command: %q", c.Payload)
	}
	// The dispatch paper's exact bug — catch it before it hits the broker.
	if strings.Contains(c.Payload, "-in.") {
		return "", fmt.Errorf("malformed argument: input flag concatenated with filename (e.g. -in.reaxff)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	id, err := run(shellCmd(ctx, fluxURI(target), c.Payload))
	if err != nil {
		return "", fmt.Errorf("flux submit: %s", err)
	}
	if id == "" {
		return "", fmt.Errorf("flux submit returned no jobid")
	}
	// stdout may carry trailing lines; the jobid is the first token.
	return strings.Fields(id)[0], nil
}

func (d *FluxDriver) Status(target graph.ClusterGraph, handle string) (queue.State, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	log, err := run(fluxCmd(ctx, fluxURI(target), "job", "eventlog", handle))
	if err != nil {
		return "", "", fmt.Errorf("flux job eventlog %s: %s", handle, err)
	}
	st, msg := parseEventlog(log)
	return st, msg, nil
}

func parseEventlog(log string) (queue.State, string) {
	running := false
	for _, line := range strings.Split(log, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		switch f[1] { // f[0] is the timestamp
		case "alloc", "start":
			running = true
		case "exception":
			// severity-0 exceptions are fatal (cancel, dependency failure, …)
			return queue.Failed, "flux exception: " + strings.Join(f[2:], " ")
		case "finish":
			status := 0
			for _, kv := range f[2:] {
				if strings.HasPrefix(kv, "status=") {
					status, _ = strconv.Atoi(strings.TrimPrefix(kv, "status="))
				}
			}
			if status == 0 {
				return queue.Completed, "finished (exit 0)"
			}
			if code := status >> 8; code != 0 {
				return queue.Failed, fmt.Sprintf("exited with code %d", code)
			}
			return queue.Failed, fmt.Sprintf("terminated by signal %d", status&0x7f)
		}
	}
	if running {
		return queue.Running, "running on flux"
	}
	return queue.Running, "scheduled on flux (awaiting resources)"
}

func (d *FluxDriver) Cancel(target graph.ClusterGraph, handle string) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	if _, err := run(fluxCmd(ctx, fluxURI(target), "cancel", handle)); err != nil {
		// best-effort: an already-inactive job is not an error worth surfacing
		if strings.Contains(err.Error(), "inactive") || strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}
	return nil
}

func (d *FluxDriver) Logs(target graph.ClusterGraph, handle string) (string, error) {
	// attach returns buffered output; cap it so a still-running job can't block.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := run(fluxCmd(ctx, fluxURI(target), "job", "attach", handle))
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", nil // still running; no final log yet
		}
		// attach exits nonzero when the job itself failed; the output is still useful
		if out != "" {
			return out, nil
		}
		return "", err
	}
	return out, nil
}

// shquote single-quotes a string for safe embedding in a `sh -c` line.
func shquote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
