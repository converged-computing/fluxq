//go:build fluxcore

package cluster

import (
	"testing"
	"time"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/queue"
)

// These run against the ambient broker; invoke under `flux start`.
var fluxTarget = graph.ClusterGraph{ID: "hpc", Manager: graph.FluxURI, Config: map[string]string{"uri": "local"}}

func specFor(t *testing.T, name string, cmd []string) Content {
	t.Helper()
	js := jobspec.New(name, "", cmd, 1, 1, time.Minute, nil)
	spec, err := js.ToFluxSpec()
	if err != nil {
		t.Fatalf("ToFluxSpec: %v", err)
	}
	return Content{Kind: "jobspec", Payload: spec}
}

func pollTerminal(t *testing.T, d *FluxCGODriver, handle string) (queue.State, string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st, note, err := d.Status(fluxTarget, handle)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.Terminal() {
			return st, note
		}
		time.Sleep(400 * time.Millisecond)
	}
	t.Fatalf("job %s never reached a terminal state", handle)
	return "", ""
}

func TestFluxDispatchSuccess(t *testing.T) {
	d := NewFluxCGODriver()
	h, err := d.Submit(fluxTarget, specFor(t, "ok", []string{"hostname"}))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	t.Logf("submitted, handle=%s", h)
	st, note := pollTerminal(t, d, h)
	if st != queue.Completed {
		t.Fatalf("state=%v note=%q, want Completed", st, note)
	}
	t.Logf("terminal: %v (%s)", st, note)
}

func TestFluxDispatchFailure(t *testing.T) {
	d := NewFluxCGODriver()
	h, err := d.Submit(fluxTarget, specFor(t, "boom", []string{"sh", "-c", "exit 3"}))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st, note := pollTerminal(t, d, h)
	if st != queue.Failed {
		t.Fatalf("state=%v note=%q, want Failed", st, note)
	}
	if note != "exited with code 3" {
		t.Fatalf("note=%q, want \"exited with code 3\"", note)
	}
	t.Logf("terminal: %v (%s)", st, note)
}
