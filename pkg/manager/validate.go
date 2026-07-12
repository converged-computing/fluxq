package manager

// The validator gate: the agent PROPOSES an artifact, this DISPOSES. Model
// output is untrusted text, so nothing reaches the credentialed driver.Submit
// until a deterministic check passes. This file is the offline half — parse,
// policy, and capability/target checks that need no cluster. The other half is
// a server-side `apply --dry-run=server`, which belongs in the k8s driver and
// slots in behind the same verdict (a Manager.validate override) once a live
// cluster is available.
//
// The verdict is what the pipeline routes on:
//   valid       -> submit
//   repairable  -> repair stage (fixable artifact: bad field, policy breach)
//   wrongTarget -> reschedule (this manager can't host this kind of artifact)

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/converged-computing/fleetq/pkg/cluster"
	"github.com/converged-computing/fleetq/pkg/graph"
)

type verdict int

const (
	verdictValid verdict = iota
	verdictRepairable
	verdictWrongTarget
)

// validateContent runs the configured validator (default: deterministic below).
func (m *Manager) validateContent(target graph.ClusterGraph, c cluster.Content) (verdict, string) {
	if m.validate != nil {
		return m.validate(target, c)
	}
	return defaultValidate(target, c)
}

func defaultValidate(target graph.ClusterGraph, c cluster.Content) (verdict, string) {
	payload := strings.TrimSpace(c.Payload)
	if payload == "" {
		return verdictRepairable, "empty artifact"
	}
	switch c.Kind {
	case "jobspec":
		var v any
		if err := json.Unmarshal([]byte(payload), &v); err != nil {
			return verdictRepairable, "jobspec is not valid JSON: " + err.Error()
		}
		return verdictValid, ""

	case "manifest":
		apiVersion, kind := scanManifestHeader(payload)
		if apiVersion == "" || kind == "" {
			return verdictRepairable, "manifest missing top-level apiVersion/kind"
		}
		if bad := policyScan(payload); bad != "" {
			return verdictRepairable, "policy violation: " + bad
		}
		// Capability check: does this manager host this kind of object? (Offline
		// proxy for CRD discovery / dry-run. A MiniCluster on a k8s-job-only
		// cluster is a wrong target, not a fixable artifact.)
		if want := expectedKind(target.Manager); want != "" && !strings.EqualFold(kind, want) {
			return verdictWrongTarget, fmt.Sprintf("cluster %q (%s) hosts %s, not %s",
				target.ID, target.Manager, want, kind)
		}
		return verdictValid, ""

	default:
		return verdictRepairable, "unknown content kind " + c.Kind
	}
}

// scanManifestHeader pulls top-level apiVersion/kind from a YAML document
// (indentation zero). Deliberately dependency-free and forgiving.
func scanManifestHeader(y string) (apiVersion, kind string) {
	for _, line := range strings.Split(y, "\n") {
		if len(line) == 0 || line[0] == ' ' || line[0] == '\t' || line[0] == '#' {
			continue // skip nested / blank / comment lines
		}
		if v, ok := topField(line, "apiVersion:"); ok {
			apiVersion = v
		}
		if v, ok := topField(line, "kind:"); ok {
			kind = v
		}
	}
	return apiVersion, kind
}

func topField(line, key string) (string, bool) {
	if !strings.HasPrefix(line, key) {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(line, key)), true
}

// policyScan rejects a small set of privilege-escalating fields. This is the
// blunt guard that stops an untrusted user hint ("mount the host fs") from
// becoming a privileged apply; refine per site policy.
func policyScan(y string) string {
	for _, bad := range []string{"hostPath", "hostNetwork: true", "hostPID: true", "hostIPC: true", "privileged: true"} {
		if strings.Contains(y, bad) {
			return bad
		}
	}
	return ""
}

func expectedKind(m graph.ManagerType) string {
	switch m {
	case graph.K8sJob:
		return "Job"
	case graph.FluxOperator:
		return "MiniCluster"
	case graph.SlurmOperator:
		return "SlurmJob"
	default:
		return "" // no capability constraint we can check offline
	}
}
