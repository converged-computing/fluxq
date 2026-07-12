package transform

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
)

func testJob() jobspec.Jobspec {
	return jobspec.New("lammps-run", "ghcr.io/lammps:latest", []string{"lmp", "-i", "in.reaxff"},
		2, 4, 0, map[string][]jobspec.Resource{"software": {{Type: "lammps"}}})
}

// mockClaude returns a server that replies with the given text in one text
// block, and records the last request it saw.
func mockClaude(t *testing.T, reply string, seen *apiRequest) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") == "" || r.Header.Get("anthropic-version") == "" {
			t.Errorf("missing auth/version headers: %v", r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		if seen != nil {
			_ = json.Unmarshal(body, seen)
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(apiResponse{Content: []apiBlock{{Type: "text", Text: reply}}})
	}))
}

func newTestAgent(endpoint string) *AgentTransformer {
	return &AgentTransformer{APIKey: "test-key", Model: "test-model", Endpoint: endpoint,
		Version: defaultVersion, MaxTokens: 512, HTTP: http.DefaultClient}
}

func TestAgentTransformParsesManifest(t *testing.T) {
	manifest := "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: lammps-run\n"
	// Model wraps it in a fence despite instructions — we must strip it.
	var seen apiRequest
	srv := mockClaude(t, "```yaml\n"+manifest+"```", &seen)
	defer srv.Close()

	c, err := newTestAgent(srv.URL).Transform(testJob(), graph.ClusterGraph{ID: "c1", Manager: graph.K8sJob})
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if c.Kind != "manifest" {
		t.Fatalf("kind = %q, want manifest", c.Kind)
	}
	if strings.Contains(c.Payload, "```") || strings.TrimSpace(c.Payload) != strings.TrimSpace(manifest) {
		t.Fatalf("payload not cleanly parsed:\n%q", c.Payload)
	}
	// The request actually carried the model + a user message built from BuildPrompt.
	if seen.Model != "test-model" || len(seen.Messages) != 1 || !strings.Contains(seen.Messages[0].Content, "k8s-job") {
		t.Fatalf("request not well-formed: %+v", seen)
	}
}

func TestAgentFluxURIEmitsJobspecKind(t *testing.T) {
	srv := mockClaude(t, `{"tasks":[{"command":["lmp"]}]}`, nil)
	defer srv.Close()
	c, err := newTestAgent(srv.URL).Transform(testJob(), graph.ClusterGraph{ID: "f1", Manager: graph.FluxURI})
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if c.Kind != "jobspec" {
		t.Fatalf("flux-uri kind = %q, want jobspec", c.Kind)
	}
}

func TestAgentRepairCarriesError(t *testing.T) {
	var seen apiRequest
	srv := mockClaude(t, "apiVersion: batch/v1\nkind: Job\n", &seen)
	defer srv.Close()
	_, err := newTestAgent(srv.URL).Repair(testJob(),
		graph.ClusterGraph{ID: "c1", Manager: graph.K8sJob},
		"kind: Jobb\n", `unknown field "Jobb"`)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if !strings.Contains(seen.Messages[0].Content, "unknown field") || !strings.Contains(seen.Messages[0].Content, "REJECTED") {
		t.Fatalf("repair prompt missing error/artifact: %q", seen.Messages[0].Content)
	}
}

func TestAgentSurfacesAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"type":"authentication_error","message":"invalid x-api-key"}}`))
	}))
	defer srv.Close()
	_, err := newTestAgent(srv.URL).Transform(testJob(), graph.ClusterGraph{ID: "c1", Manager: graph.K8sJob})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want 401 error surfaced, got %v", err)
	}
}

func TestAgentRequiresKey(t *testing.T) {
	a := &AgentTransformer{Model: "m", Endpoint: "http://unused", Version: defaultVersion}
	if _, err := a.Transform(testJob(), graph.ClusterGraph{Manager: graph.K8sJob}); err == nil {
		t.Fatal("want error when API key is empty")
	}
}
