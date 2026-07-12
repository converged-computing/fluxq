package transform

// AgentTransformer is the LLM implementation of the Transformer seam: instead
// of deterministic templates (Stub), it asks a model to compile the agnostic
// jobspec into the target manager's native artifact. It drops into the pipeline
// with no downstream change — the dispatch stage calls Transform in its
// unlocked region, and the driver applies the result exactly as before.
//
// The credential boundary from the design still applies OUTSIDE this file: the
// model's output is untrusted text and must pass a deterministic validator +
// server-side dry-run before the credentialed apply. This type only generates;
// it never holds cluster credentials and never applies anything.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/converged-computing/fleetq/pkg/cluster"
	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
)

const (
	defaultEndpoint = "https://api.anthropic.com/v1/messages"
	defaultVersion  = "2023-06-01"
	defaultModel    = "claude-sonnet-4-5" // override via WithModel / $ANTHROPIC_MODEL
	defaultMaxTok   = 2048
)

// AgentTransformer calls the Anthropic Messages API to generate (and repair)
// native artifacts. Endpoint and HTTP client are injectable for testing.
type AgentTransformer struct {
	APIKey    string
	Model     string
	Endpoint  string
	Version   string
	MaxTokens int
	HTTP      *http.Client
}

// NewAgentTransformer reads the token from $ANTHROPIC_API_KEY (and optional
// $ANTHROPIC_MODEL). This is how a real Claude token plugs in: set the env var
// and swap m.Trans = NewAgentTransformer().
func NewAgentTransformer() *AgentTransformer {
	model := os.Getenv("ANTHROPIC_MODEL")
	if model == "" {
		model = defaultModel
	}
	return &AgentTransformer{
		APIKey:    os.Getenv("ANTHROPIC_API_KEY"),
		Model:     model,
		Endpoint:  defaultEndpoint,
		Version:   defaultVersion,
		MaxTokens: defaultMaxTok,
		HTTP:      &http.Client{Timeout: 60 * time.Second},
	}
}

// kindFor mirrors Stub: flux-uri is jobspec-native, everything else is a
// manifest. The driver dispatches on this Kind, so the agent must match it.
func kindFor(target graph.ClusterGraph) string {
	if target.Manager == graph.FluxURI {
		return "jobspec"
	}
	return "manifest"
}

// Transform asks the model to compile the jobspec into the target's artifact.
func (a *AgentTransformer) Transform(js jobspec.Jobspec, target graph.ClusterGraph) (cluster.Content, error) {
	kind := kindFor(target)
	system := fmt.Sprintf(
		"You are a transform agent for heterogeneous workload managers. "+
			"Output ONLY a single valid %s for the target manager — no prose, no "+
			"explanation, no markdown fences. Preserve the application's resource "+
			"needs exactly; never change the problem size.", kind)
	user := BuildPrompt(js, target)
	out, err := a.complete(context.Background(), system, user)
	if err != nil {
		return cluster.Content{}, err
	}
	payload := stripFences(out)
	if payload == "" {
		return cluster.Content{}, fmt.Errorf("agent returned empty %s", kind)
	}
	return cluster.Content{Kind: kind, Payload: payload}, nil
}

// Repair asks the model to fix an artifact that a validator/dry-run rejected.
// It carries the exact error and the failed artifact — the tight, grounded loop
// from the design. The manager's runRepair seam calls this, then re-validates.
func (a *AgentTransformer) Repair(js jobspec.Jobspec, target graph.ClusterGraph, failed, validationErr string) (cluster.Content, error) {
	kind := kindFor(target)
	system := fmt.Sprintf(
		"You are a repair agent. A previously generated %s was rejected. Fix it "+
			"so it validates on the target, changing ONLY what the error requires. "+
			"Do not alter the requested resources or problem size. Output ONLY the "+
			"corrected %s — no prose, no fences.", kind, kind)
	var b strings.Builder
	b.WriteString(BuildPrompt(js, target))
	fmt.Fprintf(&b, "\nThe following %s was REJECTED with this error:\n%s\n\n--- artifact ---\n%s\n", kind, validationErr, failed)
	out, err := a.complete(context.Background(), system, b.String())
	if err != nil {
		return cluster.Content{}, err
	}
	payload := stripFences(out)
	if payload == "" {
		return cluster.Content{}, fmt.Errorf("repair returned empty %s", kind)
	}
	return cluster.Content{Kind: kind, Payload: payload}, nil
}

// --- Anthropic Messages API ---

type apiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type apiRequest struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	System    string       `json:"system,omitempty"`
	Messages  []apiMessage `json:"messages"`
}
type apiBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type apiResponse struct {
	Content []apiBlock `json:"content"`
	Error   *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (a *AgentTransformer) complete(ctx context.Context, system, user string) (string, error) {
	if a.APIKey == "" {
		return "", fmt.Errorf("no Anthropic API key (set $ANTHROPIC_API_KEY)")
	}
	reqBody, err := json.Marshal(apiRequest{
		Model:     a.Model,
		MaxTokens: a.MaxTokens,
		System:    system,
		Messages:  []apiMessage{{Role: "user", Content: user}},
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", a.APIKey)
	req.Header.Set("anthropic-version", a.Version)

	client := a.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("anthropic request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("anthropic %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out apiResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("decode anthropic response: %w", err)
	}
	if out.Error != nil {
		return "", fmt.Errorf("anthropic error (%s): %s", out.Error.Type, out.Error.Message)
	}
	var text strings.Builder
	for _, blk := range out.Content {
		if blk.Type == "text" {
			text.WriteString(blk.Text)
		}
	}
	return text.String(), nil
}

// stripFences removes a leading ```lang / trailing ``` if the model wrapped its
// output despite instructions, and trims surrounding whitespace.
func stripFences(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	lines := strings.Split(s, "\n")
	if len(lines) < 2 {
		return s
	}
	lines = lines[1:] // drop opening ```lang
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
