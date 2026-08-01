package main

import "testing"

func TestUnwrapJobspecAcceptsSelectorEnvelope(t *testing.T) {
	// fluxq-select writes {"jobspec": ..., "provenance": ...}
	got := string(unwrapJobspec([]byte(`{"jobspec":{"version":1},"provenance":{"application":"x"}}`)))
	if got != `{"version":1}` {
		t.Fatalf("envelope not unwrapped: %s", got)
	}
	// a bare jobspec passes through untouched
	bare := `{"version":1,"resources":[]}`
	if got := string(unwrapJobspec([]byte(bare))); got != bare {
		t.Fatalf("bare jobspec altered: %s", got)
	}
	// junk passes through so the server reports the real error
	if got := string(unwrapJobspec([]byte(`not json`))); got != "not json" {
		t.Fatalf("unexpected: %s", got)
	}
}
