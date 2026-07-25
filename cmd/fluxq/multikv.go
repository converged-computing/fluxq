package main

import "strings"

// multiKV is a repeatable `--config key=value` flag collecting backend dispatch
// metadata. The core stays agnostic to the keys; backends interpret them.
type multiKV []string

func (m *multiKV) String() string { return strings.Join(*m, ",") }
func (m *multiKV) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// Map folds the collected key=value pairs into a map (nil if none).
func (m multiKV) Map() map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for _, kv := range m {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return out
}
