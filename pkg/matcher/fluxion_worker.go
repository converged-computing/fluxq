//go:build fluxion

// Fluxion worker subprocess. Each reapi context lives in its OWN process so it
// gets its OWN string interner: flux-sched's interner is a process-global
// singleton that finalizes after the first InitContext, so a second in-process
// InitContext that introduces a new resource TYPE fails. One process == one
// context sidesteps that entirely, and makes "edit a subsystem" a clean
// kill-and-respawn (a fresh interner, so new types load).
//
// The worker is the fluxq binary re-executed with a sentinel arg; RunWorkerIfRequested
// is called at the very top of main() (and from the fluxion test TestMain). It
// speaks newline-delimited JSON on stdin/stdout: one "init" with the JGF, then
// "satisfy"/"allocate"/"cancel" requests, one response line each.
package matcher

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/flux-framework/flux-sched/resource/reapi/bindings/go/src/fluxcli"
)

// workerArg is the sentinel first argument that turns the fluxq binary into a
// reapi worker instead of the normal CLI.
const workerArg = "__fluxion_worker"

// wreq / wresp are the worker IPC messages (one JSON object per line).
type wreq struct {
	Op    string `json:"op"` // init | satisfy | allocate | cancel
	Graph string `json:"graph,omitempty"`
	Spec  string `json:"spec,omitempty"`
	Jobid uint64 `json:"jobid,omitempty"`
}

type wresp struct {
	Sat       bool   `json:"sat,omitempty"`
	Allocated string `json:"allocated,omitempty"`
	Jobid     uint64 `json:"jobid,omitempty"`
	Err       string `json:"err,omitempty"`
}

func errStr(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}

// RunWorkerIfRequested serves the reapi worker loop and exits the process if this
// invocation is a worker subprocess; otherwise it returns and normal execution
// continues. Call it first thing in main() and in the fluxion test TestMain.
func RunWorkerIfRequested() {
	if len(os.Args) < 2 || os.Args[1] != workerArg {
		return
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20) // graphs can be large
	out := json.NewEncoder(os.Stdout)
	var cli *fluxcli.ReapiClient
	for in.Scan() {
		var r wreq
		if err := json.Unmarshal(in.Bytes(), &r); err != nil {
			_ = out.Encode(wresp{Err: "bad request: " + err.Error()})
			continue
		}
		switch r.Op {
		case "init":
			cli = fluxcli.NewReapiClient()
			if err := cli.InitContext(r.Graph, fluxOpts); err != nil {
				// Report the real reapi message (GetErrMsg), not just the code.
				_ = out.Encode(wresp{Err: cli.GetErrMsg()})
				cli = nil
				continue
			}
			_ = out.Encode(wresp{})
		case "satisfy":
			if cli == nil {
				_ = out.Encode(wresp{Err: "no context"})
				continue
			}
			sat, _, err := cli.MatchSatisfy(r.Spec)
			_ = out.Encode(wresp{Sat: sat, Err: errStr(err)})
		case "allocate":
			if cli == nil {
				_ = out.Encode(wresp{Err: "no context"})
				continue
			}
			_, allocated, _, _, jobid, err := cli.MatchAllocate(false, r.Spec)
			_ = out.Encode(wresp{Allocated: allocated, Jobid: jobid, Err: errStr(err)})
		case "cancel":
			if cli == nil {
				_ = out.Encode(wresp{})
				continue
			}
			_ = out.Encode(wresp{Err: errStr(cli.Cancel(int64(r.Jobid), true))})
		default:
			_ = out.Encode(wresp{Err: "unknown op " + r.Op})
		}
	}
	os.Exit(0)
}
