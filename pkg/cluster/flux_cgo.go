//go:build fluxcore

package cluster

// FluxCGODriver dispatches to a Flux instance programmatically through libflux
// (flux-core), with no shelling out. It is compiled only under `-tags fluxcore`,
// mirroring the reapi matcher's `-tags fluxion`: the pure-Go default build keeps
// Flux emulated-only; this build links libflux and dispatches for real.
//
// Connection: config["uri"]. "local" (or unset) opens the ambient broker this
// server runs inside (flux_open(NULL)); an "ssh://host/..." URI is opened
// directly — flux_open speaks ssh natively, so there is no `flux proxy`.
//
// Submit takes the RFC-14/25 jobspec fluxq already renders (Content.Kind
// "jobspec") and hands it to flux_job_submit. Status reads the job's state via
// job-list, and on the terminal INACTIVE state reads the result + waitstatus.
//
// flux_rpc_get_unpack / flux_job_result_get_unpack are variadic and thus not
// callable from cgo, so they are wrapped below in fixed-signature C shims.

/*
#cgo pkg-config: flux-core jansson
#include <flux/core.h>
#include <jansson.h>
#include <stdlib.h>

static int fluxq_state(flux_future_t *f, int *state) {
    json_int_t s = 0;
    int rc = flux_rpc_get_unpack(f, "{s:{s:I}}", "job", "state", &s);
    if (rc == 0) *state = (int)s;
    return rc;
}

static int fluxq_result(flux_future_t *f, int *result, int *waitstatus, int *have_ws) {
    json_int_t r = 0, ws = 0;
    if (flux_job_result_get_unpack(f, "{s:I s:I}", "result", &r, "waitstatus", &ws) == 0) {
        *result = (int)r; *waitstatus = (int)ws; *have_ws = 1; return 0;
    }
    if (flux_job_result_get_unpack(f, "{s:I}", "result", &r) == 0) {
        *result = (int)r; *have_ws = 0; return 0;
    }
    return -1;
}
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/queue"
)

// flux_job_state_t values we branch on.
const (
	fluxStateRun      = 16
	fluxStateCleanup  = 32
	fluxStateInactive = 64
)

// flux_job_result_t values.
const (
	fluxResultCompleted = 1
	fluxResultFailed    = 2
	fluxResultCanceled  = 4
	fluxResultTimeout   = 8
)

type FluxCGODriver struct{ Timeout time.Duration }

func NewFluxCGODriver() *FluxCGODriver { return &FluxCGODriver{Timeout: 15 * time.Second} }

func (d *FluxCGODriver) Type() graph.ManagerType { return graph.FluxURI }

// open connects to the broker named by config["uri"] ("local"/unset = ambient).
func (d *FluxCGODriver) open(target graph.ClusterGraph) (*C.flux_t, error) {
	uri := target.Cfg("uri")
	var curi *C.char
	if uri != "" && uri != "local" {
		curi = C.CString(uri)
		defer C.free(unsafe.Pointer(curi))
	}
	h, err := C.flux_open(curi, 0)
	if h == nil {
		return nil, fmt.Errorf("flux_open(%q): %v", uri, err)
	}
	return h, nil
}

func (d *FluxCGODriver) Submit(target graph.ClusterGraph, c Content) (string, error) {
	if c.Kind != "jobspec" {
		return "", fmt.Errorf("flux expects a jobspec, got %q", c.Kind)
	}
	h, err := d.open(target)
	if err != nil {
		return "", err
	}
	defer C.flux_close(h)

	cspec := C.CString(c.Payload)
	defer C.free(unsafe.Pointer(cspec))

	f, err := C.flux_job_submit(h, cspec, 16, 0) // urgency 16 (default), flags 0
	if f == nil {
		return "", fmt.Errorf("flux_job_submit: %v", err)
	}
	defer C.flux_future_destroy(f)

	var id C.flux_jobid_t
	if rc, _ := C.flux_job_submit_get_id(f, &id); rc != 0 {
		return "", fmt.Errorf("submit rejected: %s", futErr(f))
	}
	return encodeJobid(id), nil
}

func (d *FluxCGODriver) Status(target graph.ClusterGraph, handle string) (queue.State, string, error) {
	id, err := decodeJobid(handle)
	if err != nil {
		return "", "", err
	}
	h, err := d.open(target)
	if err != nil {
		return "", "", err
	}
	defer C.flux_close(h)

	attrs := C.CString(`["state"]`)
	defer C.free(unsafe.Pointer(attrs))
	lf, lerr := C.flux_job_list_id(h, id, attrs)
	if lf == nil {
		return "", "", fmt.Errorf("flux_job_list_id: %v", lerr)
	}
	defer C.flux_future_destroy(lf)
	var state C.int
	if rc := C.fluxq_state(lf, &state); rc != 0 {
		return "", "", fmt.Errorf("read state: %s", futErr(lf))
	}

	switch {
	case int(state) < fluxStateRun:
		return queue.Running, "queued", nil
	case int(state) == fluxStateRun:
		return queue.Running, "running", nil
	case int(state) == fluxStateCleanup:
		return queue.Running, "cleanup", nil
	}

	// INACTIVE: read the result and (if it ran) the wait status.
	rf, _ := C.flux_job_result(h, id, 0)
	if rf == nil {
		return queue.Completed, "inactive", nil
	}
	defer C.flux_future_destroy(rf)
	var result, waitstatus, haveWs C.int
	if rc := C.fluxq_result(rf, &result, &waitstatus, &haveWs); rc != 0 {
		return queue.Completed, "inactive", nil
	}
	switch int(result) {
	case fluxResultCompleted:
		return queue.Completed, "finished (exit 0)", nil
	case fluxResultFailed:
		if haveWs != 0 {
			if code := exitCode(waitstatus); code != 0 {
				return queue.Failed, fmt.Sprintf("exited with code %d", code), nil
			}
		}
		return queue.Failed, "failed (did not start)", nil
	case fluxResultCanceled:
		return queue.Failed, "canceled", nil
	case fluxResultTimeout:
		return queue.Failed, "timeout", nil
	default:
		return queue.Completed, "inactive", nil
	}
}

func (d *FluxCGODriver) Cancel(target graph.ClusterGraph, handle string) error {
	id, err := decodeJobid(handle)
	if err != nil {
		return err
	}
	h, err := d.open(target)
	if err != nil {
		return err
	}
	defer C.flux_close(h)

	reason := C.CString("canceled by fluxq")
	defer C.free(unsafe.Pointer(reason))
	f, cerr := C.flux_job_cancel(h, id, reason)
	if f == nil {
		return fmt.Errorf("flux_job_cancel: %v", cerr)
	}
	defer C.flux_future_destroy(f)
	if rc, _ := C.flux_future_get(f, nil); rc != 0 {
		return fmt.Errorf("cancel failed: %s", futErr(f))
	}
	return nil
}

// Logs: reading a job's output eventlog over libflux is a follow-up; return
// empty rather than shell out. (The status/exit signal above is what the
// dispatch loop needs to complete or fail a job.)
func (d *FluxCGODriver) Logs(target graph.ClusterGraph, handle string) (string, error) {
	return "", nil
}

// --- helpers ---

func futErr(f *C.flux_future_t) string {
	if s := C.flux_future_error_string(f); s != nil {
		return C.GoString(s)
	}
	return "unknown flux error"
}

func encodeJobid(id C.flux_jobid_t) string {
	buf := make([]C.char, 64)
	typ := C.CString("f58")
	defer C.free(unsafe.Pointer(typ))
	if rc, _ := C.flux_job_id_encode(id, typ, &buf[0], C.size_t(len(buf))); rc == 0 {
		return C.GoString(&buf[0])
	}
	return fmt.Sprintf("%d", uint64(id))
}

func decodeJobid(handle string) (C.flux_jobid_t, error) {
	cs := C.CString(handle)
	defer C.free(unsafe.Pointer(cs))
	var id C.flux_jobid_t
	if rc, _ := C.flux_job_id_parse(cs, &id); rc != 0 {
		return 0, fmt.Errorf("not a flux jobid: %q", handle)
	}
	return id, nil
}

func exitCode(waitstatus C.int) int {
	var e C.flux_error_t
	return int(C.flux_job_waitstatus_to_exitcode(waitstatus, &e))
}
