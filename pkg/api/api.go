// Package api exposes the queue manager over HTTP under /v1 with explicit,
// named actions (no ambiguous verbs on collections). Registration and
// subsystem-attach are separate concerns: registering a cluster creates an
// EMPTY named cluster and returns a per-cluster secret; resources arrive only
// as JGF subsystems (containment included). Everything ingested is JGF or a
// jobspec — the server never synthesizes graphs from ad-hoc fields.
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
	"github.com/converged-computing/fleetq/pkg/manager"
	"github.com/converged-computing/fleetq/pkg/queue"
)

// JobAuthenticator gates submit/assess. Default: allow all.
type JobAuthenticator interface {
	AuthenticateJob(r *http.Request, js jobspec.Jobspec) error
}

// ClusterAuthenticator gates cluster edits (add/remove subsystem, unregister).
// Default: the register-issued per-cluster secret, presented as a bearer token.
type ClusterAuthenticator interface {
	AuthenticateClusterEdit(r *http.Request, cluster string) error
}

type Server struct {
	M           *manager.Manager
	JobAuth     JobAuthenticator
	ClusterAuth ClusterAuthenticator
}

// NewServer wires the default auth: allow-all jobs, secret-gated cluster edits.
func NewServer(m *manager.Manager) *Server {
	return &Server{M: m, JobAuth: allowAllJobs{}, ClusterAuth: newSecretStore()}
}

func (s *Server) Routes() *http.ServeMux {
	if s.JobAuth == nil {
		s.JobAuth = allowAllJobs{}
	}
	if s.ClusterAuth == nil {
		s.ClusterAuth = newSecretStore()
	}
	mux := http.NewServeMux()
	// clusters
	mux.HandleFunc("POST /v1/clusters/register", s.registerCluster)
	mux.HandleFunc("POST /v1/clusters/{name}/unregister", s.unregisterCluster)
	mux.HandleFunc("GET /v1/clusters", s.clusters)
	mux.HandleFunc("GET /v1/managers", s.managers)
	mux.HandleFunc("GET /v1/clusters/{name}", s.cluster)
	// subsystems (POST/DELETE on the typed subresource = add/remove)
	mux.HandleFunc("POST /v1/clusters/{name}/subsystems/{sub}", s.registerSubsystem)
	mux.HandleFunc("DELETE /v1/clusters/{name}/subsystems/{sub}", s.unregisterSubsystem)
	// jobs
	mux.HandleFunc("POST /v1/jobs/submit", s.submit)
	mux.HandleFunc("POST /v1/jobs/satisfy", s.satisfy)
	mux.HandleFunc("GET /v1/jobs", s.jobs)
	mux.HandleFunc("GET /v1/jobs/{id}", s.job)
	mux.HandleFunc("GET /v1/jobs/{id}/log", s.log)
	return mux
}

// ---- cluster registration (creates an empty cluster, returns a secret) ----

type RegisterRequest struct {
	Name    string `json:"name"`
	Manager string `json:"manager"`
	Handle  string `json:"handle,omitempty"`
	// Config is backend-interpreted dispatch metadata (agnostic to the core).
	// Providing it opts a cluster into REAL dispatch; omitting it emulates.
	// Flux: {"uri":"local"} or {"uri":"ssh://host/run/flux/local"}.
	// Kubernetes (future): {"kubeconfig":"/path/to/kubeconfig"}.
	Config map[string]string `json:"config,omitempty"`
}

type RegisterResponse struct {
	Name   string `json:"name"`
	Handle string `json:"handle"`
	Secret string `json:"secret"`
}

func (s *Server) registerCluster(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "cluster name is required", http.StatusBadRequest)
		return
	}
	// handle is the driver's native connection target. It defaults to the
	// human-readable manager://name, but stays opaque and free-form — a real
	// driver may need richer coordinates (namespace, endpoint, kube-context,
	// flux URI), so the caller can override it. manager remains its own
	// authoritative field; we never parse it back out of the handle.
	handle := req.Handle
	if handle == "" {
		if req.Manager != "" {
			handle = req.Manager + "://" + req.Name
		} else {
			handle = req.Name
		}
	}
	cg := graph.ClusterGraph{
		ID: req.Name, Manager: graph.ManagerType(req.Manager), Handle: handle,
		Config:     req.Config,
		Subsystems: map[string]*graph.JGF{}, Descriptive: map[string]bool{},
	}
	if err := s.M.RegisterCluster(cg); err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, manager.ErrNotImplemented) {
			code = http.StatusNotImplemented
		}
		http.Error(w, err.Error(), code)
		return
	}
	secret := ""
	if ss, ok := s.ClusterAuth.(*secretStore); ok {
		secret = ss.issue(req.Name)
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, RegisterResponse{Name: req.Name, Handle: handle, Secret: secret})
}

func (s *Server) unregisterCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.ClusterAuth.AuthenticateClusterEdit(r, name); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if !s.M.UnregisterCluster(name) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if ss, ok := s.ClusterAuth.(*secretStore); ok {
		ss.forget(name)
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- reads ----

type ClusterInfo struct {
	Name       string   `json:"name"`
	Manager    string   `json:"manager"`
	Dispatch   string   `json:"dispatch"` // emulated | real | not-implemented
	Handle     string   `json:"handle,omitempty"`
	Nodes      int      `json:"nodes"`
	Subsystems []string `json:"subsystems,omitempty"`
	// Capability property keys on the cluster (software + network, e.g. lammps,
	// efa) — what a selector matches a container against. Matched by requires.
	Capabilities []string `json:"capabilities,omitempty"`
}

func infoOf(cg graph.ClusterGraph) ClusterInfo {
	var subs []string
	for name := range cg.Subsystems {
		subs = append(subs, name)
	}
	sort.Strings(subs)
	nodes := 0
	if g := cg.Containment(); g != nil {
		nodes = len(g.VerticesOfTypeExported("node"))
	}
	var caps []string
	for k := range cg.Capabilities() {
		caps = append(caps, k)
	}
	sort.Strings(caps)
	return ClusterInfo{Name: cg.ID, Manager: string(cg.Manager), Handle: cg.Handle, Nodes: nodes, Subsystems: subs, Capabilities: caps}
}

// ManagerInfo reports a manager type and whether this server can dispatch to it
// for real (a backend driver is wired in) and/or emulate it.
type ManagerInfo struct {
	Manager  string `json:"manager"`
	Real     bool   `json:"real"`
	Emulated bool   `json:"emulated"`
}

func (s *Server) managers(w http.ResponseWriter, r *http.Request) {
	out := []ManagerInfo{}
	for _, ms := range s.M.SupportedManagers() {
		out = append(out, ManagerInfo{Manager: string(ms.Manager), Real: ms.Real, Emulated: ms.Emulated})
	}
	writeJSON(w, out)
}

func (s *Server) clusters(w http.ResponseWriter, r *http.Request) {
	out := []ClusterInfo{}
	for _, cg := range s.M.Clusters() {
		ci := infoOf(cg)
		ci.Dispatch = s.M.DispatchMode(cg)
		out = append(out, ci)
	}
	writeJSON(w, out)
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	cg, ok := s.M.Fleet.Get(r.PathValue("name"))
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	ci := infoOf(cg)
	ci.Dispatch = s.M.DispatchMode(cg)
	writeJSON(w, ci)
}

// ---- subsystems (JGF only) ----

type SubsystemSpec struct {
	Descriptive bool       `json:"descriptive"`
	Graph       *graph.JGF `json:"graph"`
}

func (s *Server) registerSubsystem(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.ClusterAuth.AuthenticateClusterEdit(r, name); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var spec SubsystemSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if spec.Graph == nil {
		http.Error(w, "subsystem graph (JGF) is required", http.StatusBadRequest)
		return
	}
	sub := r.PathValue("sub")
	if err := s.M.RegisterSubsystem(name, sub, spec.Graph, spec.Descriptive); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, map[string]any{"cluster": name, "subsystem": sub, "descriptive": spec.Descriptive})
}

func (s *Server) unregisterSubsystem(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.ClusterAuth.AuthenticateClusterEdit(r, name); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := s.M.UnregisterSubsystem(name, r.PathValue("sub")); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- jobs ----

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	js, err := decodeJob(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.JobAuth.AuthenticateJob(r, js); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	id, err := s.M.Submit(js)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"id": id})
}

func (s *Server) satisfy(w http.ResponseWriter, r *http.Request) {
	js, err := decodeJob(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.JobAuth.AuthenticateJob(r, js); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	writeJSON(w, s.M.Satisfy(js))
}

func decodeJob(r *http.Request) (jobspec.Jobspec, error) {
	var js jobspec.Jobspec
	err := json.NewDecoder(r.Body).Decode(&js)
	return js, err
}

func (s *Server) job(w http.ResponseWriter, r *http.Request) {
	j, ok := s.M.Queue.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, j)
}

func (s *Server) jobs(w http.ResponseWriter, r *http.Request) { writeJSON(w, s.M.Queue.All()) }

func (s *Server) log(w http.ResponseWriter, r *http.Request) {
	out, err := s.M.Logs(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(out))
}

// ---- default auth ----

type allowAllJobs struct{}

func (allowAllJobs) AuthenticateJob(*http.Request, jobspec.Jobspec) error { return nil }

// secretStore is the default ClusterAuthenticator: register mints a per-cluster
// secret; edits must present it as `Authorization: Bearer <secret>`. In-memory
// only (not persisted across restarts) — a real deployment plugs in its own
// ClusterAuthenticator.
type secretStore struct {
	mu sync.Mutex
	m  map[string]string
}

func newSecretStore() *secretStore { return &secretStore{m: map[string]string{}} }

func (s *secretStore) issue(cluster string) string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	secret := hex.EncodeToString(b)
	s.mu.Lock()
	s.m[cluster] = secret
	s.mu.Unlock()
	return secret
}

func (s *secretStore) forget(cluster string) {
	s.mu.Lock()
	delete(s.m, cluster)
	s.mu.Unlock()
}

func (s *secretStore) AuthenticateClusterEdit(r *http.Request, cluster string) error {
	s.mu.Lock()
	want, ok := s.m[cluster]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown cluster %q", cluster)
	}
	if bearer(r) != want {
		return fmt.Errorf("invalid or missing cluster secret")
	}
	return nil
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(h[len("Bearer "):])
	}
	return ""
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// RenderTable prints a flux-jobs-like table (handy from the demo / CLI).
func RenderTable(w interface {
	Write([]byte) (int, error)
}, jobs []queue.Job) {
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "JOBID\tNAME\tSTATE\tCLUSTER\tHANDLE\tNOTE")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			j.ID, j.Spec.Name(), j.State, dash(j.ClusterID), dash(j.RemoteHandle), dash(j.Note))
	}
	_ = tw.Flush()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
