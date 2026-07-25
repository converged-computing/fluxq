package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/converged-computing/fluxq/pkg/api"
	"github.com/converged-computing/fluxq/pkg/queue"
)

// --- tiny HTTP client ---

func httpDoAuth(method, url, secret string, body any) ([]byte, int, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("%s %s: %w (is `fluxq serve` running?)", method, url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return out, resp.StatusCode, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(out)))
	}
	return out, resp.StatusCode, nil
}

func httpDo(method, url string, body any) ([]byte, int, error) {
	return httpDoAuth(method, url, "", body)
}

func serverFlag(fs *flag.FlagSet) *string {
	return fs.String("server", "http://localhost:8080", "fluxq server URL")
}

// secretFlag resolves the per-cluster secret from --secret or $FLUXQ_SECRET.
func secretFlag(fs *flag.FlagSet) *string {
	return fs.String("secret", os.Getenv("FLUXQ_SECRET"), "cluster secret (or $FLUXQ_SECRET) for edits")
}

// idArg resolves --server and a job id given either positionally (`job job-1`)
// or via a flag (`job --id job-1` / `--id=job-1`).
func idArg(args []string) (server, id string) {
	server = "http://localhost:8080"
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--server" || a == "-server":
			if i+1 < len(args) {
				server = args[i+1]
				i++
			}
		case strings.HasPrefix(a, "--server="):
			server = strings.TrimPrefix(a, "--server=")
		case strings.HasPrefix(a, "-server="):
			server = strings.TrimPrefix(a, "-server=")
		case a == "--id" || a == "-id":
			if i+1 < len(args) {
				id = args[i+1]
				i++
			}
		case strings.HasPrefix(a, "--id="):
			id = strings.TrimPrefix(a, "--id=")
		default:
			if id == "" {
				id = a
			}
		}
	}
	return server, id
}

func serverAndArg(args []string) (server, arg string) {
	server = "http://localhost:8080"
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--server" || a == "-server":
			if i+1 < len(args) {
				server = args[i+1]
				i++
			}
		case strings.HasPrefix(a, "--server="):
			server = strings.TrimPrefix(a, "--server=")
		case strings.HasPrefix(a, "-server="):
			server = strings.TrimPrefix(a, "-server=")
		default:
			if arg == "" {
				arg = a
			}
		}
	}
	return server, arg
}

// --- cluster ---

func runCluster(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("cluster: want register|list|unregister|subsystem")
	}
	switch args[0] {
	case "register":
		return clusterRegister(args[1:])
	case "list", "ls":
		return clusterList(args[1:])
	case "unregister", "rm":
		return clusterUnregister(args[1:])
	case "subsystem", "sub":
		return clusterSubsystem(args[1:])
	default:
		return fmt.Errorf("cluster: unknown subcommand %q", args[0])
	}
}

func clusterRegister(args []string) error {
	fs := flag.NewFlagSet("cluster register", flag.ExitOnError)
	server := serverFlag(fs)
	name := fs.String("name", "", "cluster name (identity)")
	mgr := fs.String("manager", "", "manager: flux-operator|slurm-operator|k8s-job|flux-uri")
	handle := fs.String("handle", "", "native connection target (default: <manager>://<name>)")
	var conf multiKV
	fs.Var(&conf, "config", "backend dispatch metadata key=value (repeatable). flux: uri=local|ssh://host/run/flux/local ; k8s: context=NAME|kubeconfig=/path ; emulate=true simulates the backend (satisfy-only).")
	_ = fs.Parse(args)
	if *name == "" || *mgr == "" {
		return fmt.Errorf("usage: fluxq cluster register --name N --manager M [--handle H] [--config k=v ...]")
	}
	body := api.RegisterRequest{Name: *name, Manager: *mgr, Handle: *handle, Config: conf.Map()}
	out, _, err := httpDo("POST", *server+"/v1/clusters/register", body)
	if err != nil {
		return err
	}
	var resp api.RegisterResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		return err
	}
	fmt.Printf("registered cluster %q (handle %s)\n", resp.Name, resp.Handle)
	fmt.Printf("secret: %s\n", resp.Secret)
	fmt.Printf("save it for edits:  export FLUXQ_SECRET=%s\n", resp.Secret)
	fmt.Println("note: the cluster is empty. Add a containment subsystem (JGF) before it can schedule.")
	return nil
}

func clusterList(args []string) error {
	fs := flag.NewFlagSet("cluster list", flag.ExitOnError)
	server := serverFlag(fs)
	_ = fs.Parse(args)
	out, _, err := httpDo("GET", *server+"/v1/clusters", nil)
	if err != nil {
		return err
	}
	var infos []api.ClusterInfo
	if err := json.Unmarshal(out, &infos); err != nil {
		return err
	}
	if len(infos) == 0 {
		fmt.Println("(no clusters registered)")
		return nil
	}
	fmt.Printf("%-16s %-14s %-16s %-6s %s\n", "NAME", "MANAGER", "DISPATCH", "NODES", "SUBSYSTEMS")
	for _, c := range infos {
		subs := strings.Join(c.Subsystems, ",")
		if subs == "" {
			subs = "-"
		}
		fmt.Printf("%-16s %-14s %-16s %-6d %s\n", c.Name, c.Manager, c.Dispatch, c.Nodes, subs)
	}
	return nil
}

func clusterUnregister(args []string) error {
	fs := flag.NewFlagSet("cluster unregister", flag.ExitOnError)
	server := serverFlag(fs)
	secret := secretFlag(fs)
	name := fs.String("name", "", "cluster name")
	_ = fs.Parse(args)
	if *name == "" {
		return fmt.Errorf("usage: fluxq cluster unregister --name N --secret S")
	}
	if _, _, err := httpDoAuth("POST", *server+"/v1/clusters/"+*name+"/unregister", *secret, nil); err != nil {
		return err
	}
	fmt.Printf("unregistered cluster %q\n", *name)
	return nil
}

// --- cluster subsystem (JGF only) ---

func clusterSubsystem(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("cluster subsystem: want register|unregister")
	}
	switch args[0] {
	case "register", "add":
		return subsystemRegister(args[1:])
	case "unregister", "rm":
		return subsystemUnregister(args[1:])
	case "from-flux":
		return subsystemFromFlux(args[1:])
	default:
		return fmt.Errorf("cluster subsystem: unknown subcommand %q", args[0])
	}
}

func subsystemRegister(args []string) error {
	fs := flag.NewFlagSet("cluster subsystem register", flag.ExitOnError)
	server := serverFlag(fs)
	secret := secretFlag(fs)
	cluster := fs.String("cluster", "", "cluster to attach the subsystem to")
	name := fs.String("name", "", "subsystem name (default: from --file base name); e.g. containment, software")
	descriptive := fs.Bool("descriptive", true, "satisfy-only (true) vs countable/allocated (false, e.g. containment)")
	file := fs.String("file", "", "JGF file for the subsystem graph")
	_ = fs.Parse(args)
	if *cluster == "" || *file == "" {
		return fmt.Errorf("usage: fluxq cluster subsystem register --cluster C --file g.json [--name N] [--descriptive=false] --secret S")
	}
	sub := *name
	if sub == "" {
		sub = strings.TrimSuffix(baseName(*file), ".json")
	}
	raw, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	body := map[string]any{"descriptive": *descriptive, "graph": json.RawMessage(raw)}
	out, _, err := httpDoAuth("POST", *server+"/v1/clusters/"+*cluster+"/subsystems/"+sub, *secret, body)
	if err != nil {
		return err
	}
	fmt.Printf("registered subsystem: %s\n", strings.TrimSpace(string(out)))
	return nil
}

func subsystemUnregister(args []string) error {
	fs := flag.NewFlagSet("cluster subsystem unregister", flag.ExitOnError)
	server := serverFlag(fs)
	secret := secretFlag(fs)
	cluster := fs.String("cluster", "", "cluster")
	name := fs.String("name", "", "subsystem name")
	_ = fs.Parse(args)
	if *cluster == "" || *name == "" {
		return fmt.Errorf("usage: fluxq cluster subsystem unregister --cluster C --name N --secret S")
	}
	if _, _, err := httpDoAuth("DELETE", *server+"/v1/clusters/"+*cluster+"/subsystems/"+*name, *secret, nil); err != nil {
		return err
	}
	fmt.Printf("unregistered subsystem %s on %s\n", *name, *cluster)
	return nil
}

func baseName(p string) string {
	if i := strings.LastIndexAny(p, "/\\"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// --- jobs (jobspecs are files) ---

func jobFromFile(fs *flag.FlagSet, args []string) (string, json.RawMessage, error) {
	server := serverFlag(fs)
	file := fs.String("file", "", "Jobspec JSON file")
	_ = fs.Parse(args)
	if *file == "" {
		return *server, nil, fmt.Errorf("a --file jobspec.json is required")
	}
	raw, err := os.ReadFile(*file)
	return *server, json.RawMessage(raw), err
}

func runSubmit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	server, js, err := jobFromFile(fs, args)
	if err != nil {
		return err
	}
	out, _, err := httpDo("POST", server+"/v1/jobs/submit", js)
	if err != nil {
		return err
	}
	fmt.Println(strings.TrimSpace(string(out)))
	return nil
}

func runSatisfy(args []string) error {
	fs := flag.NewFlagSet("satisfy", flag.ExitOnError)
	server, js, err := jobFromFile(fs, args)
	if err != nil {
		return err
	}
	out, _, err := httpDo("POST", server+"/v1/jobs/satisfy", js)
	if err != nil {
		return err
	}
	var cands []struct {
		Cluster   string   `json:"cluster"`
		FreeNow   bool     `json:"free_now"`
		FreeNodes int      `json:"free_nodes"`
		Matched   []string `json:"matched"`
		Score     float64  `json:"score"`
	}
	if err := json.Unmarshal(out, &cands); err != nil {
		return err
	}
	if len(cands) == 0 {
		fmt.Println("(no feasible clusters)")
		return nil
	}
	fmt.Printf("%-16s %-7s %-8s %-6s %s\n", "CLUSTER", "SCORE", "FREE-NOW", "FREE", "MATCHED")
	for _, c := range cands {
		fmt.Printf("%-16s %-7.2f %-8v %-6d %s\n", c.Cluster, c.Score, c.FreeNow, c.FreeNodes, strings.Join(c.Matched, ","))
	}
	return nil
}

func runManagers(args []string) error {
	server, _ := serverAndArg(args)
	out, _, err := httpDo("GET", server+"/v1/managers", nil)
	if err != nil {
		return err
	}
	var infos []api.ManagerInfo
	if err := json.Unmarshal(out, &infos); err != nil {
		return err
	}
	yesno := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	fmt.Printf("%-16s %-14s %s\n", "MANAGER", "REAL DISPATCH", "EMULATED")
	for _, m := range infos {
		fmt.Printf("%-16s %-14s %s\n", m.Manager, yesno(m.Real), yesno(m.Emulated))
	}
	fmt.Println()
	fmt.Println("REAL DISPATCH=yes: register with backend --config (flux-uri: uri=… ; k8s-job: context=…|kubeconfig=…).")
	fmt.Println("REAL DISPATCH=no:  register with --config emulate=true (satisfy-only; never dispatched).")
	return nil
}

func runJobs(args []string) error {
	server, _ := serverAndArg(args)
	out, _, err := httpDo("GET", server+"/v1/jobs", nil)
	if err != nil {
		return err
	}
	var jobs []queue.Job
	if err := json.Unmarshal(out, &jobs); err != nil {
		return err
	}
	api.RenderTable(os.Stdout, jobs)
	return nil
}

func runJob(args []string) error {
	server, id := idArg(args)
	if id == "" {
		return fmt.Errorf("usage: fluxq job <id>")
	}
	out, _, err := httpDo("GET", server+"/v1/jobs/"+id, nil)
	if err != nil {
		return err
	}
	fmt.Println(strings.TrimSpace(string(out)))
	return nil
}

func runLog(args []string) error {
	server, id := idArg(args)
	if id == "" {
		return fmt.Errorf("usage: fluxq log <id>")
	}
	out, _, err := httpDo("GET", server+"/v1/jobs/"+id+"/log", nil)
	if err != nil {
		return err
	}
	fmt.Print(string(out))
	return nil
}
