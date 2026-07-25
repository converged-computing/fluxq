// Command fluxq is both the server and its client.
//
//	fluxq serve [--queue memory|sqlite|postgres] [--addr :8080] [--fleet DIR]
//	  Start the fleet. It comes up with ZERO clusters unless --fleet preloads a
//	  JGF directory. Register clusters and submit jobs against it via the API.
//
//	fluxq cluster register --name c1 --manager flux-operator --nodes 4:64 --caps lammps,efa
//	fluxq cluster list
//	fluxq cluster unregister c1
//	fluxq submit --name job1 --image img --command "lmp -i in.reaxff" --nodes 4 --tasks-per-node 64 --caps lammps,efa
//	fluxq jobs
//	fluxq job <id>
//	fluxq log <id>
//
// The client commands talk to a running server (--server, default
// http://localhost:8080).
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprint(os.Stderr, `fluxq — fleet-level dispatch queue

server:
  fluxq serve [--queue memory|sqlite|postgres] [--dsn DSN] [--addr :8080] [--fleet DIR]

clusters (edits need --secret / $FLUXQ_SECRET):
  fluxq managers                                                # supported managers + real/emulated dispatch
  fluxq cluster register --name N --manager M [--handle H]      # prints a secret
  fluxq cluster list
  fluxq cluster unregister --name N
  fluxq cluster subsystem register   --cluster C --file g.json [--name S] [--descriptive=false]
  fluxq cluster subsystem unregister --cluster C --name S

jobs (content is a Flux jobspec file):
  fluxq submit  --file job.json
  fluxq satisfy --file job.json       # dry-run: ranked feasible clusters, allocates nothing
  fluxq jobs
  fluxq job <id>
  fluxq log <id>

Client commands accept --server (default http://localhost:8080).
`)
}

func main() {
	// If invoked as a Fluxion worker subprocess, serve reapi and exit (fluxion build only).
	maybeFluxionWorker()
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "cluster":
		err = runCluster(os.Args[2:])
	case "managers":
		err = runManagers(os.Args[2:])
	case "submit":
		err = runSubmit(os.Args[2:])
	case "satisfy":
		err = runSatisfy(os.Args[2:])
	case "jobs":
		err = runJobs(os.Args[2:])
	case "job":
		err = runJob(os.Args[2:])
	case "log":
		err = runLog(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
