// Command fluxq-benchfmt turns `go test -bench` output into a human-readable
// comparison table. It reads benchmark lines from stdin (or a file arg), groups
// them by matcher (Fluxion vs Simulated) and fleet size, and prints one table
// per fleet size with clearly-named metrics as rows and a side-by-side column
// for each matcher plus a comparison.
//
//	go test -tags fluxion -run x -bench . -benchmem ./pkg/matcher/ | go run ./cmd/fluxq-benchfmt
package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

// e.g. "BenchmarkFluxionEvaluate/clusters=100-16   34   31854448 ns/op   44540 B/op   631 allocs/op"
// The trailing "-16" (GOMAXPROCS) is present on most machines but not all, so it's optional.
var line = regexp.MustCompile(
	`Benchmark(\w+?)Evaluate/clusters=(\d+)(?:-\d+)?\s+\d+\s+([\d.]+)\s+ns/op\s+([\d.]+)\s+B/op\s+([\d.]+)\s+allocs/op`)

type sample struct {
	ns, bytes, allocs float64
	present           bool
}

func main() {
	in := os.Stdin
	if len(os.Args) > 1 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		in = f
	}

	// data[fleetSize][matcher] = sample
	data := map[int]map[string]sample{}
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		m := line.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		matcher := m[1]
		if matcher == "Sim" {
			matcher = "Simulated"
		}
		n := atoi(m[2])
		if data[n] == nil {
			data[n] = map[string]sample{}
		}
		data[n][matcher] = sample{ns: atof(m[3]), bytes: atof(m[4]), allocs: atof(m[5]), present: true}
	}
	if len(data) == 0 {
		fmt.Fprintln(os.Stderr, "no benchmark lines found on input")
		os.Exit(1)
	}

	sizes := make([]int, 0, len(data))
	for n := range data {
		sizes = append(sizes, n)
	}
	sort.Ints(sizes)

	fmt.Println()
	fmt.Println("Fleet match benchmark — cost of matching ONE job against a whole fleet")
	fmt.Println("of N clusters (one `satisfy` pass). Lower is better on every metric.")
	fmt.Println("Fluxion = real flux-sched graph matcher; Simulated = the pure-Go dev double.")

	for _, n := range sizes {
		flux := data[n]["Fluxion"]
		sim := data[n]["Simulated"]
		fmt.Printf("\nFleet of %d clusters\n", n)
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(w, "  Metric\tFluxion\tSimulated\tComparison")
		fmt.Fprintln(w, "  ------\t-------\t---------\t----------")
		row(w, "Time per fleet match", dur(flux.ns), dur(sim.ns), cmp(sim.ns, flux.ns, "slower"), flux.present, sim.present)
		row(w, "Memory allocated per match", bytesH(flux.bytes), bytesH(sim.bytes), cmp(sim.bytes, flux.bytes, "more"), flux.present, sim.present)
		row(w, "Heap allocations per match", commas(flux.allocs), commas(sim.allocs), cmp(sim.allocs, flux.allocs, "more"), flux.present, sim.present)
		w.Flush()
	}
	fmt.Println()
	fmt.Println("Why: Fluxion builds each cluster graph once at load and only traverses on")
	fmt.Println("each match; the Simulated matcher re-indexes every graph on every match, so")
	fmt.Println("its memory and allocations climb with fleet size.")
	fmt.Println()
}

func row(w *tabwriter.Writer, metric, a, b, comparison string, aOK, bOK bool) {
	if !aOK {
		a = "—"
	}
	if !bOK {
		b = "—"
	}
	if !aOK || !bOK {
		comparison = ""
	}
	fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", metric, a, b, comparison)
}

// cmp renders "Simulated N× {word}" comparing sim to flux (lower is better).
func cmp(sim, flux float64, word string) string {
	if flux <= 0 || sim <= 0 {
		return ""
	}
	if sim >= flux {
		return fmt.Sprintf("Simulated %s %s", ratio(sim/flux), word)
	}
	// unexpected here, but stay honest if Fluxion is ever worse
	inv := map[string]string{"slower": "slower", "more": "more"}[word]
	return fmt.Sprintf("Fluxion %s %s", ratio(flux/sim), inv)
}

func ratio(r float64) string {
	switch {
	case r >= 100:
		return fmt.Sprintf("%.0f×", r)
	case r >= 10:
		return fmt.Sprintf("%.0f×", r)
	default:
		return fmt.Sprintf("%.1f×", r)
	}
}

func dur(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2f s", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2f ms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.1f µs", ns/1e3)
	default:
		return fmt.Sprintf("%.0f ns", ns)
	}
}

func bytesH(b float64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

func commas(f float64) string {
	s := strconv.FormatInt(int64(f), 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

func atoi(s string) int     { n, _ := strconv.Atoi(s); return n }
func atof(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }
