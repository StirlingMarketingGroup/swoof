package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v2"
)

// completionFlags is the full set of flags offered when the current word looks
// like a flag. Kept in sync with the flag definitions in main.go.
var completionFlags = []string{
	"-a", "-c", "-n", "-t", "-all", "-insert-ignore", "-dry-run", "-v",
	"-funcs", "-views", "-procs", "-no-progress", "-version", "-skip-count",
	"-r", "-w", "-p", "-refresh-completions", "-diff", "-d", "-missing", "-m",
	"-h", "-help",
}

// valueFlags are flags that consume the following token as their value, so it
// must not be counted as a positional argument during completion.
var valueFlags = map[string]bool{
	"-a": true, "-c": true, "-t": true, "-r": true, "-w": true, "-p": true,
}

// runComplete is the hidden `swoof __complete` entry point. It receives the
// words typed after the program name, with the final element being the word
// currently under the cursor (possibly empty), and prints newline-separated
// candidates. It must never print anything but candidates.
func runComplete(argv []string) {
	if len(argv) == 0 {
		return
	}
	cur := argv[len(argv)-1]
	words := argv[:len(argv)-1]

	connFile, aliasFile, positionals := parseCompletionContext(words)

	var out []string
	switch {
	case strings.HasPrefix(cur, "-"):
		out = completionFlags
	case len(positionals) == 0: // source
		out = connectionCandidates(connFile, true, false)
	case len(positionals) == 1: // dest (comma-separated list of destinations)
		prefix := ""
		if idx := strings.LastIndexByte(cur, ','); idx >= 0 {
			prefix = cur[:idx+1]
		}
		for _, name := range connectionCandidates(connFile, false, true) {
			out = append(out, prefix+name)
		}
		out = append(out, prefix+"clipboard")
		if prefix == "" {
			out = append(out, "file:")
		}
	default: // tables
		used := make(map[string]bool)
		for _, t := range positionals[2:] {
			used[t] = true
		}
		for _, name := range aliasNames(aliasFile) {
			if !used[name] {
				out = append(out, name)
			}
		}
		for _, t := range cachedTablesForCompletion(positionals[0], connFile) {
			if !used[t] {
				out = append(out, t)
			}
		}
	}

	for _, s := range out {
		fmt.Println(s)
	}
}

// parseCompletionContext walks the already-typed words, resolving the effective
// connections/aliases file paths (honoring -c/-a) and collecting positional
// arguments so the caller knows which argument is being completed.
func parseCompletionContext(words []string) (connFile, aliasFile string, positionals []string) {
	connFile = defaultConnectionsFile()
	aliasFile = defaultAliasesFile()

	for i := 0; i < len(words); i++ {
		w := words[i]
		if strings.HasPrefix(w, "-") {
			name, val, hasVal := strings.Cut(w, "=")
			if valueFlags[name] {
				if !hasVal && i+1 < len(words) {
					val = words[i+1]
					i++
				}
				switch name {
				case "-c":
					if val != "" {
						connFile = val
					}
				case "-a":
					if val != "" {
						aliasFile = val
					}
				}
			}
			continue
		}
		positionals = append(positionals, w)
	}
	return connFile, aliasFile, positionals
}

// connectionCandidates returns connection names valid for the requested role,
// honoring the source_only / dest_only guards from the connections file.
func connectionCandidates(connFile string, asSource, asDest bool) []string {
	conns, err := getConnections(connFile)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(conns))
	for name, c := range conns {
		if asSource && c.DestOnly {
			continue
		}
		if asDest && c.SourceOnly {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// aliasNames returns the keys of the aliases file, or nil if it is missing.
func aliasNames(aliasFile string) []string {
	b, err := os.ReadFile(aliasFile)
	if err != nil {
		return nil
	}
	var aliases map[string][]string
	if err := yaml.Unmarshal(b, &aliases); err != nil {
		return nil
	}
	names := make([]string, 0, len(aliases))
	for name := range aliases {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
