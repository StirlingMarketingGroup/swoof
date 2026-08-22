package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/term"
)

// dispatchSubcommand handles the reserved subcommands that must run before the
// normal flag parser and without the startup banner. It returns true if a
// subcommand was handled (and the caller should return immediately).
func dispatchSubcommand() bool {
	if len(os.Args) < 2 {
		return false
	}
	switch os.Args[1] {
	case "__complete":
		runComplete(os.Args[2:])
	case "__refresh-tables":
		runRefreshTables(os.Args[2:])
	case "init":
		runInit(os.Args[2:])
	case "completion":
		runCompletionCmd(os.Args[2:])
	case "man":
		runManCmd(os.Args[2:])
	default:
		return false
	}
	return true
}

// spawnBackgroundRefresh fires off a detached `swoof __refresh-tables` so a
// stale or unreachable cache is refreshed without blocking the current shell.
func spawnBackgroundRefresh(connName, connFile string) {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	cmd := exec.Command(exe, "__refresh-tables", connName, "-c", connFile)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	detach(cmd)
	if err := cmd.Start(); err == nil && cmd.Process != nil {
		_ = cmd.Process.Release()
	}
}

// runRefreshTables refreshes a single connection's cache. It is invoked
// detached as a background job, so it is silent.
func runRefreshTables(args []string) {
	connFile := defaultConnectionsFile()
	var name string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-c":
			if i+1 < len(args) {
				connFile = args[i+1]
				i++
			}
		default:
			if name == "" && !strings.HasPrefix(args[i], "-") {
				name = args[i]
			}
		}
	}
	if name == "" {
		return
	}
	conns, err := getConnections(connFile)
	if err != nil {
		return
	}
	c, ok := conns[name]
	if !ok || c.DestOnly {
		return
	}
	if tables, err := fetchSourceTables(c, refreshTimeout); err == nil {
		_ = writeTableCache(name, tables)
	}
}

func runCompletionCmd(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: swoof completion bash|zsh|fish")
		os.Exit(2)
	}
	switch args[0] {
	case "bash":
		os.Stdout.Write(completionBash)
	case "zsh":
		os.Stdout.Write(completionZsh)
	case "fish":
		os.Stdout.Write(completionFish)
	default:
		fmt.Fprintf(os.Stderr, "unknown shell %q (want bash, zsh, or fish)\n", args[0])
		os.Exit(2)
	}
}

func runManCmd(args []string) {
	section := "1"
	if len(args) > 0 {
		section = args[0]
	}
	switch section {
	case "1":
		os.Stdout.Write(manPage1)
	case "5":
		os.Stdout.Write(manPage5)
	default:
		fmt.Fprintf(os.Stderr, "unknown man section %q (want 1 or 5)\n", section)
		os.Exit(2)
	}
}

// --- swoof init ---------------------------------------------------------

type installTarget struct {
	dst     string
	content []byte
}

func runInit(args []string) {
	var forceSystem, forceUser, assumeYes, noCache bool
	var shellsArg string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--system" || a == "-system":
			forceSystem = true
		case a == "--user" || a == "-user":
			forceUser = true
		case a == "-y" || a == "--yes" || a == "-yes":
			assumeYes = true
		case a == "--no-cache" || a == "-no-cache":
			noCache = true
		case a == "--shell" || a == "-shell":
			if i+1 < len(args) {
				shellsArg = args[i+1]
				i++
			}
		case strings.HasPrefix(a, "--shell="):
			shellsArg = strings.TrimPrefix(a, "--shell=")
		default:
			fmt.Fprintf(os.Stderr, "swoof init: unknown argument %q\n", a)
			os.Exit(2)
		}
	}

	shells := detectShells(shellsArg)

	isRoot := os.Geteuid() == 0
	system := decideSystemScope(forceSystem, forceUser, assumeYes, isRoot)
	useSudo := system && !isRoot

	dataHome := xdgDataHome()
	targets := installTargets(system, dataHome, shells)

	fmt.Println("Installing man pages and completions" + scopeLabel(system) + "...")
	if useSudo {
		fmt.Println("(some files go under /usr; you may be prompted for your sudo password)")
	}

	installed := 0
	for _, t := range targets {
		if err := writeInstall(t, useSudo); err != nil {
			fmt.Fprintf(os.Stderr, "  failed to write %s: %v\n", t.dst, err)
			continue
		}
		fmt.Println("  " + t.dst)
		installed++
	}

	if system {
		refreshMandb(useSudo)
	} else {
		maybeWireZsh(shells, dataHome, assumeYes)
	}

	if !noCache {
		fmt.Println("Pre-warming completion caches...")
		refreshAllConnections(defaultConnectionsFile(), refreshTimeout)
	}

	fmt.Printf("\nDone. Installed %d file(s). Restart your shell (or open a new one) to pick up completions.\n", installed)
}

func detectShells(shellsArg string) []string {
	if shellsArg != "" {
		var out []string
		for s := range strings.SplitSeq(shellsArg, ",") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	var out []string
	for _, s := range []string{"bash", "zsh", "fish"} {
		if _, err := exec.LookPath(s); err == nil {
			out = append(out, s)
		}
	}
	return out
}

func decideSystemScope(forceSystem, forceUser, assumeYes, isRoot bool) bool {
	switch {
	case forceUser:
		return false
	case forceSystem:
		return true
	case isRoot:
		return true
	case assumeYes:
		return false // non-interactive without an explicit choice: stay per-user
	case term.IsTerminal(int(os.Stdin.Fd())):
		return promptYesNo("Install system-wide (needs sudo)? [Y/n] ", true)
	default:
		return false
	}
}

func installTargets(system bool, dataHome string, shells []string) []installTarget {
	var t []installTarget
	if system {
		t = append(t,
			installTarget{"/usr/share/man/man1/swoof.1", manPage1},
			installTarget{"/usr/share/man/man5/swoof.5", manPage5},
		)
		for _, s := range shells {
			switch s {
			case "bash":
				t = append(t, installTarget{"/usr/share/bash-completion/completions/swoof", completionBash})
			case "zsh":
				// site-functions (Fedora/Homebrew) and vendor-completions
				// (Debian/Ubuntu) — zsh reads whichever is in fpath.
				t = append(t,
					installTarget{"/usr/share/zsh/site-functions/_swoof", completionZsh},
					installTarget{"/usr/share/zsh/vendor-completions/_swoof", completionZsh},
				)
			case "fish":
				t = append(t, installTarget{"/usr/share/fish/vendor_completions.d/swoof.fish", completionFish})
			}
		}
		return t
	}

	t = append(t,
		installTarget{filepath.Join(dataHome, "man", "man1", "swoof.1"), manPage1},
		installTarget{filepath.Join(dataHome, "man", "man5", "swoof.5"), manPage5},
	)
	for _, s := range shells {
		switch s {
		case "bash":
			t = append(t, installTarget{filepath.Join(dataHome, "bash-completion", "completions", "swoof"), completionBash})
		case "zsh":
			t = append(t, installTarget{filepath.Join(dataHome, "zsh", "site-functions", "_swoof"), completionZsh})
		case "fish":
			t = append(t, installTarget{filepath.Join(confDir, "fish", "completions", "swoof.fish"), completionFish})
		}
	}
	return t
}

func writeInstall(t installTarget, useSudo bool) error {
	if useSudo {
		return sudoInstall(t.dst, t.content)
	}
	if err := os.MkdirAll(filepath.Dir(t.dst), 0o755); err != nil {
		return err
	}
	return os.WriteFile(t.dst, t.content, 0o644)
}

func sudoInstall(dst string, content []byte) error {
	tmp, err := os.CreateTemp("", "swoof-install-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()

	cmd := exec.Command("sudo", "install", "-D", "-m", "0644", tmp.Name(), dst)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd.Run()
}

func refreshMandb(useSudo bool) {
	if _, err := exec.LookPath("mandb"); err != nil {
		return
	}
	var cmd *exec.Cmd
	if useSudo {
		cmd = exec.Command("sudo", "mandb", "-q")
	} else {
		cmd = exec.Command("mandb", "-q")
	}
	_ = cmd.Run()
}

// maybeWireZsh prints (and optionally appends) the fpath lines a per-user zsh
// completion install needs, since there is no auto-loaded per-user fpath dir.
func maybeWireZsh(shells []string, dataHome string, assumeYes bool) {
	hasZsh := false
	for _, s := range shells {
		if s == "zsh" {
			hasZsh = true
		}
	}
	if !hasZsh {
		return
	}
	dir := filepath.Join(dataHome, "zsh", "site-functions")
	snippet := fmt.Sprintf("fpath+=(%q)\nautoload -Uz compinit && compinit\n", dir)

	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Println("\nAdd these lines to your ~/.zshrc to enable zsh completions:\n\n" + snippet)
		return
	}
	rc := filepath.Join(home, ".zshrc")

	if existing, err := os.ReadFile(rc); err == nil && strings.Contains(string(existing), dir) {
		return // already wired
	}

	fmt.Println("\nzsh needs these lines in your ~/.zshrc to load per-user completions:\n\n" + snippet)
	if !assumeYes && !promptYesNo("Append them to ~/.zshrc now? [y/N] ", false) {
		return
	}
	f, err := os.OpenFile(rc, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  could not update ~/.zshrc: %v\n", err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString("\n# added by `swoof init`\n" + snippet); err != nil {
		fmt.Fprintf(os.Stderr, "  could not update ~/.zshrc: %v\n", err)
		return
	}
	fmt.Println("  updated ~/.zshrc")
}

func xdgDataHome() string {
	if v := os.Getenv("XDG_DATA_HOME"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".local/share"
	}
	return filepath.Join(home, ".local", "share")
}

func scopeLabel(system bool) string {
	if system {
		return " system-wide"
	}
	return " for the current user"
}

func promptYesNo(prompt string, def bool) bool {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "":
		return def
	case "y", "yes":
		return true
	default:
		return false
	}
}
