package main

import (
	_ "embed"
	"os"
	"path/filepath"
)

//go:embed completions/swoof.bash
var completionBash []byte

//go:embed completions/swoof.zsh
var completionZsh []byte

//go:embed completions/swoof.fish
var completionFish []byte

//go:embed man/swoof.1
var manPage1 []byte

//go:embed man/swoof.5
var manPage5 []byte

func defaultConnectionsFile() string {
	return filepath.Join(confDir, "swoof", "connections.yaml")
}

func defaultAliasesFile() string {
	return filepath.Join(confDir, "swoof", "aliases.yaml")
}

func tableCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "swoof", "tables"), nil
}
