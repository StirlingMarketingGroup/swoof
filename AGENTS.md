# Repository Guidelines

## Project Structure & Module Organization
The CLI entry point lives in `main.go`, with shared import helpers in `aliases.go`, `connections.go`, and `util.go`. Precompiled releases and packaging metadata land in `dist/`; you usually do not edit these by hand. Example configs (`aliases-example.yaml`, `connections-example.yaml`) document expected keys for connection aliases. Visual assets (`swoof.gif`, `local-backup.png`) support the README and stay untouched unless you refresh documentation.

## Build, Test, and Development Commands
Use `go build ./...` to verify the project compiles across all packages. `go run . [flags] [source] [destination]` is the quickest way to exercise the CLI during development. Keep `go test ./...` green—while the suite is currently thin, this command should succeed even when no tests are defined. For container workflows, `docker build -t swoof .` produces a runnable image; pair it with `docker run --rm swoof ...` to mirror README usage.

## Coding Style & Naming Conventions
Format every change with `gofmt` (tabs for indent, trailing newline required) or `go fmt ./...` before sending a review. Follow Go’s export rules: exported types and functions use PascalCase and carry doc comments starting with the identifier; internals stay lowerCamelCase or snake_case only when matching MySQL identifiers. Favor small, single-purpose files, keeping connection logic in the existing helper files rather than `main.go`.

## Testing Guidelines
Add `_test.go` files alongside the code they cover and prefer table-driven cases using Go’s standard `testing` package. Stub external databases with lightweight fixtures or Dockerized MySQL when integration coverage is needed; gate slow tests behind a `-run Integration` pattern. Always run `go test ./...` locally before opening a pull request and confirm it passes without network credentials.

## Commit & Pull Request Guidelines
Git history favors concise, present-tense summaries such as `fix collation issues on macos` or dependency bumps (`Bump package from x to y`). Keep subject lines under ~70 characters and omit trailing periods. Pull requests should link the motivating issue, describe the migration or query impact, and include CLI examples when behavior changes. Screenshots are only necessary when updating documentation assets.

## Packaging & Docs
Release artifacts (`.deb`/`.rpm`/`.apk` + archives) are built by GoReleaser from `.goreleaser.yaml`; validate with `goreleaser check` and dry-run with `goreleaser release --snapshot --clean`. Completion candidates come from the hidden `swoof __complete` command (see `complete.go`), which reads the connections/aliases files and the table cache (`cache.go`); the `completions/*` scripts are thin shell wrappers around it. The man pages `man/swoof.1` and `man/swoof.5` are generated from their `.md` sources and embedded into the binary via `go:embed` (`assets.go`); regenerate after editing a source with `go run github.com/cpuguy83/go-md2man/v2@latest -in man/swoof.5.md -out man/swoof.5` (and likewise for `swoof.1`). Both the packages and `swoof init` install the completions and man pages to the same standard paths — keep them in sync. Reserved subcommands (`init`, `completion`, `man`, `__complete`, `__refresh-tables`) are dispatched in `commands.go` before the flag parser.

## Configuration Tips
Ship new config defaults by updating the `*-example.yaml` files and documenting the change in `README.md`. Remind users that runtime configuration resolves via `os.UserConfigDir()`, so note platform-specific paths when relevant. Never commit real database credentials; store only sanitized fixtures.
