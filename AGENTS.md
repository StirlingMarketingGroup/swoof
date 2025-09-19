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

## Configuration Tips
Ship new config defaults by updating the `*-example.yaml` files and documenting the change in `README.md`. Remind users that runtime configuration resolves via `os.UserConfigDir()`, so note platform-specific paths when relevant. Never commit real database credentials; store only sanitized fixtures.
