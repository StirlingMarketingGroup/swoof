swoof 1 "2026" swoof "User Commands"
====================================

## NAME

swoof - move MySQL tables between databases using transactional swaps

## SYNOPSIS

`swoof` [*flags*] *source* *dest* [*table*...]

`swoof init` [`--system`|`--user`] [`-y`] [`--shell` *list*] [`--no-cache`]

`swoof completion` *bash*|*zsh*|*fish*

`swoof man` [*section*]

## DESCRIPTION

**swoof** copies MySQL tables from a source database to one or more
destination databases. Each table is imported into a temporary table and then
atomically swapped into place with `RENAME TABLE`, so long imports do not take
the destination table offline and a failed import never leaves a half-written
table behind.

*source* is a MySQL DSN (`user:pass@(host)/dbname`) or a name defined in your
connections file.

*dest* is one or more comma-separated destinations, each of which may be:

  * a DSN or a connection name (a live MySQL destination);
  * `clipboard`, which copies the generated SQL to the system clipboard
    instead of writing to a database;
  * `file:`*path*, which writes the generated SQL to a file (a local backup).

Table arguments may be literal table names, glob-style patterns, or aliases
defined in your aliases file.

The connection and alias file formats are documented in **swoof**(5).

## OPTIONS

`-a` *file*
: Your aliases file (default `$XDG_CONFIG_HOME/swoof/aliases.yaml`).

`-c` *file*
: Your connections file (default `$XDG_CONFIG_HOME/swoof/connections.yaml`).

`-all`
: Grab all tables; specified tables are ignored.

`-diff`, `-d`
: List the tables, views, functions, and stored procedures that exist on the
source but are missing from the destination, then exit without making any
changes. Requires exactly one database destination.

`-missing`, `-m`
: Select only the objects missing from the destination, like `-all` but
filtered to what the destination lacks. Applies to tables, and — when the
corresponding `-funcs`/`-views`/`-procs` flags are given — to functions,
views, and procedures. Requires exactly one database destination.

`-n`
: Drop/create tables and triggers only, without importing data.

`-insert-ignore`
: Insert into the existing table without overwriting the existing rows.

`-funcs`
: Import all functions after tables.

`-views`
: Import all views after tables and functions.

`-procs`
: Import all stored procedures after tables, functions, and views.

`-t` *n*
: Max concurrent tables imported at the same time (default 4). Import
stability may vary wildly between servers as this increases.

`-r` *n*
: Max rows buffer size; this many rows are downloaded and kept ready for
importing (default 10000).

`-w` *clause*
: Optional WHERE clause used to filter rows from the source table, e.g.
`-w "ID > 1000"`.

`-p` *prefix*
: Prefix of the temp table used for initial creation before the swap and drop
(default `_swoof_`).

`-dry-run`
: Do not actually execute any queries that have an effect.

`-v`
: Write all queries to stdout.

`-no-progress`
: Disable the progress bars.

`-skip-count`
: Skip the per-table count query, which can be slow for very large tables.

`-refresh-completions`
: Refresh the cached table lists used for shell completion for every
source-capable named connection, then exit.

`-version`
: Print the version and exit.

`-h`, `-help`
: Show help and exit.

## COMMANDS

`init`
: Install the man pages and shell completions and pre-warm the completion
caches. Installs system-wide when run as root, otherwise offers to use
**sudo**(8) and falls back to a per-user install. Detects the installed shells
(bash, zsh, fish) and can be limited with `--shell`. `--no-cache` skips
cache pre-warming; `-y` assumes yes for prompts.

`completion` *shell*
: Print the completion script for *shell* (`bash`, `zsh`, or `fish`) to
standard output.

`man` [*section*]
: Print the troff man page for the given *section* (1 or 5; default 1) to
standard output.

## EXAMPLES

Copy specific tables from one DSN to another:

    swoof 'user:pass@(prod-host)/shop' 'user:pass@(localhost)/shop' products orders

Use named connections from your connections file:

    swoof production localhost products orders

Fan out to multiple destinations:

    swoof production localhost,staging products orders

Copy every table matching a glob pattern:

    swoof production localhost 'order_*'

Only sync recent rows with a WHERE clause:

    swoof -w "Created > '2025-01-01'" production localhost orders

Dump a table to a local SQL file instead of a database:

    swoof production file:backup.sql orders

Set up completions, man pages, and warm the caches:

    swoof init

## EXIT STATUS

`0`
: Success.

`1`
: An error occurred (bad flags, connection failure, failed import, etc.).

`130`
: Interrupted (SIGINT / Ctrl-C).

## ENVIRONMENT

`XDG_CONFIG_HOME`, `HOME`, `AppData`
: Determine where the connections and aliases files are read from, via Go's
`os.UserConfigDir()`.

`XDG_CACHE_HOME`, `HOME`
: Determine where the shell-completion table cache is stored, via Go's
`os.UserCacheDir()` (`swoof/tables/` beneath it).

## FILES

`$XDG_CONFIG_HOME/swoof/connections.yaml`
: Named connection definitions. See **swoof**(5).

`$XDG_CONFIG_HOME/swoof/aliases.yaml`
: Named sets of tables. See **swoof**(5).

`$XDG_CACHE_HOME/swoof/tables/`*conn*`.json`
: Cached table lists per connection, used for shell completion. Refreshed on
use (1h TTL) and by `swoof -refresh-completions`.

## SEE ALSO

**swoof**(5)

Project home and full documentation:
<https://github.com/StirlingMarketingGroup/swoof>

MySQL DSN format:
<https://github.com/go-sql-driver/mysql#dsn-data-source-name>

## REPORTING BUGS

Report bugs at <https://github.com/StirlingMarketingGroup/swoof/issues>.

## AUTHOR

Written by Stirling Marketing Group.
