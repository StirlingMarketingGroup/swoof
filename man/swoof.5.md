swoof 5 "2026" swoof "File Formats"
===================================

## NAME

connections.yaml, aliases.yaml - configuration files for swoof

## DESCRIPTION

**swoof**(1) reads two optional YAML configuration files. Both live in the
per-user config directory resolved by Go's `os.UserConfigDir()`:
`$XDG_CONFIG_HOME/swoof` (or `$HOME/.config/swoof`) on Linux,
`$HOME/Library/Application Support/swoof` on macOS, and `%AppData%\swoof` on
Windows. Their locations can be overridden with the `-c` and `-a` flags
respectively.

## CONNECTIONS FILE

`connections.yaml` maps a short name to the details of a MySQL connection, so
commands can refer to `production` instead of a full DSN. Each entry supports
the following keys:

`user`
: MySQL user name.

`pass`
: MySQL password.

`host`
: Host, optionally with a port, e.g. `db.example.com:3307`.

`schema`
: Default database (schema) to use.

`params`
: A map of extra DSN parameters, passed through to the MySQL driver (for
example `maxAllowedPacket`).

`source_only`
: If true, the connection may only be used as a *source*. swoof refuses to use
it as a destination, guarding against accidental writes to, e.g., production.

`dest_only`
: If true, the connection may only be used as a *destination*.

Example:

    production:
      user: root
      pass: super secret password
      host: my-live.db:3307
      schema: cooldb
      source_only: true
    localhost:
      user: root
      pass: correct horse battery staple
      host: 127.0.0.1
      schema: cooldb
      dest_only: true
      params:
        maxAllowedPacket: 1048576

## ALIASES FILE

`aliases.yaml` maps a short name to a list of tables, so a group of tables can
be referred to by one name. An alias entry may reference other aliases, which
are resolved recursively until only table names remain.

When a table argument is given, swoof first checks whether it matches an alias
key; if not, it is treated as a table name. Alias names must therefore not
collide with real table names.

Example:

    Products:
      - products
      - productlines
    Orders:
      - orders
      - orderdetails
      - customers
      - payments
    All:
      - Products
      - Orders

## SEE ALSO

**swoof**(1)

MySQL DSN format:
<https://github.com/go-sql-driver/mysql#dsn-data-source-name>
