# Swoof

Just swoof it in. Ultra fast MySQL table importer. But not too fast.

![Swoof](swoof.gif)

This still uses MySQL commands to do the importing, so it wont be as quick as, say, the Percona xtra-backup tool, but because it uses queries, this will work across a broad range of MySQL versions.

---

## Dependencies

You will need Golang, which you can get from here https://golang.org/doc/install.

## Installing

```shell
cd ~ # or wherever you store your git projects
git clone https://github.com/StirlingMarketingGroup/swoof.git
go install
```

If you already have `$GOPATH/bin` in your path, this should be all you need! If you don't though, you can add this to to your `~/.bashrc` or `~/.zshrc` or whatever file your shell runs on login.

```shell
export GOPATH=$HOME/go # if you don't already have a GOPATH set
export PATH=$PATH:$GOPATH/bin
```

## Usage

```shell
swoof [-n -r 50 -t 4] 'user:pass@(host)/dbname' 'user:pass@(host)/dbname' table1 table2 table3
```
### Flags:

  - `-n`    drop/create tables and triggers only, without importing data
  - `-r` value
        max rows buffer size. Will have this many rows downloaded and ready for importing, or in Go terms, the channel size used to communicate the rows (default 50)
  - `-t` value
        max concurrent tables at the same time. Anything more than 4 seems to crash things, so YMMV (default 4)

### DSN (Data Source Name)

Source and destination strings are DSNs. The Data Source Name has a common format, like e.g. [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php) uses it, but without type-prefix (optional parts marked by squared brackets):
```
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
```

A DSN in its fullest form:
```
username:password@protocol(address)/dbname?param=value
```

Except for the databasename, all values are optional. So the minimal DSN is:
```
/dbname
```

If you do not want to preselect a database, leave `dbname` empty:
```
/
```
This has the same effect as an empty DSN string:
```
```

Using the DNS you can also tweak the packet size that's used. If you're running into deadlock issues or other issues, you can try to tune the packet size down by setting the packet size in the DNS like so.

##### `maxAllowedPacket`
```
Type:          decimal number
Default:       4194304
```

Max packet size allowed in bytes. The default value is 4 MiB and should be adjusted to match the server settings. `maxAllowedPacket=0` can be used to automatically fetch the `max_allowed_packet` variable from server *on every connection*.

You can read more about DSNs here https://github.com/go-sql-driver/mysql#dsn-data-source-name.