package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"golang.org/x/sync/errgroup"
)

const (
	// how long a cached table list is considered fresh
	cacheTTL = 1 * time.Hour
	// bound for the synchronous fetch on a cold completion miss
	coldFetchTimeout = 2 * time.Second
	// bound for foreground/background refreshes, where a short pause is fine
	refreshTimeout = 10 * time.Second
	// dial budget for the MySQL handshake — an unreachable host fails here
	// instead of tying up the whole read budget
	connectTimeout = 3 * time.Second
	// sub-second raw TCP pre-check, so a dead host fails almost instantly
	dialProbeTimeout = 500 * time.Millisecond
	// how many connections to warm at once during a batch refresh
	refreshConcurrency = 8

	listTablesQuery = "select`table_name`" +
		"from`information_schema`.`TABLES`" +
		"where`table_schema`=database()" +
		"and`table_type`='BASE TABLE'" +
		"order by`table_name`"
)

type tableCache struct {
	FetchedAt time.Time `json:"fetched_at"`
	Tables    []string  `json:"tables"`
}

// cacheKey turns a connection name into a filesystem-safe basename.
func cacheKey(name string) string {
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_", string(os.PathSeparator), "_").Replace(name)
}

func cachePathForConn(name string) (string, error) {
	dir, err := tableCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, cacheKey(name)+".json"), nil
}

func readTableCache(name string) (*tableCache, error) {
	path, err := cachePathForConn(name)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c tableCache
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func writeTableCache(name string, tables []string) error {
	path, err := cachePathForConn(name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(tableCache{FetchedAt: time.Now(), Tables: tables})
	if err != nil {
		return err
	}
	// atomic replace so a concurrent reader never sees a half-written file
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// fetchSourceTables connects to a source-capable connection and lists its base
// tables, bounded by timeout so a cold completion can never hang the shell.
func fetchSourceTables(c connection, timeout time.Duration) ([]string, error) {
	// Fast reachability pre-check: a dead or firewalled host fails here in
	// well under a second instead of eating the connect/read budget.
	if err := probeReachable(c.Host, dialProbeTimeout); err != nil {
		return nil, err
	}

	if c.Params == nil {
		c.Params = make(map[string]string)
	}
	// Short connect timeout (reachability) paired with a longer read timeout,
	// so a live-but-slow server still gets time to answer while a dead one
	// fails fast during the handshake.
	connect := min(connectTimeout, timeout)
	if _, ok := c.Params["timeout"]; !ok {
		c.Params["timeout"] = connect.String()
	}
	if _, ok := c.Params["readTimeout"]; !ok {
		c.Params["readTimeout"] = timeout.String()
	}

	dsn := connectionToDSN(c)
	db, err := mysql.NewFromDSN(dsn, dsn)
	if err != nil {
		return nil, err
	}
	db.DisableUnusedColumnWarnings = true

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var tables []string
	if err := db.SelectContext(ctx, &tables, listTablesQuery, 0); err != nil {
		return nil, err
	}
	return tables, nil
}

// probeReachable does a cheap TCP dial to decide whether a host is worth a full
// connection attempt. It defaults to MySQL's port when the host omits one.
func probeReachable(host string, timeout time.Duration) error {
	addr := host
	if _, _, err := net.SplitHostPort(host); err != nil {
		addr = net.JoinHostPort(host, "3306")
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	return conn.Close()
}

// cachedTablesForCompletion returns table names for a source connection for use
// in completion. It never blocks on the network beyond a short cold-miss fetch:
//   - fresh cache  -> returned immediately
//   - stale cache  -> returned immediately, refreshed in the background
//   - cold cache   -> one bounded synchronous fetch; on timeout, returns
//     nothing and kicks off a background refresh for next time
//
// Only named, source-capable connections are fetched; inline DSNs are ignored.
func cachedTablesForCompletion(source, connFile string) []string {
	conns, err := getConnections(connFile)
	if err != nil {
		return nil
	}
	c, ok := conns[source]
	if !ok || c.DestOnly {
		return nil
	}

	if cache, err := readTableCache(source); err == nil {
		if time.Since(cache.FetchedAt) >= cacheTTL {
			spawnBackgroundRefresh(source, connFile)
		}
		return cache.Tables
	}

	// cold miss — take the lock so parallel tab presses don't storm the server
	release, locked := acquireFetchLock(source)
	if !locked {
		if cache, err := readTableCache(source); err == nil {
			return cache.Tables
		}
		return nil
	}
	defer release()

	tables, err := fetchSourceTables(c, coldFetchTimeout)
	if err != nil {
		// too slow / unreachable: let a longer background fetch populate it
		spawnBackgroundRefresh(source, connFile)
		return nil
	}
	_ = writeTableCache(source, tables)
	return tables
}

// warmTableCacheAsync refreshes the cache for a named source connection using
// the already-open source database, as a zero-setup side effect of a real run.
func warmTableCacheAsync(src *mysql.Database, name string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
		defer cancel()
		var tables []string
		if err := src.SelectContext(ctx, &tables, listTablesQuery, 0); err != nil {
			return
		}
		_ = writeTableCache(name, tables)
	}()
}

// refreshAllConnections warms the cache for every source-capable named
// connection, concurrently, so the batch takes about as long as the slowest
// single connection rather than the sum. Used by `-refresh-completions` and
// `swoof init`.
func refreshAllConnections(connFile string, timeout time.Duration) {
	conns, err := getConnections(connFile)
	if err != nil {
		printSetupStatus("could not read connections file: " + err.Error())
		return
	}
	names := make([]string, 0, len(conns))
	for name := range conns {
		if !conns[name].DestOnly {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(names))

	var g errgroup.Group
	g.SetLimit(refreshConcurrency)
	for i, name := range names {
		g.Go(func() error {
			tables, err := fetchSourceTables(conns[name], timeout)
			if err != nil {
				results[i] = result{err: err}
				return nil
			}
			if err := writeTableCache(name, tables); err != nil {
				results[i] = result{err: err}
				return nil
			}
			results[i] = result{count: len(tables)}
			return nil
		})
	}
	_ = g.Wait()

	// Print after the fan-out completes so concurrent warms don't interleave.
	for i, name := range names {
		if r := results[i]; r.err != nil {
			printSetupStatus(name + ": " + r.err.Error())
		} else {
			printSetupStatus(name + ": " + strconv.Itoa(r.count) + " tables cached")
		}
	}
}

// acquireFetchLock guards the cold-fetch path with an exclusive lock file so
// mashing <Tab> doesn't open a burst of connections. A stale lock (from a
// crashed process) is stolen after coldFetchTimeout has comfortably passed.
func acquireFetchLock(name string) (release func(), ok bool) {
	dir, err := tableCacheDir()
	if err != nil {
		return nil, false
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, false
	}
	lp := filepath.Join(dir, cacheKey(name)+".lock")

	open := func() (*os.File, error) {
		return os.OpenFile(lp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	}
	f, err := open()
	if err != nil {
		if fi, statErr := os.Stat(lp); statErr == nil && time.Since(fi.ModTime()) > 5*time.Second {
			_ = os.Remove(lp)
			f, err = open()
		}
	}
	if err != nil {
		return nil, false
	}
	_ = f.Close()
	return func() { _ = os.Remove(lp) }, true
}
