package main

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/cenkalti/backoff/v5"
	"github.com/fatih/color"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/posener/cmd"
	"golang.design/x/clipboard"
	"golang.org/x/mod/semver"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
)

const modulePath = "github.com/StirlingMarketingGroup/swoof"

var (
	version    = ""
	confDir, _ = os.UserConfigDir()
)

var (
	root = cmd.New()

	aliasesFiles = root.String("a", confDir+"/swoof/aliases.yaml", "your aliases file")

	connectionsFile = root.String("c", confDir+"/swoof/connections.yaml", "your connections file")

	skipData = root.Bool("n", false, "drop/create tables and triggers only, without importing data")

	threads = root.Int("t", 4, "max concurrent tables at the same time, import stability may vary wildly between servers while increasing this")

	all = root.Bool("all", false, "grabs all tables, specified tables are ignored")

	insertIgnoreInto = root.Bool("insert-ignore", false, "inserts into the existing table without overwriting the existing rows")

	dryRun = root.Bool("dry-run", false, "doesn't actually execute any queries that have an effect")

	verbose = root.Bool("v", false, "writes all queries to stdout")

	funcs = root.Bool("funcs", false, "imports all functions after tables")

	views = root.Bool("views", false, "imports all views after tables and funcs")

	procs = root.Bool("procs", false, "imports all stored procedures after tables, funcs, and views")

	noProgressBars = root.Bool("no-progress", false, "disables the progress bars")

	showVersion = root.Bool("version", false, "prints the version and exits")

	skipCount = root.Bool("skip-count", false, "skips the count query for each table, which can be slow for large tables")

	// not entirely sure how much this really affects performance,
	// since the performance bottleneck is almost guaranteed to be writing
	// the rows to the source
	rowBufferSize = root.Int("r", 10_000, "max rows buffer size. Will have this many rows downloaded and ready for importing")

	whereClause = root.String("w", "", "optional WHERE clause to filter rows from the source table (e.g. \"ID > 1000\")")

	tempTablePrefix = root.String("p", "_swoof_", "prefix of the temp table used for initial creation before the swap and drop")

	args = root.Args("source, dest, tables", "source, dest, tables, ex:\n"+
		"swoof [flags] 'user:pass@(host)/dbname' 'user:pass@(host)/dbname' table1 table2 table3\n\n"+
		"see: https://github.com/go-sql-driver/mysql#dsn-data-source-name\n\n"+
		"Or, optionally, you can use your connections in your connections file like so:\n\n"+
		"swoof [flags] production localhost table1 table2 table3\n\n"+
		"Multiple destinations can be comma-separated:\n\n"+
		"swoof [flags] production localhost,staging table1 table2 table3")
)

var definerRegexp = regexp.MustCompile(`\sDEFINER\s*=\s*[^ ]+`)

func maybeReportNewVersion() {
	module, current := moduleVersion()
	if module == "" || current == "" {
		return
	}
	if strings.Contains(strings.ToLower(current), "dev") {
		return
	}

	currentSemver := ensureSemverPrefix(current)

	goBin, err := exec.LookPath("go")
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, goBin, "list", "-m", "-u", "-json", fmt.Sprintf("%s@%s", module, currentSemver))
	cmd.Dir = os.TempDir()
	cmd.Env = append(os.Environ(), "GO111MODULE=on", "GOWORK=off")
	cmd.Stderr = io.Discard

	out, err := cmd.Output()
	if err != nil {
		return
	}

	var info struct {
		Update *struct {
			Version string `json:"Version"`
		} `json:"Update"`
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return
	}

	if info.Update == nil || info.Update.Version == "" {
		return
	}

	latestRaw := info.Update.Version
	if !isNewerVersion(latestRaw, current) {
		return
	}

	label := color.New(color.FgHiYellow).Sprint("\n⚠ update available")
	newVersion := color.New(color.FgHiGreen).Add(color.Bold).Sprintf("swoof %s", latestRaw)
	fmt.Fprintf(os.Stderr, "%s: %s is available!\nhttps://github.com/StirlingMarketingGroup/swoof/releases/latest\n\n", label, newVersion)
}

func moduleVersion() (string, string) {
	if version != "" {
		return modulePath, version
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return modulePath, ""
	}

	module := info.Main.Path
	if module == "" {
		module = modulePath
	}

	if info.Main.Version == "" || info.Main.Version == "(devel)" {
		return module, ""
	}

	return module, info.Main.Version
}

func isNewerVersion(latest, current string) bool {
	latestSemver := ensureSemverPrefix(latest)
	currentSemver := ensureSemverPrefix(current)

	if !semver.IsValid(latestSemver) || !semver.IsValid(currentSemver) {
		return false
	}

	return semver.Compare(latestSemver, currentSemver) > 0
}

// printTitle prints the top-of-screen banner: `swoof vX.Y.Z - Copyright…`.
// Emitted before the update-available warning and the "connecting to source"
// setup lines so the user sees exactly what's running before anything else.
func printTitle() {
	_, current := moduleVersion()
	if current == "" {
		current = "dev"
	}
	labelGreen := color.New(color.FgHiGreen).Add(color.Bold).SprintFunc()
	value := color.New(color.FgHiWhite).SprintFunc()
	copyrightWithYear := fmt.Sprintf("Copyright © %d Stirling Marketing Group", time.Now().Year())
	fmt.Printf("%s %s - %s\n\n", labelGreen("swoof"), value(current), value(copyrightWithYear))
}

func printRunHeader(source string, destinations []string, tables []string) {
	labelCyan := color.New(color.FgHiCyan).SprintFunc()
	value := color.New(color.FgHiWhite).SprintFunc()

	fmt.Printf("%s %s\n", labelCyan("source:       "), value(source))

	destLabel := "destination:  "
	if len(destinations) > 1 {
		destLabel = "destinations:"
	}
	fmt.Printf("%s %s\n", labelCyan(destLabel), value(strings.Join(destinations, ", ")))

	if len(tables) == 0 {
		fmt.Printf("%s %s\n", labelCyan("tables:       "), value("none"))
		return
	}
	fmt.Printf("%s %s\n\n", labelCyan("tables:       "), value(fmt.Sprintf("%d total", len(tables))))
	printTableColumns(tables, value)
	fmt.Println()
}

// printTableColumns prints the tables in ls-style column-major order sized
// to the terminal width. Falls back to one-per-line when stdout isn't a TTY
// or the width can't be determined.
func printTableColumns(tables []string, colorize func(a ...any) string) {
	const indent = 2
	const gutter = 2
	indentStr := strings.Repeat(" ", indent)

	longest := 0
	for _, t := range tables {
		if len(t) > longest {
			longest = len(t)
		}
	}

	width, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || width-indent < longest {
		for _, t := range tables {
			fmt.Printf("%s%s\n", indentStr, colorize(t))
		}
		return
	}

	colWidth := longest + gutter
	cols := max((width-indent+gutter)/colWidth, 1)
	cols = min(cols, len(tables))
	rows := (len(tables) + cols - 1) / cols

	for r := range rows {
		fmt.Print(indentStr)
		// Count how many columns actually have an item in this row, so
		// we can skip padding on the rightmost cell.
		actualCols := 0
		for c := range cols {
			if c*rows+r < len(tables) {
				actualCols++
			}
		}
		for c := range actualCols {
			name := tables[c*rows+r]
			if c < actualCols-1 {
				fmt.Print(colorize(fmt.Sprintf("%-*s", colWidth, name)))
			} else {
				fmt.Print(colorize(name))
			}
		}
		fmt.Println()
	}
}

// printSetupStatus writes a dim status line to stderr during the pre-TUI
// setup phase so the user can see the "connecting / resolving tables" work
// is actually progressing instead of looking like a silent hang.
func printSetupStatus(msg string) {
	prefix := color.New(color.FgHiBlack).Sprint("→")
	fmt.Fprintf(os.Stderr, "%s %s\n", prefix, msg)
}

func ensureSemverPrefix(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return v
	}
	switch v[0] {
	case 'v':
		return v
	case 'V':
		return "v" + v[1:]
	}
	return "v" + v
}

func main() {
	start := time.Now()

	// parse our command line arguments and make sure we
	// were given something that makes sense
	root.ParseArgs(os.Args...)

	if *showVersion {
		_, current := moduleVersion()
		if current == "" {
			current = "unknown"
		}
		fmt.Println(current)
		os.Exit(0)
	}

	printTitle()
	maybeReportNewVersion()

	if len(*args) < 2 {
		root.Usage()
		os.Exit(1)
	}

	if *skipData {
		*noProgressBars = true
	}

	// The TUI is the primary rendering path. It's disabled when the user
	// explicitly asks for plain output (-no-progress), when -verbose is set
	// (every query would flood the log pane), or when stdout is not a TTY
	// (piping / redirection). In the non-TUI path we also skip the COUNT(*)
	// per table since there's no bar to feed it into.
	useTUI := !*noProgressBars && !*verbose && term.IsTerminal(int(os.Stdout.Fd()))
	if !useTUI {
		*skipCount = true
	}

	// guard channel of structs makes sure we can easily block for
	// running only a max number of goroutines at a time
	var guard = make(chan struct{}, *threads)

	sourceDSN := (*args)[0]
	sourceFriendly := sourceDSN
	destDSNs := strings.Split((*args)[1], ",")
	destFriendlyNames := make([]string, 0, len(destDSNs))

	blue := color.New(color.FgBlue).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()

	// lookup connection information in the users config file
	// for much easier and shorter (and probably safer) command usage
	connections, _ := getConnections(*connectionsFile)

	// resolve source connection name
	if connections != nil {
		if c, ok := connections[sourceDSN]; ok {
			if c.DestOnly {
				slog.Error("source use is not allowed by config", "source", sourceDSN)
				os.Exit(1)
			}

			sourceDSN = connectionToDSN(c)
		}
	}

	// Anchor the source session to UTC so SHOW CREATE TABLE emits timestamp
	// defaults (e.g. TIMESTAMP's 2038-01-19 03:14:07.999999 max) in UTC rather
	// than rendered into whatever tz the server defaulted to.
	sourceDSN, err := ensureUTCSession(sourceDSN)
	if err != nil {
		slog.Error("failed to apply UTC session tz to source DSN", "error", err)
		os.Exit(1)
	}

	// Stop the source server from killing a streaming read conn when dest
	// backpressure stalls our TCP read side.
	sourceDSN, err = ensureLongSourceStream(sourceDSN)
	if err != nil {
		slog.Error("failed to apply net_write_timeout to source DSN", "error", err)
		os.Exit(1)
	}

	// source connection is the first argument
	// this is where our rows are coming from
	printSetupStatus(fmt.Sprintf("connecting to source %q...", sourceFriendly))
	src, err := mysql.NewFromDSN(sourceDSN, sourceDSN)
	if err != nil {
		slog.Error("failed to create source connection", "error", err, "sourceDSN", sourceDSN)
		os.Exit(1)
	}

	src.DisableUnusedColumnWarnings = true

	if *verbose {
		src.Log = func(detail mysql.LogDetail) {
			slog.Info(fmt.Sprintf("%s %s", blue("src:"), detail.Query))
		}
	}

	logFnSrc := src.Log
	src.Log = func(detail mysql.LogDetail) {
		if logFnSrc != nil {
			logFnSrc(detail)
		}
	}

	// resolve and create all destination connections upfront so that
	// each table's data is read from the source only once and fanned
	// out to every destination in parallel
	type destInfo struct {
		db          *mysql.Database
		isPath      bool
		isClipboard bool
		clipboard   *bytes.Buffer
	}

	var dsts []destInfo
	seenDestKeys := make(map[string]string)
	for _, rawDSN := range destDSNs {
		destDSN := strings.TrimSpace(rawDSN)
		friendlyName := destDSN

		destIsPath := strings.HasPrefix(destDSN, "file:")
		destIsClipboard := strings.EqualFold(destDSN, "clipboard")

		printSetupStatus(fmt.Sprintf("opening destination %q...", friendlyName))

		// resolve destination connection name
		if connections != nil && !destIsPath && !destIsClipboard {
			if c, ok := connections[destDSN]; ok {
				if c.SourceOnly {
					slog.Error("destination use is not allowed by config", "destination", destDSN)
					os.Exit(1)
				}

				if c.Params == nil {
					c.Params = make(map[string]string)
				}

				// we will disable foreign key checks on the destination
				// since we are importing more than one table at a time,
				// otherwise we *will* get errors about foreign key constraints
				if _, ok := c.Params["foreign_key_checks"]; !ok {
					c.Params["foreign_key_checks"] = "0"
				}

				destDSN = connectionToDSN(c)
			}
		}

		// warn when two destinations resolve to the same physical target, since
		// concurrent writes would collide on the shared temp table
		var dedupeKey string
		switch {
		case destIsPath:
			dedupeKey = "file:" + strings.TrimPrefix(destDSN, "file:")
		case destIsClipboard:
			dedupeKey = "clipboard"
		default:
			if cfg, parseErr := mysqldriver.ParseDSN(destDSN); parseErr == nil {
				dedupeKey = cfg.User + "@" + cfg.Addr + "/" + cfg.DBName
			} else {
				dedupeKey = destDSN
			}
		}
		if prev, ok := seenDestKeys[dedupeKey]; ok {
			label := color.New(color.FgHiYellow).Sprint("⚠ duplicate destination")
			fmt.Fprintf(os.Stderr, "%s: %q resolves to the same target as %q; concurrent writes will collide on the temp table\n", label, friendlyName, prev)
		} else {
			seenDestKeys[dedupeKey] = friendlyName
		}

		var db *mysql.Database
		var clipboardBuf *bytes.Buffer
		if destIsPath {
			name := strings.TrimPrefix(destDSN, "file:")

			if _, err := os.Stat(name); err == nil {
				incompleteName := name + ".incomplete"
				oldName := name + ".old"
				finalName := name
				name = incompleteName
				defer func() {
					// if the "old" directory exists, we can remove it
					if _, err := os.Stat(oldName); err == nil {
						slog.Info("removing old directory", "directory", oldName)
						if err := os.RemoveAll(oldName); err != nil {
							slog.Error("failed to remove old directory", "error", err, "directory", oldName)
							os.Exit(1)
						}
					}

					// once we're done, we can rename the already existing directory to something else,
					// so that we can rename our new directory to the correct name
					slog.Info("moving directory", "from", finalName, "to", oldName)
					if err := os.Rename(finalName, oldName); err != nil {
						slog.Error("failed to rename directory", "error", err, "from", finalName, "to", oldName)
						os.Exit(1)
					}

					// and then we can rename our new directory to the correct name
					slog.Info("moving directory", "from", name, "to", finalName)
					if err := os.Rename(name, finalName); err != nil {
						slog.Error("failed to rename directory", "error", err, "from", name, "to", finalName)
						os.Exit(1)
					}

					// and then we can remove the old directory
					slog.Info("removing old directory", "directory", oldName)
					if err := os.RemoveAll(oldName); err != nil {
						slog.Error("failed to remove old directory", "error", err, "directory", oldName)
						os.Exit(1)
					}
				}()
			}

			slog.Info("writing to file", "name", name)

			db, err = mysql.NewLocalWriter(name)
			if err != nil {
				slog.Error("failed to create local writer", "error", err, "name", name)
				os.Exit(1)
			}
		} else if destIsClipboard {
			clipboardBuf = new(bytes.Buffer)
			clipboardBuf.WriteString("set foreign_key_checks=0;\n\n")

			db, err = mysql.NewWriter(clipboardBuf)
			if err != nil {
				slog.Error("failed to create writer", "error", err)
				os.Exit(1)
			}
		} else {
			// Anchor every dest pool conn to UTC, matching the source. Without
			// this a non-UTC dest session reinterprets the SHOW CREATE TABLE
			// output from the source, shifting boundary timestamp defaults past
			// the 32-bit seconds range and 1067'ing on CREATE.
			destDSN, err = ensureUTCSession(destDSN)
			if err != nil {
				slog.Error("failed to apply UTC session tz to destination DSN", "error", err, "destinationDSN", friendlyName)
				os.Exit(1)
			}
			db, err = mysql.NewFromDSN(destDSN, destDSN)
			if err != nil {
				slog.Error("failed to create destination connection", "error", err, "destinationDSN", destDSN)
				os.Exit(1)
			}
		}

		db.DisableUnusedColumnWarnings = true

		if *verbose {
			db.Log = func(detail mysql.LogDetail) {
				slog.Info(fmt.Sprintf("%s %s", red("dst:"), detail.Query))
			}
		}

		logFnDst := db.Log
		db.Log = func(detail mysql.LogDetail) {
			if logFnDst != nil {
				logFnDst(detail)
			}
		}

		dsts = append(dsts, destInfo{db, destIsPath, destIsClipboard, clipboardBuf})
		destFriendlyNames = append(destFriendlyNames, friendlyName)
	}

	printSetupStatus("resolving tables...")
	tableNames, err := getTables(*aliasesFiles, *all, args, src)
	if err != nil {
		slog.Error("failed to get tables", "error", err, "aliasesFile", *aliasesFiles, "all", *all, "args", *args)
		os.Exit(1)
	}

	// get our tables ordered by the largest physical tables first
	// this *should* help performance, so that the longest table doesn't start last
	// and draw out the total process time
	// this also has the nice side effect of de-duplicating our tables list
	// we collect into a slice so it can be reused across multiple destinations
	var orderedTables []string
	if len(*tableNames) > 0 {
		printSetupStatus(fmt.Sprintf("ordering %d tables by size...", len(*tableNames)))
		tablesCh := make(chan string, len(*tableNames))
		go func() {
			defer close(tablesCh)
			err := src.Select(tablesCh, "select`table_name`"+
				"from`information_schema`.`TABLES`"+
				"where`table_schema`=database()"+
				"and`table_name`in({{ range $i, $table := .Tables }}{{ if $i }},{{ end }}{{ $table | printf `'%s'` }}{{ end }})"+
				"and`table_type`='BASE TABLE'"+
				"order by`data_length`+`index_length`desc", 0, mysql.Params{
				"Tables": *tableNames,
			})
			if err != nil {
				slog.Error("failed to select tables", "error", err)
				os.Exit(1)
			}
		}()
		for t := range tablesCh {
			orderedTables = append(orderedTables, t)
		}
	}

	printRunHeader(sourceFriendly, destFriendlyNames, orderedTables)

	var wg sync.WaitGroup

	// In TUI mode, build the UI up front — every table registers as a row
	// before any worker starts, so the first draw shows the full run. Route
	// slog output through the UI's log writer (which tees to a temp file).
	var u *ui
	if useTUI {
		var uiErr error
		u, uiErr = newUI(sourceFriendly, destFriendlyNames, orderedTables)
		if uiErr != nil {
			slog.Error("failed to start TUI", "error", uiErr)
			os.Exit(1)
		}
		defer u.Stop()
		log.SetOutput(u.LogWriter())
		go u.Run()
	}

	slog.Info("swoof run started",
		"source", sourceFriendly,
		"destinations", destFriendlyNames,
		"tables", len(orderedTables),
		"threads", *threads)

	// we need to delay some funcs, most notably the foreign key constraint part.
	// problem comes from us importing two tables that depend on each other; when
	// one finishes before the other, if we create the constraints as well, then it will fail
	// because the other table doesn't exist yet.
	delayedFuncs := make(chan func(), len(orderedTables))

	tableCount := 0
	for _, table := range orderedTables {
		tableCount++

		// this makes sure we capture tableName in a way that it doesn't
		// change on us within our loop
		// And IMO this is cleaner than having the func below accept the string
		tableName := table

		wg.Add(1)

		var state *tableState
		if u != nil {
			state = u.State(tableName)
		}

		go func() {
			defer wg.Done()

			// Throttle after the bar is registered so progress rendering
			// reflects all tables from the start; the semaphore just paces
			// how many goroutines actually do work concurrently.
			guard <- struct{}{}
			defer func() { <-guard }()

			// compute the per-table destination, applying WriterWithSubdir for file dests
			tableDsts := make([]*mysql.Database, len(dsts))
			for i, d := range dsts {
				tableDsts[i] = d.db
				if d.isPath {
					tableDsts[i] = d.db.WriterWithSubdir(filepath.Join("tables", tableName))
				}
			}

			tempTableName := *tempTablePrefix + tableName
			tableStart := time.Now()

			// Safe to retry because the real table is only swapped in by the
			// delayed finalization pass, which runs after the attempt succeeds.
			runOnce := func(attempt int) (struct{}, error) {
				if attempt > 1 {
					slog.Warn("retrying table import",
						"tableName", tableName,
						"attempt", attempt)
					state.Reset()
				}
				state.Begin(attempt)

				// Fresh *sql.DB pool per attempt so NewFromDSN / Ping failures
				// also go through the retry loop, and each table goroutine's
				// cursors and metadata queries don't contend on a shared pool.
				srcTable, err := mysql.NewFromDSN(sourceDSN, sourceDSN)
				if err != nil {
					return struct{}{}, errors.Wrapf(err, "open source connection for %q", tableName)
				}
				defer func() {
					if err := srcTable.Close(); err != nil {
						slog.Warn("failed to close per-attempt source pool", "error", err, "tableName", tableName)
					}
				}()
				// Don't recycle connections mid-stream — cool-mysql's 27s default
				// is Lambda-oriented and wrong for multi-hour table imports.
				srcTable.SetMaxConnectionTime(0)
				srcTable.DisableUnusedColumnWarnings = true
				if src.Log != nil {
					srcTable.Log = src.Log
				}

				columns := make(chan struct {
					ColumnName           string `mysql:"COLUMN_NAME"`
					Position             int    `mysql:"ORDINAL_POSITION"`
					DataType             string `mysql:"DATA_TYPE"`
					ColumnType           string `mysql:"COLUMN_TYPE"`
					GenerationExpression string `mysql:"GENERATION_EXPRESSION"`
				})

				// Deliberately outside the errgroup below: errgroup cancels ctx
				// asynchronously after a goroutine returns an error, but the
				// metadata goroutine's defer close(columns) runs first, so the
				// drain loop could see a clean channel close and proceed into
				// destination DDL before the error surfaced.
				//
				// `GENERATION_EXPRESSION` sometimes exists and sometimes doesn't, so we can't select for it.
				// You MAY be able to check the `INFORMATION_SCHEMA` table for column info on `INFORMATION_SCHEMA` itself
				// but Aurora MySQL doesn't seem to have values for this, unlike regular MySQL.
				columnsErrCh := make(chan error, 1)
				go func() {
					defer close(columns)
					columnsErrCh <- srcTable.SelectContext(context.Background(), columns, "select*"+
						"from`INFORMATION_SCHEMA`.`columns`"+
						"where`TABLE_SCHEMA`=database()"+
						"and`table_name`='"+tableName+"'"+
						"order by`ORDINAL_POSITION`", 0)
				}()

				rowStruct := dynamicstruct.NewStruct()
				columnsQuotedBld := new(strings.Builder)
				i := 0

				// Drain columns into the dynamic row struct. Cool mysql channel
				// selecting keeps only one row in memory at a time.
				for c := range columns {
					if len(c.GenerationExpression) != 0 {
						// You can't insert into generated columns, and mysql will actually
						// throw errors if you try. Skip them entirely.
						continue
					}

					if i != 0 {
						columnsQuotedBld.WriteByte(',')
					}
					columnsQuotedBld.WriteByte('`')
					columnsQuotedBld.WriteString(c.ColumnName)
					columnsQuotedBld.WriteByte('`')

					unsigned := strings.HasSuffix(c.ColumnType, "unsigned")

					f := "F" + strconv.Itoa(c.Position)
					tag := `mysql:"` + strings.ReplaceAll(c.ColumnName, `,`, `0x2C`) + `"`

					var v any

					// All field types are pointers so mysql scanning handles NULL gracefully.
					switch c.DataType {
					case "tinyint":
						if unsigned {
							v = new(uint8)
						} else {
							v = new(int8)
						}
					case "smallint":
						if unsigned {
							v = new(uint16)
						} else {
							v = new(int16)
						}
					case "int", "mediumint":
						if unsigned {
							v = new(uint32)
						} else {
							v = new(int32)
						}
					case "bigint":
						if unsigned {
							v = new(uint64)
						} else {
							v = new(int64)
						}
					case "float", "double":
						v = new(float64)
					case "decimal":
						// mysql.Raw is passed directly into the query with no escaping;
						// safe here because a decimal from mysql can't contain breaking characters.
						v = new(mysql.Raw)
					case "timestamp", "date", "datetime":
						v = new(string)
					case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
						v = new([]byte)
					case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum":
						v = new(string)
					case "json":
						// json.RawMessage lets cool mysql surround the value with charset info,
						// since mysql needs utf8 charset info for json columns.
						v = new(json.RawMessage)
					case "set":
						v = new(any)
					default:
						// Unknown column types are not transient — fail permanently so we
						// don't spin through retries on a schema shape we can't handle.
						// Drain remaining columns so the streaming goroutine can finish and
						// close its channel cleanly, then surface the error.
						for range columns {
						}
						<-columnsErrCh
						return struct{}{}, backoff.Permanent(errors.Errorf("unknown mysql column type %q for column %q on table %q", c.ColumnType, c.ColumnName, tableName))
					}

					rowStruct.AddField(f, v, tag)
					i++
				}

				// Synchronously collect the columns goroutine's result before any
				// destination DDL runs — this is the fix for the close/ctx race.
				if err := <-columnsErrCh; err != nil {
					return struct{}{}, errors.Wrapf(err, "select columns for %q", tableName)
				}

				structType := reflect.Indirect(reflect.ValueOf(rowStruct.Build().New())).Type()
				columnsQuoted := columnsQuotedBld.String()

				// errgroup scopes the row-stream, fan-out, and per-dest insert
				// goroutines for this attempt. First error cancels ctx so everyone
				// unwinds; g.Wait() returns the first error.
				g, ctx := errgroup.WithContext(context.Background())

				var count int64
				if !*skipData && !*skipCount {
					countQ := "select count(*)`Count`from`" + tableName + "`"
					if *whereClause != "" {
						countQ += " where " + *whereClause + " "
					}
					if err := srcTable.SelectContext(ctx, &count, countQ, 0); err != nil {
						return struct{}{}, errors.Wrapf(err, "count rows for %q", tableName)
					}
					state.SetTotal(count)
				}

				// Build the finalization closure during the attempt but do NOT push it
				// to delayedFuncs until the attempt actually succeeds — otherwise a
				// failed+retried table would have two entries.
				var onSuccess func()

				if !*insertIgnoreInto {
					var tableInfo struct {
						CreateMySQL string `mysql:"Create Table"`
					}
					if err := srcTable.SelectContext(ctx, &tableInfo, "show create table`"+tableName+"`", 0); err != nil {
						return struct{}{}, errors.Wrapf(err, "show create table %q", tableName)
					}

					// FK constraints have globally unique names, so creating the temp table
					// with them inline would collide with the already-existing real table.
					// Strip them out here and re-apply after the rename.
					var constraints string

					constraintsStart := strings.Index(tableInfo.CreateMySQL, ",\n  CONSTRAINT ")
					if constraintsStart != -1 {
						// MySQL always gives constraints as a contiguous block, so locate
						// the last one and treat everything between as the constraint block.
						constraintsEnd := strings.LastIndex(tableInfo.CreateMySQL, ",\n  CONSTRAINT ")
						constraintsEnd = constraintsEnd + strings.IndexByte(tableInfo.CreateMySQL[constraintsEnd+2:], '\n') + 2
						constraints = tableInfo.CreateMySQL[constraintsStart:constraintsEnd]
						tableInfo.CreateMySQL = tableInfo.CreateMySQL[:constraintsStart] + tableInfo.CreateMySQL[constraintsEnd:]
					}

					createSuffix := strings.TrimPrefix(tableInfo.CreateMySQL, "CREATE TABLE `"+tableName+"`")

					for _, dst := range tableDsts {
						if !*dryRun {
							if err := dst.Exec("drop table if exists`" + tempTableName + "`"); err != nil {
								return struct{}{}, errors.Wrapf(err, "drop temp table %q", tempTableName)
							}
							if err := dst.Exec("CREATE TABLE `" + tempTableName + "`" + createSuffix); err != nil {
								return struct{}{}, errors.Wrapf(err, "create temp table %q", tempTableName)
							}
						}
					}

					onSuccess = func() {
						// Queued to run after all tables finish importing. Renames the temp
						// table over the real one, re-adds constraints, and copies triggers.
						delayedFuncs <- func() {
							if !*dryRun {
								for _, dst := range tableDsts {
									if err := dst.Exec("drop table if exists`" + tableName + "`"); err != nil {
										slog.Error("failed to drop table", "error", err, "tableName", tempTableName)
										os.Exit(1)
									}

									// Non-atomic rename on purpose — atomic would also rename
									// other tables' FK references to point at the old name.
									// Small downtime on live dest is the accepted tradeoff.
									if err := dst.Exec("alter table`" + tempTableName + "`rename`" + tableName + "`"); err != nil {
										slog.Error("failed to rename table", "error", err, "from", tempTableName, "to", tableName)
										os.Exit(1)
									}

									if len(constraints) != 0 {
										if err := dst.Exec("alter table`" + tableName + "`" + strings.ReplaceAll(strings.TrimLeft(constraints, ","), "\n", "\nadd")); err != nil {
											slog.Warn("failed to add constraints to table", "error", err, "tableName", tableName)
										}
									}
								}
							}

							// Triggers: copy from the source (top-level src, since per-table
							// pool is already closed by the time delayed funcs run) to every dest.
							triggers := make(chan struct {
								Trigger string
							})
							go func() {
								defer close(triggers)
								if err := src.Select(triggers, "show triggers where`table`='"+tableName+"'", 0); err != nil {
									slog.Error("failed to select triggers", "error", err, "tableName", tableName)
									os.Exit(1)
								}
							}()
							for r := range triggers {
								var trigger struct {
									CreateMySQL string `mysql:"SQL Original Statement"`
								}
								if err := src.Select(&trigger, "show create trigger`"+r.Trigger+"`", 0); err != nil {
									slog.Error("failed to select trigger creation syntax", "error", err, "trigger", r.Trigger, "tableName", tableName)
									os.Exit(1)
								}

								// Strip DEFINER — the definer user may not exist on dest.
								trigger.CreateMySQL = definerRegexp.ReplaceAllString(trigger.CreateMySQL, "")

								if !*dryRun {
									for _, dst := range tableDsts {
										if err := dst.Exec(trigger.CreateMySQL); err != nil {
											slog.Error("failed to execute trigger creation SQL", "error", err, "trigger", r.Trigger, "tableName", tableName)
											os.Exit(1)
										}
									}
								}
							}
						}
					}
				}

				if attempt == 1 {
					if count > 0 {
						slog.Info("starting table", "tableName", tableName, "rows", count)
					} else {
						slog.Info("starting table", "tableName", tableName)
					}
				}

				if !*skipData && !*dryRun {
					insertPrefix := "insert into`" + tempTableName + "`"
					if *insertIgnoreInto {
						insertPrefix = "insert ignore into`" + tableName + "`"
					}

					// Spawned only after every synchronous setup step succeeds — a
					// pre-insert failure would otherwise return without g.Wait(),
					// leaking this producer blocked on a buffer with no consumer
					// and holding a source connection across the retry.
					srcChRef := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, structType), *rowBufferSize)

					g.Go(func() error {
						defer srcChRef.Close()

						q := "select /*+ MAX_EXECUTION_TIME(2147483647) */ " + columnsQuoted + "from`" + tableName + "`"
						if *whereClause != "" {
							q += " where " + *whereClause + " "
						}
						if err := srcTable.SelectContext(ctx, srcChRef.Interface(), q, 0); err != nil {
							return errors.Wrapf(err, "select rows for %q", tableName)
						}
						return nil
					})

					if len(dsts) == 1 {
						// Single destination: consume source channel directly.
						g.Go(func() error {
							inserter := tableDsts[0].I()
							if state != nil {
								inserter = inserter.SetAfterRowExec(func(_ time.Time) {
									state.Increment()
								})
							}
							if err := inserter.InsertContext(ctx, insertPrefix, srcChRef.Interface()); err != nil {
								return errors.Wrapf(err, "insert into %q", tableName)
							}
							return nil
						})
					} else {
						// Multiple destinations: fan out each row from the single source
						// channel to per-dest channels so the source is read only once.
						dstChRefs := make([]reflect.Value, len(dsts))
						for j := range dsts {
							dstChRefs[j] = reflect.MakeChan(reflect.ChanOf(reflect.BothDir, structType), *rowBufferSize)
						}

						g.Go(func() error {
							defer func() {
								for _, ref := range dstChRefs {
									ref.Close()
								}
							}()
							doneRef := reflect.ValueOf(ctx.Done())
							// Pre-allocated reflect.SelectCase slices — reused every
							// iteration. Allocating fresh per-row would burn millions
							// of tiny allocations on a big multi-dest fan-out.
							recvCases := []reflect.SelectCase{
								{Dir: reflect.SelectRecv, Chan: srcChRef},
								{Dir: reflect.SelectRecv, Chan: doneRef},
							}
							sendCasesByDest := make([][]reflect.SelectCase, len(dstChRefs))
							for i, ref := range dstChRefs {
								sendCasesByDest[i] = []reflect.SelectCase{
									{Dir: reflect.SelectSend, Chan: ref},
									{Dir: reflect.SelectRecv, Chan: doneRef},
								}
							}
							for {
								chosen, val, ok := reflect.Select(recvCases)
								if chosen == 1 {
									return ctx.Err()
								}
								if !ok {
									return nil
								}
								// Send to each dest channel with ctx cancellation awareness —
								// otherwise a downed insert goroutine that stopped receiving
								// would deadlock the fan-out.
								for i := range dstChRefs {
									sendCasesByDest[i][0].Send = val
									if chosen, _, _ := reflect.Select(sendCasesByDest[i]); chosen == 1 {
										return ctx.Err()
									}
								}
							}
						})

						for j := range dsts {
							g.Go(func() error {
								inserter := tableDsts[j].I()
								// Track progress from the first destination only.
								if j == 0 && state != nil {
									inserter = inserter.SetAfterRowExec(func(_ time.Time) {
										state.Increment()
									})
								}
								if err := inserter.InsertContext(ctx, insertPrefix, dstChRefs[j].Interface()); err != nil {
									return errors.Wrapf(err, "insert into %q (dest %d)", tableName, j)
								}
								return nil
							})
						}
					}
				}

				if err := g.Wait(); err != nil {
					return struct{}{}, err
				}

				// All streams and inserts succeeded — queue the finalization closure.
				if onSuccess != nil {
					onSuccess()
				}

				return struct{}{}, nil
			}

			bop := backoff.NewExponentialBackOff()
			bop.InitialInterval = 1 * time.Second
			bop.MaxInterval = 30 * time.Second

			attempts := 0
			wrappedOp := func() (struct{}, error) {
				attempts++
				v, err := runOnce(attempts)
				if err == nil {
					return v, nil
				}
				var perm *backoff.PermanentError
				if stderrors.As(err, &perm) {
					return v, err
				}
				// -insert-ignore writes directly to the real table (no temp swap),
				// so retry after a partial first attempt would double-insert rows
				// on tables without a unique key.
				if *insertIgnoreInto {
					return v, backoff.Permanent(err)
				}
				if !isTransientError(err) {
					return v, backoff.Permanent(err)
				}
				short := rootErrorMsg(err)
				slog.Warn("transient table import error, will retry",
					"tableName", tableName,
					"attempt", attempts,
					"cause", short,
					"error", err,
					"chain", formatErrorChain(err),
					"stack", extractErrorStack(err))
				state.Retrying(short)
				return v, err
			}

			if _, err := backoff.Retry(context.Background(), wrappedOp,
				backoff.WithBackOff(bop),
				backoff.WithMaxTries(5),
				// Disable backoff's default 15-minute total-time budget — a
				// multi-hour table attempt would exceed it on the first try
				// and skip retry entirely. MaxTries above bounds attempts.
				backoff.WithMaxElapsedTime(0),
			); err != nil {
				inner := err
				var perm *backoff.PermanentError
				if stderrors.As(err, &perm) {
					inner = perm.Err
				}
				slog.Error("table import failed after retries",
					"error", inner,
					"chain", formatErrorChain(inner),
					"stack", extractErrorStack(inner),
					"tableName", tableName,
					"attempts", attempts)
				state.Fail(rootErrorMsg(inner))
				if u != nil {
					u.Fatal(fmt.Errorf("table %q import failed after %d attempts: %w", tableName, attempts, inner))
					return
				}
				os.Exit(1)
			}

			elapsed := time.Since(tableStart).Round(time.Second)
			if attempts > 1 {
				slog.Info("recovered after retries",
					"tableName", tableName,
					"attempts", attempts,
					"duration", elapsed)
			}
			slog.Info("finished table",
				"tableName", tableName,
				"rows", state.Rows(),
				"duration", elapsed)

			state.Complete()
		}()
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	if u != nil {
		// If the TUI exits before workers finish, either the user interrupted
		// (Ctrl+C / q -> errInterrupted) or a worker hit a permanent failure
		// and called u.Fatal. We can't gracefully cancel the in-flight workers
		// (no shared ctx), so force-exit either way; differentiate the message
		// and exit code based on the recorded firstErr.
		select {
		case <-workersDone:
		case <-u.Done():
			log.SetOutput(os.Stderr)
			fatalErr := u.FirstError()
			path := u.LogPath()
			if stderrors.Is(fatalErr, errInterrupted) {
				fmt.Fprintln(os.Stderr, "\nswoof: interrupted")
				if path != "" {
					fmt.Fprintf(os.Stderr, "log: %s\n", path)
				}
				os.Exit(130)
			}
			if fatalErr != nil {
				fmt.Fprintf(os.Stderr, "\nswoof: %v\n", fatalErr)
			}
			if path != "" {
				fmt.Fprintf(os.Stderr, "full log: %s\n", path)
			}
			os.Exit(1)
		}

		// Normal completion: tear down the TUI before the finalization
		// phase so those log lines (and any os.Exit paths they may hit)
		// land on real stderr instead of being absorbed by a live tview
		// screen.
		fatalErr := u.FirstError()
		if fatalErr == nil {
			slog.Info("table imports complete",
				"tables", tableCount,
				"duration", time.Since(start).Round(time.Second))
		}
		u.Stop()
		log.SetOutput(os.Stderr)
		if fatalErr != nil {
			fmt.Fprintf(os.Stderr, "\nswoof: %v\n", fatalErr)
			if path := u.LogPath(); path != "" {
				fmt.Fprintf(os.Stderr, "full log: %s\n", path)
			}
			os.Exit(1)
		}
		if path := u.LogPath(); path != "" {
			fmt.Fprintf(os.Stderr, "log: %s\n", path)
		}
	} else {
		<-workersDone
	}

	if !*insertIgnoreInto {
		close(delayedFuncs)

		slog.Info("finalizing table imports...")

		for f := range delayedFuncs {
			f()
		}
	}

	// funcs, views, and procs are read from source once and applied to all dests
	if *funcs {
		slog.Info("importing functions...")

		funcDsts := make([]*mysql.Database, len(dsts))
		for i, d := range dsts {
			funcDsts[i] = d.db
			if d.isPath {
				funcDsts[i] = d.db.WriterWithSubdir("funcs")
			}
		}

		var funcs []struct {
			FuncName string `mysql:"ROUTINE_NAME"`
		}
		err = src.Select(&funcs, "select`ROUTINE_NAME`"+
			"from`information_schema`.`ROUTINES`"+
			"where`ROUTINE_SCHEMA`=database()"+
			"and`ROUTINE_TYPE`='FUNCTION'", 0)
		if err != nil {
			slog.Error("failed to select functions", "error", err)
			os.Exit(1)
		}

		for _, f := range funcs {
			var funcInfo struct {
				CreateMySQL string `mysql:"Create Function"`
			}
			err = src.Select(&funcInfo, "show create function`"+f.FuncName+"`", 0)
			if err != nil {
				slog.Error("failed to select function creation syntax", "error", err, "functionName", f.FuncName)
				os.Exit(1)
			}

			funcInfo.CreateMySQL = definerRegexp.ReplaceAllString(funcInfo.CreateMySQL, "")

			if !*dryRun {
				for _, dst := range funcDsts {
					err = dst.Exec("drop function if exists`" + f.FuncName + "`")
					if err != nil {
						slog.Error("failed to drop function", "error", err, "functionName", f.FuncName)
						os.Exit(1)
					}

					err = dst.Exec(funcInfo.CreateMySQL)
					if err != nil {
						slog.Error("failed to execute function creation SQL", "error", err, "functionName", f.FuncName)
						os.Exit(1)
					}
				}
			}
		}
	}

	if *views {
		slog.Info("importing views...")

		viewDsts := make([]*mysql.Database, len(dsts))
		for i, d := range dsts {
			viewDsts[i] = d.db
			if d.isPath {
				viewDsts[i] = d.db.WriterWithSubdir("views")
			}
		}

		var views []struct {
			ViewName string `mysql:"TABLE_NAME"`
		}
		err = src.Select(&views, "select`TABLE_NAME`"+
			"from`information_schema`.`TABLES`"+
			"where`TABLE_SCHEMA`=database()"+
			"and`TABLE_TYPE`='VIEW'", 0)
		if err != nil {
			slog.Error("failed to select views", "error", err)
			os.Exit(1)
		}

		for _, v := range views {
			var view struct {
				CreateMySQL string `mysql:"Create View"`
			}
			err = src.Select(&view, "show create view`"+v.ViewName+"`", 0)
			if err != nil {
				slog.Error("failed to select view creation syntax", "error", err, "viewName", v.ViewName)
				os.Exit(1)
			}

			view.CreateMySQL = definerRegexp.ReplaceAllString(view.CreateMySQL, "")

			if !*dryRun {
				for _, dst := range viewDsts {
					err = dst.Exec("drop view if exists`" + v.ViewName + "`")
					if err != nil {
						slog.Error("failed to drop view", "error", err, "viewName", v.ViewName)
						os.Exit(1)
					}

					err = dst.Exec(view.CreateMySQL)
					if err != nil {
						slog.Error("failed to execute view creation SQL", "error", err, "viewName", v.ViewName)
						os.Exit(1)
					}
				}
			}
		}
	}

	if *procs {
		slog.Info("importing stored procedures...")

		procDsts := make([]*mysql.Database, len(dsts))
		for i, d := range dsts {
			procDsts[i] = d.db
			if d.isPath {
				procDsts[i] = d.db.WriterWithSubdir("procs")
			}
		}

		var procs []struct {
			ProcName string `mysql:"ROUTINE_NAME"`
		}
		err = src.Select(&procs, "select`ROUTINE_NAME`"+
			"from`information_schema`.`ROUTINES`"+
			"where`ROUTINE_SCHEMA`=database()"+
			"and`ROUTINE_TYPE`='PROCEDURE'", 0)
		if err != nil {
			slog.Error("failed to select stored procedures", "error", err)
			os.Exit(1)
		}

		for _, p := range procs {
			var procInfo struct {
				CreateMySQL string `mysql:"Create Procedure"`
			}
			err = src.Select(&procInfo, "show create procedure`"+p.ProcName+"`", 0)
			if err != nil {
				slog.Error("failed to select stored procedure creation syntax", "error", err, "procedureName", p.ProcName)
				os.Exit(1)
			}

			procInfo.CreateMySQL = definerRegexp.ReplaceAllString(procInfo.CreateMySQL, "")

			if !*dryRun {
				for _, dst := range procDsts {
					err = dst.Exec("drop procedure if exists`" + p.ProcName + "`")
					if err != nil {
						slog.Error("failed to drop procedure", "error", err, "procedureName", p.ProcName)
						os.Exit(1)
					}

					err = dst.Exec(procInfo.CreateMySQL)
					if err != nil {
						slog.Error("failed to execute stored procedure creation SQL", "error", err, "procedureName", p.ProcName)
						os.Exit(1)
					}
				}
			}
		}
	}

	for _, d := range dsts {
		if d.isClipboard {
			d.clipboard.WriteString("set foreign_key_checks=1;\n")

			if err := clipboard.Init(); err != nil {
				slog.Error("failed to initialize clipboard", "error", err)
				os.Exit(1)
			}

			clipboard.Write(clipboard.FmtText, d.clipboard.Bytes())

			slog.Info("copied to clipboard", "size", len(d.clipboard.Bytes()))
		}
	}

	slog.Info("finished importing tables", "count", tableCount, "destinations", len(dsts), "duration", time.Since(start))
}
