package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"golang.org/x/term"
)

// Render tick interval. tview coalesces redraws at ~50ms so this lands well
// under its ceiling without burning CPU.
const uiTickInterval = 100 * time.Millisecond

type tableStatus int32

const (
	statusPending tableStatus = iota
	statusRunning
	statusRetrying
	statusDone
	statusFailed
)

// tableState is the single source of truth for one table's render state. All
// fields are atomically read/written so worker goroutines never need to hold a
// lock or call into tview.
type tableState struct {
	Name       string
	Total      atomic.Int64
	Current    atomic.Int64
	Status     atomic.Int32
	Attempt    atomic.Int32
	StartedAt  atomic.Int64
	FinishedAt atomic.Int64
	LastCause  atomic.Pointer[string]
}

func newTableState(name string) *tableState {
	return &tableState{Name: name}
}

func (s *tableState) setStatus(st tableStatus) { s.Status.Store(int32(st)) }

func (s *tableState) setCause(msg string) {
	if msg == "" {
		s.LastCause.Store(nil)
		return
	}
	s.LastCause.Store(&msg)
}

// The methods below are all nil-receiver safe so the caller in main.go can
// hold a *tableState that's nil in non-TUI mode and treat every call as a
// no-op without inline guards.

func (s *tableState) Begin(attempt int) {
	if s == nil {
		return
	}
	if s.StartedAt.Load() == 0 {
		s.StartedAt.Store(time.Now().UnixNano())
	}
	s.Attempt.Store(int32(attempt))
	s.setCause("")
	s.setStatus(statusRunning)
}

func (s *tableState) Retrying(cause string) {
	if s == nil {
		return
	}
	s.setCause(cause)
	s.setStatus(statusRetrying)
}

func (s *tableState) SetTotal(n int64) {
	if s == nil {
		return
	}
	s.Total.Store(n)
}

func (s *tableState) Increment() {
	if s == nil {
		return
	}
	s.Current.Add(1)
}

func (s *tableState) Reset() {
	if s == nil {
		return
	}
	s.Current.Store(0)
}

func (s *tableState) Complete() {
	if s == nil {
		return
	}
	s.Total.Store(s.Current.Load())
	s.FinishedAt.Store(time.Now().UnixNano())
	s.setStatus(statusDone)
}

func (s *tableState) Fail(cause string) {
	if s == nil {
		return
	}
	s.FinishedAt.Store(time.Now().UnixNano())
	s.setCause(cause)
	s.setStatus(statusFailed)
}

type tableSnapshot struct {
	name       string
	total      int64
	current    int64
	status     tableStatus
	attempt    int32
	startedAt  int64
	finishedAt int64
	cause      string
}

func (s *tableState) snapshot() tableSnapshot {
	var cause string
	if p := s.LastCause.Load(); p != nil {
		cause = *p
	}
	return tableSnapshot{
		name:       s.Name,
		total:      s.Total.Load(),
		current:    s.Current.Load(),
		status:     tableStatus(s.Status.Load()),
		attempt:    s.Attempt.Load(),
		startedAt:  s.StartedAt.Load(),
		finishedAt: s.FinishedAt.Load(),
		cause:      cause,
	}
}

// ringBuffer holds the tail of log output for the TUI footer. Mutex-guarded
// because slog can emit from many goroutines simultaneously.
type ringBuffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
}

func newRingBuffer(cap int) *ringBuffer {
	return &ringBuffer{cap: cap, lines: make([]string, 0, cap)}
}

func (r *ringBuffer) append(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.lines) < r.cap {
		r.lines = append(r.lines, line)
		return
	}
	copy(r.lines, r.lines[1:])
	r.lines[len(r.lines)-1] = line
}

func (r *ringBuffer) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.lines))
	copy(out, r.lines)
	return out
}

// logWriter implements io.Writer. slog's default handler routes through the
// log package, so `log.SetOutput(logWriter)` captures every slog.Info/Warn/Error
// emitted during the run. One Write = one log line (log always emits whole
// records), so we don't need to split on internal newlines.
type logWriter struct {
	ring *ringBuffer
	file io.Writer
}

func (w *logWriter) Write(p []byte) (int, error) {
	if w.file != nil {
		_, _ = w.file.Write(p)
	}
	line := strings.TrimRight(string(p), "\n")
	if line != "" {
		w.ring.append(line)
	}
	return len(p), nil
}

type ui struct {
	app       *tview.Application
	flex      *tview.Flex
	header    *tview.TextView
	body      *tview.TextView
	footer    *tview.TextView
	states    []*tableState
	byName    map[string]*tableState
	logRing   *ringBuffer
	logFile   *os.File
	logWriter *logWriter
	fatal     chan error
	startedAt time.Time
	stopOnce  sync.Once
	stopped   atomic.Bool
	stopTick  chan struct{}
	firstErr  atomic.Pointer[error]
	source    string
	dests     []string
	done      chan struct{}
}

// newUI constructs the TUI but does NOT start it. Call Run to drive the event
// loop. The returned ui registers all tables up front so the body renders
// every row immediately on the first draw.
func newUI(source string, dests []string, tableNames []string) (*ui, error) {
	logPath := filepath.Join(os.TempDir(), "swoof.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("create log file %q: %w", logPath, err)
	}

	u := &ui{
		app:       tview.NewApplication(),
		states:    make([]*tableState, 0, len(tableNames)),
		byName:    make(map[string]*tableState, len(tableNames)),
		logRing:   newRingBuffer(500),
		logFile:   logFile,
		fatal:     make(chan error, 1),
		startedAt: time.Now(),
		source:    source,
		dests:     dests,
		done:      make(chan struct{}),
	}
	u.logWriter = &logWriter{ring: u.logRing, file: logFile}

	for _, name := range tableNames {
		st := newTableState(name)
		u.states = append(u.states, st)
		u.byName[name] = st
	}

	u.header = tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	u.body = tview.NewTextView().SetDynamicColors(true).SetWrap(false).SetScrollable(true)
	u.footer = tview.NewTextView().SetDynamicColors(true).SetWrap(false).SetScrollable(true)
	u.footer.SetBorder(true).SetTitle(" log ")

	u.flex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(u.header, 3, 0, false).
		AddItem(u.body, 0, 1, true).
		AddItem(u.footer, 10, 0, false)

	u.app.SetRoot(u.flex, true).EnableMouse(false)

	u.app.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		switch ev.Key() {
		case tcell.KeyCtrlC:
			u.requestStop(errors.New("interrupted"))
			return nil
		case tcell.KeyRune:
			if ev.Rune() == 'q' || ev.Rune() == 'Q' {
				u.requestStop(errors.New("interrupted"))
				return nil
			}
		}
		return ev
	})

	// Seed body rows so the first paint shows every table at status pending.
	u.body.Clear()
	for range u.states {
		// Actual cell content filled in by tick; rows just need to exist.
	}

	return u, nil
}

func (u *ui) LogWriter() io.Writer { return u.logWriter }

func (u *ui) State(name string) *tableState { return u.byName[name] }

// LogPath returns the path of the temp log file written during the run.
func (u *ui) LogPath() string {
	if u.logFile == nil {
		return ""
	}
	return u.logFile.Name()
}

// Fatal records an unrecoverable error and tears down the TUI. Non-blocking —
// only the first error is retained.
func (u *ui) Fatal(err error) {
	if err == nil {
		return
	}
	if u.firstErr.CompareAndSwap(nil, &err) {
		select {
		case u.fatal <- err:
		default:
		}
	}
	u.Stop()
}

// FirstError returns the error passed to Fatal, if any, else nil.
func (u *ui) FirstError() error {
	if p := u.firstErr.Load(); p != nil {
		return *p
	}
	return nil
}

// Done returns a channel closed when the TUI event loop has finished.
func (u *ui) Done() <-chan struct{} { return u.done }

// Stop tears down the TUI and closes the log file. Idempotent and
// goroutine-safe.
func (u *ui) Stop() {
	u.stopOnce.Do(func() {
		u.app.Stop()
		if u.logFile != nil {
			_ = u.logFile.Sync()
			_ = u.logFile.Close()
		}
	})
}

func (u *ui) requestStop(err error) {
	if err != nil && u.firstErr.Load() == nil {
		u.firstErr.CompareAndSwap(nil, &err)
		select {
		case u.fatal <- err:
		default:
		}
	}
	u.Stop()
}

// Run starts the render tick goroutine and drives the tview event loop. Blocks
// until Stop is called or tview exits.
func (u *ui) Run() {
	defer close(u.done)

	stopTick := make(chan struct{})
	u.stopTick = stopTick
	tick := time.NewTicker(uiTickInterval)
	go func() {
		defer tick.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-tick.C:
				if u.stopped.Load() {
					return
				}
				u.app.QueueUpdateDraw(func() { u.render() })
			}
		}
	}()

	_ = u.app.Run()
	u.stopped.Store(true)
	close(stopTick)
}

func (u *ui) render() {
	_, h, err := term.GetSize(int(os.Stdout.Fd()))
	if err == nil {
		footerH := min(10, max(3, h*3/10))
		u.flex.ResizeItem(u.footer, footerH, 0)
	}

	snaps := make([]tableSnapshot, len(u.states))
	for i, s := range u.states {
		snaps[i] = s.snapshot()
	}

	u.renderHeader(snaps)
	u.renderBody(snaps)
	u.renderFooter()
}

func (u *ui) renderHeader(snaps []tableSnapshot) {
	_, current := moduleVersion()
	if current == "" {
		current = "dev"
	}

	var done, retrying, running, pending, failed int
	for _, s := range snaps {
		switch s.status {
		case statusDone:
			done++
		case statusRetrying:
			retrying++
		case statusRunning:
			running++
		case statusFailed:
			failed++
		default:
			pending++
		}
	}

	elapsed := time.Since(u.startedAt).Round(time.Second)

	destLabel := "destination"
	if len(u.dests) > 1 {
		destLabel = "destinations"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "[green::b]swoof[-:-:-] [white]%s[-]  [gray]·[-]  elapsed [white]%s[-]\n",
		current, elapsed)
	fmt.Fprintf(&b, "source: [white]%s[-]  [gray]→[-]  %s: [white]%s[-]\n",
		u.source, destLabel, strings.Join(u.dests, ", "))
	fmt.Fprintf(&b, "[cyan]%d tables[-]: [green]%d done[-], [yellow]%d retrying[-], [aqua]%d running[-], [gray]%d pending[-]",
		len(snaps), done, retrying, running, pending)
	if failed > 0 {
		fmt.Fprintf(&b, ", [red]%d failed[-]", failed)
	}
	u.header.SetText(b.String())
}

func (u *ui) renderBody(snaps []tableSnapshot) {
	width, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || width < 40 {
		width = 120
	}

	nameW := 8
	for _, s := range snaps {
		if l := len(s.name); l > nameW {
			nameW = l
		}
	}
	if nameW > 40 {
		nameW = 40
	}

	now := time.Now()
	var b strings.Builder
	for i, s := range snaps {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(renderRow(s, nameW, width, now))
	}
	u.body.SetText(b.String())
}

// renderRow builds one progress line for a single table. Layout fills the
// terminal width: <glyph> <name> <pct> [bar fills remaining] <counts> <rate> <state>.
// Fixed-width columns on the right let the bar absorb any leftover space.
// `name` and `state` share the bar's status color so the row reads as a unit.
func renderRow(s tableSnapshot, nameW, termW int, now time.Time) string {
	color := statusColor(s.status)

	glyph := fmt.Sprintf("[%s]%s[-]", color, statusRune(s.status)) // visible width 1

	name := s.name
	if len(name) > nameW {
		name = name[:nameW]
	}
	namePadded := fmt.Sprintf("[%s]%-*s[-]", color, nameW, name)

	pct := renderPct(s)       // 4 chars, e.g. "  9%", " 99%", "100%", " -- "
	counts := renderCounts(s) // plain text
	rate := renderRate(s, now)
	state := renderState(s, now)

	const (
		pctW    = 4
		countsW = 11
		rateW   = 7
		stateW  = 22
	)

	countsP := fmt.Sprintf("%*s", countsW, truncateRight(counts, countsW))
	rateP := fmt.Sprintf("%*s", rateW, truncateRight(rate, rateW))
	stateP := fmt.Sprintf("[%s]%-*s[-]", color, stateW, truncateRight(state, stateW))

	// Width budget: 1 glyph + 1 space + nameW + 1 space + pctW + 1 space
	//             + [bar] + 2 spaces + countsW + 2 spaces + rateW + 2 spaces + stateW.
	barW := max(termW-(1+1+nameW+1+pctW+1+2+countsW+2+rateW+2+stateW), 10)

	bar := renderBar(s, barW)

	return fmt.Sprintf("%s %s %s %s  %s  %s  %s",
		glyph, namePadded, pct, bar, countsP, rateP, stateP)
}

func renderPct(s tableSnapshot) string {
	if s.status == statusDone {
		return "100%"
	}
	if s.total <= 0 {
		return " -- "
	}
	p := min(100*s.current/s.total, 100)
	return fmt.Sprintf("%3d%%", p)
}

func truncateRight(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

func (u *ui) renderFooter() {
	lines := u.logRing.snapshot()
	// Escape bracket chars so arbitrary log text doesn't trip dynamic-color
	// parsing. tview's color tags look like [red] — a stray '[' in a SQL
	// error would otherwise be swallowed.
	for i, ln := range lines {
		lines[i] = tview.Escape(ln)
	}
	u.footer.SetText(strings.Join(lines, "\n"))
	u.footer.ScrollToEnd()
}

func statusColor(s tableStatus) string {
	switch s {
	case statusRunning:
		return "aqua"
	case statusRetrying:
		return "yellow"
	case statusDone:
		return "green"
	case statusFailed:
		return "red"
	}
	return "gray"
}

func statusRune(s tableStatus) string {
	switch s {
	case statusRunning:
		return "⟳"
	case statusRetrying:
		return "⏳"
	case statusDone:
		return "✓"
	case statusFailed:
		return "✗"
	}
	return "·"
}

// renderBar draws an mpb-style bracketed bar like `[=====>----------]`. Width
// is the total visible width including the brackets. Output uses tview color
// tags and `[[` to escape the literal opening bracket.
func renderBar(s tableSnapshot, width int) string {
	if width < 3 {
		return strings.Repeat("-", width)
	}
	inner := width - 2
	var filled int
	switch {
	case s.total <= 0:
		filled = 0
	case s.status == statusDone:
		filled = inner
	default:
		cur := min(s.current, s.total)
		filled = int(int64(inner) * cur / s.total)
	}

	var equalsRun, dashRun string
	switch {
	case filled == 0:
		dashRun = strings.Repeat("-", inner)
	case filled >= inner:
		equalsRun = strings.Repeat("=", inner)
	default:
		equalsRun = strings.Repeat("=", filled-1) + ">"
		dashRun = strings.Repeat("-", inner-filled)
	}

	color := statusColor(s.status)

	var inside string
	if equalsRun != "" {
		inside += fmt.Sprintf("[%s]%s[-]", color, equalsRun)
	}
	if dashRun != "" {
		inside += fmt.Sprintf("[gray]%s[-]", dashRun)
	}

	// `[[` is tview's escape for a literal `[`. `]` needs no escaping.
	return "[[" + inside + "]"
}

func renderCounts(s tableSnapshot) string {
	if s.total <= 0 && s.current <= 0 {
		return "--"
	}
	if s.total <= 0 {
		return formatShort(s.current) + "/?"
	}
	return formatShort(s.current) + "/" + formatShort(s.total)
}

func renderRate(s tableSnapshot, now time.Time) string {
	if s.startedAt == 0 {
		return ""
	}
	var endNanos int64
	if s.finishedAt > 0 {
		endNanos = s.finishedAt
	} else {
		endNanos = now.UnixNano()
	}
	elapsed := time.Duration(endNanos - s.startedAt)
	if elapsed <= 0 {
		return ""
	}
	rate := int64(float64(s.current) / elapsed.Seconds())
	return formatShort(rate) + "/s"
}

func renderState(s tableSnapshot, now time.Time) string {
	switch s.status {
	case statusPending:
		return "pending"
	case statusDone:
		if s.startedAt > 0 && s.finishedAt > 0 {
			return "done " + time.Duration(s.finishedAt-s.startedAt).Round(time.Second).String()
		}
		return "done"
	case statusFailed:
		if s.cause != "" {
			return "failed: " + s.cause
		}
		return "failed"
	case statusRetrying:
		if s.cause != "" {
			return fmt.Sprintf("retry %d: %s", s.attempt+1, s.cause)
		}
		return fmt.Sprintf("retry %d", s.attempt+1)
	case statusRunning:
		if s.startedAt > 0 && s.total > 0 && s.current > 0 {
			elapsed := now.Sub(time.Unix(0, s.startedAt))
			if elapsed > 0 {
				rate := float64(s.current) / elapsed.Seconds()
				if rate > 0 {
					remaining := float64(s.total-s.current) / rate
					return "eta " + time.Duration(remaining*float64(time.Second)).Round(time.Second).String()
				}
			}
		}
		return "running"
	}
	return ""
}
