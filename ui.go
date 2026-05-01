package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"golang.org/x/term"
)

// Render tick interval. tview coalesces redraws at ~50ms so this lands well
// under its ceiling without burning CPU.
const uiTickInterval = 100 * time.Millisecond

// errInterrupted is the sentinel stored on `firstErr` when the user exits via
// Ctrl+C or `q`. main checks for it to distinguish a user interrupt (exit
// code 130, "interrupted" message) from a worker-driven Fatal (exit code 1
// with the underlying error).
var errInterrupted = errors.New("interrupted")

type tableStatus int32

const (
	statusPending tableStatus = iota
	statusRunning
	statusRetrying
	// statusDone marks a table whose worker has finished importing data into
	// the temp table but whose post-run finalization (drop+rename of the real
	// table, trigger copy) has not yet happened. Rendered blue.
	statusDone
	// statusFinalized is the terminal success state: the temp table has been
	// renamed over the real one and triggers were copied. Rendered green.
	// Tables in -insert-ignore mode skip directly to this state since there
	// is no temp-table swap to wait on.
	statusFinalized
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

// Begin marks the table as running for `attempt`. StartedAt is reset every
// time so the rate/ETA shown in the body reflects the current attempt rather
// than time accumulated across earlier (failed) tries. Total wall-clock time
// for the table is tracked separately by the caller via a local time.Now().
func (s *tableState) Begin(attempt int) {
	if s == nil {
		return
	}
	s.StartedAt.Store(time.Now().UnixNano())
	s.FinishedAt.Store(0)
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

// Rows returns the current row count for logging, nil-safe so non-TUI callers
// can use it without branching.
func (s *tableState) Rows() int64 {
	if s == nil {
		return 0
	}
	return s.Current.Load()
}

func (s *tableState) Complete() {
	if s == nil {
		return
	}
	s.Total.Store(s.Current.Load())
	s.FinishedAt.Store(time.Now().UnixNano())
	s.setStatus(statusDone)
}

// Finalize bumps the table from "imported" (statusDone) to "fully done"
// (statusFinalized). Called by the delayed temp-table swap closure on
// success, or directly by the worker in -insert-ignore mode where there
// is no separate finalize step.
func (s *tableState) Finalize() {
	if s == nil {
		return
	}
	s.setStatus(statusFinalized)
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
	app                    *tview.Application
	flex                   *tview.Flex
	header                 *tview.TextView
	body                   *tview.TextView
	footer                 *tview.TextView
	states                 []*tableState
	byName                 map[string]*tableState
	logRing                *ringBuffer
	logFile                *os.File
	logWriter              *logWriter
	fatal                  chan error
	startedAt              time.Time
	stopOnce               sync.Once
	stopped                atomic.Bool
	firstErr               atomic.Pointer[error]
	source                 string
	dests                  []string
	done                   chan struct{}
	completed              atomic.Bool
	completedAt            atomic.Int64
	postCompletionRendered atomic.Bool
}

// newUI constructs the TUI but does NOT start it. Call Run to drive the event
// loop. The returned ui registers all tables up front so the body renders
// every row immediately on the first draw.
func newUI(source string, dests []string, tableNames []string) (*ui, error) {
	// Inherit the terminal's own colors instead of tview's default
	// black-bg/white-fg theme, so swoof blends with whatever color scheme
	// the user has configured (light themes, transparent terminals, etc.).
	// Explicit `[color]` tags in our output still override these per-text.
	tview.Styles = tview.Theme{
		PrimitiveBackgroundColor:    tcell.ColorDefault,
		ContrastBackgroundColor:     tcell.ColorDefault,
		MoreContrastBackgroundColor: tcell.ColorDefault,
		BorderColor:                 tcell.ColorDefault,
		TitleColor:                  tcell.ColorDefault,
		GraphicsColor:               tcell.ColorDefault,
		PrimaryTextColor:            tcell.ColorDefault,
		SecondaryTextColor:          tcell.ColorDefault,
		TertiaryTextColor:           tcell.ColorDefault,
		InverseTextColor:            tcell.ColorDefault,
		ContrastSecondaryTextColor:  tcell.ColorDefault,
	}

	logFile, err := os.CreateTemp("", "swoof-*.log")
	if err != nil {
		return nil, fmt.Errorf("create log file: %w", err)
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

	_, current := moduleVersion()
	if current == "" {
		current = "dev"
	}

	u.header = tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	u.header.SetBorder(true).SetTitle(fmt.Sprintf("swoof %s", current))
	u.body = tview.NewTextView().SetDynamicColors(true).SetWrap(false).SetScrollable(true)
	u.body.SetBorder(true).SetTitle(" tables ")
	u.footer = tview.NewTextView().SetDynamicColors(true).SetWrap(false).SetScrollable(true)
	u.footer.SetBorder(true).SetTitle(" log ")

	u.flex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(u.header, 4, 0, false).
		AddItem(u.body, 0, 1, true).
		AddItem(u.footer, 10, 0, false)

	u.app.SetRoot(u.flex, true).EnableMouse(false)

	u.app.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		isExit := ev.Key() == tcell.KeyCtrlC ||
			(ev.Key() == tcell.KeyRune && (ev.Rune() == 'q' || ev.Rune() == 'Q'))
		if !isExit {
			return ev
		}
		// In completion mode the run already finished — exit without
		// recording an interruption error so main reports success.
		if u.completed.Load() {
			u.Stop()
			return nil
		}
		u.Fatal(errInterrupted)
		return nil
	})

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

// Fatal records err as the run's first error (if none yet) and tears down
// the TUI. Idempotent and goroutine-safe. Used both by worker-driven failures
// and by the Ctrl+C / q input handler (which passes errInterrupted).
func (u *ui) Fatal(err error) {
	if err != nil && u.firstErr.CompareAndSwap(nil, &err) {
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

// MarkCompleted switches the UI into completion mode. The header swaps to a
// "Swoofed!" banner with a "Press Q to exit" prompt and the elapsed time
// freezes; the body and footer render once more (with their final content)
// then stop being touched, so the user can scroll either pane without the
// next render tick yanking their position. Q / Ctrl+C now stop cleanly
// instead of recording errInterrupted.
//
// We deliberately don't QueueUpdateDraw an immediate render here — if the
// user already pressed Q during finalization the event loop has stopped
// and the queued send would block forever. The 100ms render tick picks
// up the flags within one frame.
func (u *ui) MarkCompleted() {
	if u == nil {
		return
	}
	u.completedAt.Store(time.Now().UnixNano())
	u.completed.Store(true)
}

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

// Run starts the render tick goroutine and drives the tview event loop. Blocks
// until Stop is called or tview exits.
func (u *ui) Run() {
	defer close(u.done)

	stopTick := make(chan struct{})
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
	// Once we've drawn the completion frame we leave the body and footer
	// alone — calling SetText again would reset the user's scroll position.
	if u.postCompletionRendered.Load() {
		return
	}

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

	if u.completed.Load() {
		// Tint every pane's border green so the "done" state is obvious at
		// a glance even before the user reads the banner. Done on the last
		// frame only — postCompletionRendered short-circuits the next tick,
		// so the border color sticks until exit.
		u.header.SetBorderColor(tcell.ColorGreen)
		u.body.SetBorderColor(tcell.ColorGreen)
		u.footer.SetBorderColor(tcell.ColorGreen)
		u.postCompletionRendered.Store(true)
	}
}

func (u *ui) renderHeader(snaps []tableSnapshot) {
	_, current := moduleVersion()
	if current == "" {
		current = "dev"
	}

	var done, finalized, retrying, running, pending, failed int
	for _, s := range snaps {
		switch s.status {
		case statusDone:
			done++
		case statusFinalized:
			finalized++
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

	// Freeze the elapsed time at completion so the banner shows the final
	// run duration rather than ticking forever after the user is done.
	elapsedEnd := time.Now()
	if t := u.completedAt.Load(); t > 0 {
		elapsedEnd = time.Unix(0, t)
	}
	elapsed := elapsedEnd.Sub(u.startedAt).Round(time.Second)

	termW, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || termW < 20 {
		termW = 120
	}

	// Line 1: centered semantic one-liner summarising the run. Static text
	// is dim gray; values are bold white. Each value is wrapped as
	// `[white::b]VALUE[-:-:-]` so the bold attr always resets cleanly, and
	// `[gray]` re-applies gray for the next static run after each reset.
	// In completion mode the static text turns green; the "Press Q to exit"
	// prompt lives in the log pane (renderFooter) so it can't be missed.
	destStr := strings.Join(u.dests, ", ")
	var summaryPlain, line1 string
	if u.completed.Load() {
		summaryPlain = fmt.Sprintf("Swoofed %d tables from %s to %s in %s",
			len(snaps), u.source, destStr, elapsed)
		summaryPad := max((termW-utf8.RuneCountInString(summaryPlain))/2, 0)
		line1 = fmt.Sprintf("%s[green::b]Swoofed [white::b]%d[-:-:-][green::b] tables from [white::b]%s[-:-:-][green::b] to [white::b]%s[-:-:-][green::b] in [white::b]%s[-:-:-]",
			strings.Repeat(" ", summaryPad), len(snaps), u.source, destStr, elapsed)
	} else {
		summaryPlain = fmt.Sprintf("Swoofing %d tables from %s to %s - Elapsed time: %s",
			len(snaps), u.source, destStr, elapsed)
		summaryPad := max((termW-utf8.RuneCountInString(summaryPlain))/2, 0)
		line1 = fmt.Sprintf("%s[gray]Swoofing [white::b]%d[-:-:-][gray] tables from [white::b]%s[-:-:-][gray] to [white::b]%s[-:-:-][gray] - Elapsed time: [white::b]%s[-:-:-]",
			strings.Repeat(" ", summaryPad), len(snaps), u.source, destStr, elapsed)
	}

	// Line 2: four status counts evenly distributed across the width.
	type statItem struct {
		count int
		label string
		color string
	}
	// Lifecycle order, left-to-right: each row moves through the buckets
	// pending → running → retrying → imported → done as the run progresses.
	stats := []statItem{
		{pending, "pending", "gray"},
		{running, "running", "aqua"},
		{retrying, "retrying", "yellow"},
		{done, "imported", "blue"},
		{finalized, "done", "green"},
	}
	if failed > 0 {
		stats = append(stats, statItem{failed, "failed", "red"})
	}
	per := termW / len(stats)
	var line2 strings.Builder
	for i, it := range stats {
		text := fmt.Sprintf("%d %s", it.count, it.label)
		pad := max((per-len(text))/2, 0)
		line2.WriteString(strings.Repeat(" ", pad))
		fmt.Fprintf(&line2, "[%s]%s[-]", it.color, text)
		// Fill the rest of this segment (except the last) so everything stays aligned.
		if i < len(stats)-1 {
			line2.WriteString(strings.Repeat(" ", max(per-pad-len(text), 0)))
		}
	}

	u.header.SetText(line1 + "\n" + line2.String())
}

func (u *ui) renderBody(snaps []tableSnapshot) {
	width, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || width < 40 {
		width = 120
	}
	// Body has a border; tview draws left/right border columns, so every row
	// has two fewer usable columns than the raw terminal.
	width -= 2

	// Fixed caps for the two variable-width columns so widths don't bounce
	// around as tables finish or retry. Names longer than the cap are
	// middle-truncated (factorylab...itycosts); states longer are
	// right-truncated with a trailing ellipsis.
	const (
		maxNameW    = 28
		fixedStateW = 12
	)

	nameW := 8
	for _, s := range snaps {
		if l := len(s.name); l > nameW {
			nameW = l
		}
	}
	if nameW > maxNameW {
		nameW = maxNameW
	}

	now := time.Now()

	// Reorder for display: running > retrying > pending > done > failed.
	// Stable so tables within each bucket keep their data-size order.
	sort.SliceStable(snaps, func(i, j int) bool {
		return statusRank(snaps[i].status) < statusRank(snaps[j].status)
	})

	// Each table occupies two lines in narrow mode, so separate blocks with
	// a blank line; single-line wide mode just needs one newline.
	separator := "\n"
	if width < narrowWidth {
		separator = "\n\n"
	}

	var b strings.Builder
	for i, s := range snaps {
		if i > 0 {
			b.WriteString(separator)
		}
		b.WriteString(renderRow(s, nameW, fixedStateW, width, now))
	}
	u.body.SetText(b.String())
}

// narrowWidth is the threshold below which we split each table into a
// two-line block so the bar still has room to breathe. Measured after the
// body's border has been subtracted.
const narrowWidth = 100

const (
	pctW    = 4
	countsW = 11
	rateW   = 7
)

// renderRow picks between the wide single-line layout and the narrow two-line
// layout based on the available body width.
func renderRow(s tableSnapshot, nameW, stateW, termW int, now time.Time) string {
	if termW < narrowWidth {
		return renderRowNarrow(s, nameW, termW, now)
	}
	return renderRowWide(s, nameW, stateW, termW, now)
}

// renderRowWide is the single-line layout: glyph name pct [bar] counts rate state.
func renderRowWide(s tableSnapshot, nameW, stateW, termW int, now time.Time) string {
	attrs := statusAttrs(s.status)

	glyph := fmt.Sprintf("[%s]%s[-:-:-]", attrs, statusRune(s.status))
	name := truncateMiddle(s.name, nameW)
	namePadded := fmt.Sprintf("[%s]%-*s[-:-:-]", attrs, nameW, name)

	pct := renderPct(s)
	counts := renderCounts(s)
	rate := renderRate(s, now)
	state := renderState(s, now)

	countsP := fmt.Sprintf("%*s", countsW, truncateRight(counts, countsW))
	rateP := fmt.Sprintf("%*s", rateW, truncateRight(rate, rateW))
	stateP := fmt.Sprintf("[%s]%-*s[-:-:-]", attrs, stateW, truncateRight(state, stateW))

	// Width budget: 1 glyph + 1 sp + nameW + 1 sp + pctW + 1 sp
	//             + [bar] + 2 sp + countsW + 2 sp + rateW + 2 sp + stateW.
	barW := max(termW-(1+1+nameW+1+pctW+1+2+countsW+2+rateW+2+stateW), 10)
	bar := renderBar(s, barW)

	return fmt.Sprintf("%s %s %s %s  %s  %s  %s",
		glyph, namePadded, pct, bar, countsP, rateP, stateP)
}

// renderRowNarrow is the two-line layout used when the body is under
// narrowWidth cols. Line 1 is glyph/name/pct/bar; line 2 keeps counts and
// rate flush left and aligns state under the bar column (or, for short
// names where that would overlap the rate column, with at least a 2-space
// gap after rate).
func renderRowNarrow(s tableSnapshot, nameW, termW int, now time.Time) string {
	attrs := statusAttrs(s.status)

	glyph := fmt.Sprintf("[%s]%s[-:-:-]", attrs, statusRune(s.status))
	name := truncateMiddle(s.name, nameW)
	namePadded := fmt.Sprintf("[%s]%-*s[-:-:-]", attrs, nameW, name)
	pct := renderPct(s)

	// Column at which the bar starts on line 1. Line 2's state uses the
	// same column so the eye can track the row.
	indentW := 1 + 1 + nameW + 1 + pctW + 1

	// Line 1: glyph + name + pct + [bar fills remaining]
	barW := max(termW-indentW, 10)
	bar := renderBar(s, barW)
	line1 := fmt.Sprintf("%s %s %s %s", glyph, namePadded, pct, bar)

	// Line 2: counts  rate  <pad to bar column>  state
	counts := renderCounts(s)
	rate := renderRate(s, now)
	state := renderState(s, now)

	countsP := fmt.Sprintf("%-*s", countsW, truncateRight(counts, countsW))
	rateP := fmt.Sprintf("%-*s", rateW, truncateRight(rate, rateW))

	// Indent line 2 by 2 so counts/rate line up with the table name on line 1.
	const leadIndent = 2
	// Push state out to the bar column, with a minimum 2-space gap so short
	// names don't collide with the rate column.
	pad := max(indentW-(leadIndent+countsW+2+rateW), 2)
	stateW := max(termW-(leadIndent+countsW+2+rateW+pad), 0)
	stateP := fmt.Sprintf("[%s]%s[-:-:-]", attrs, truncateRight(state, stateW))

	line2 := strings.Repeat(" ", leadIndent) + countsP + "  " + rateP + strings.Repeat(" ", pad) + stateP
	return line1 + "\n" + line2
}

// statusAttrs returns the tview attr string (`color` or `color::b`) so running
// and retrying rows stand out in bold.
func statusAttrs(st tableStatus) string {
	c := statusColor(st)
	if st == statusRunning || st == statusRetrying {
		return c + "::b"
	}
	return c
}

func renderPct(s tableSnapshot) string {
	if s.status == statusDone || s.status == statusFinalized {
		return "100%"
	}
	if s.total <= 0 {
		return " -- "
	}
	p := min(100*s.current/s.total, 100)
	return fmt.Sprintf("%3d%%", p)
}

func truncateRight(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return string(r[:n])
	}
	return string(r[:n-1]) + "…"
}

// truncateMiddle collapses the middle of an over-width string with "..." so
// the beginning and end both survive (helpful for MySQL table names like
// factorylaboratorytestingutilitycosts → factoryla...itycosts). Falls back to
// right truncation when width is too small to fit the ellipsis.
func truncateMiddle(s string, w int) string {
	r := []rune(s)
	if len(r) <= w {
		return s
	}
	const ell = "..."
	if w <= len(ell)+1 {
		return truncateRight(s, w)
	}
	keep := w - len(ell)
	front := (keep + 1) / 2
	back := keep / 2
	return string(r[:front]) + ell + string(r[len(r)-back:])
}

func (u *ui) renderFooter() {
	lines := u.logRing.snapshot()
	for i, ln := range lines {
		lines[i] = colorizeLogLine(ln)
	}
	text := strings.Join(lines, "\n")
	if u.completed.Load() {
		// Pin a bright "Press Q to exit" prompt to the bottom of the log
		// pane on the final render so the user can't miss it. The blank
		// line separates it from the last log entry; ScrollToEnd below
		// keeps it visible by default while still letting the user scroll
		// back through the log (the render loop has stopped touching the
		// pane by this point).
		const prompt = "Press Q to exit"
		termW, _, err := term.GetSize(int(os.Stdout.Fd()))
		if err != nil || termW < 20 {
			termW = 120
		}
		// Footer has a border (2 cols) — usable width is termW-2.
		pad := max((termW-2-len(prompt))/2, 0)
		if text != "" {
			text += "\n\n"
		}
		text += strings.Repeat(" ", pad) + "[yellow::b]" + prompt + "[-:-:-]"
	}
	u.footer.SetText(text)
	u.footer.ScrollToEnd()
}

// logLevels maps the text levels emitted by slog's default handler to tview
// color tags; unknown levels fall through to plain text.
var logLevels = map[string]string{
	"DEBUG": "gray",
	"INFO":  "aqua",
	"WARN":  "yellow",
	"ERROR": "red",
}

// logKeyPattern matches `<space-or-start>key=` so the key part of a key=value
// slog attribute can be dimmed. Values are left as-is — they may be quoted
// and contain arbitrary content, so we don't try to parse them.
var logKeyPattern = regexp.MustCompile(`(^|\s)([A-Za-z_][A-Za-z0-9_.-]*)=`)

// colorizeLogLine applies inline tview color tags to a raw slog line of the
// form `YYYY/MM/DD HH:MM:SS LEVEL msg key=value…`. Timestamp is dim, level
// is colored by severity, attribute keys are dim, everything else keeps its
// default color. All raw text is escaped against tview's tag parser so
// bracket chars in SQL errors can't accidentally trigger color tags.
func colorizeLogLine(raw string) string {
	// Minimum prefix is "YYYY/MM/DD HH:MM:SS LEVEL " = 25 chars.
	if len(raw) < 25 || raw[4] != '/' || raw[7] != '/' || raw[10] != ' ' ||
		raw[13] != ':' || raw[16] != ':' || raw[19] != ' ' {
		return tview.Escape(raw)
	}

	ts := raw[:19]
	rest := raw[20:]

	var level, tail string
	for lvl := range logLevels {
		if strings.HasPrefix(rest, lvl) && (len(rest) == len(lvl) || rest[len(lvl)] == ' ') {
			level = lvl
			tail = strings.TrimPrefix(rest[len(lvl):], " ")
			break
		}
	}
	if level == "" {
		return tview.Escape(raw)
	}

	tailEscaped := tview.Escape(tail)
	tailColored := logKeyPattern.ReplaceAllString(tailEscaped, "$1[gray]$2[-]=")

	return fmt.Sprintf("[gray]%s[-] [%s::b]%s[-:-:-] %s",
		ts, logLevels[level], level, tailColored)
}

func statusColor(s tableStatus) string {
	switch s {
	case statusRunning:
		return "aqua"
	case statusRetrying:
		return "yellow"
	case statusDone:
		// Imported into the temp table but not yet swapped in. Blue makes
		// the in-flight finalize phase pop against the green "fully done"
		// rows below it.
		return "blue"
	case statusFinalized:
		return "green"
	case statusFailed:
		return "red"
	}
	return "gray"
}

// statusRank orders rows in the body so active work floats to the top and
// finished tables sink to the bottom. Ties preserve the size-descending
// order computed at startup. statusDone (imported, awaiting finalize) sits
// just above statusFinalized so finalized rows accumulate at the bottom
// of the "done" cluster as the swap pass progresses.
func statusRank(s tableStatus) int {
	switch s {
	case statusRunning:
		return 0
	case statusRetrying:
		return 1
	case statusPending:
		return 2
	case statusDone:
		return 3
	case statusFinalized:
		return 4
	case statusFailed:
		return 5
	}
	return 6
}

func statusRune(s tableStatus) string {
	switch s {
	case statusRunning:
		return "⟳"
	case statusRetrying:
		return "⏳"
	case statusDone, statusFinalized:
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
	case s.status == statusDone, s.status == statusFinalized:
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

	// tview only treats `[...]` as a tag when the contents form a valid one;
	// a standalone `[` or `]` renders literally, so we just wrap `inside`.
	return "[" + inside + "]"
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
			return "imported " + time.Duration(s.finishedAt-s.startedAt).Round(time.Second).String()
		}
		return "imported"
	case statusFinalized:
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
