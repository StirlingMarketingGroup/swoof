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

// Below tview's ~50ms redraw coalescing ceiling.
const uiTickInterval = 100 * time.Millisecond

// Distinguishes a user Ctrl+C / q (exit 130) from a worker Fatal (exit 1).
var errInterrupted = errors.New("interrupted")

type tableStatus int32

const (
	statusPending tableStatus = iota
	statusRunning
	statusRetrying
	statusDone      // imported into temp table, awaiting swap. Blue.
	statusFinalized // swap + triggers complete. Green. -insert-ignore lands here directly.
	statusFailed
)

// Atomic fields so workers never need a lock or a tview call.
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

// All methods below are nil-receiver safe so non-TUI callers can ignore u.

// StartedAt resets every attempt so rate/ETA track the current try only.
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

// Called once the temp-table swap completes (or directly in -insert-ignore).
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

// One Write per log record (the log package emits whole records).
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

// tview v0.42.0 has no built-in TextView scrollbar. Callers must reserve
// the rightmost inner column so the bar doesn't sit on top of text.
type scrollBarView struct {
	*tview.TextView
}

func newScrollBarView() *scrollBarView {
	return &scrollBarView{TextView: tview.NewTextView()}
}

func (s *scrollBarView) Draw(screen tcell.Screen) {
	s.TextView.Draw(screen)

	x, y, w, h := s.GetInnerRect()
	if w <= 0 || h <= 0 {
		return
	}

	// SetWrap(false) means one '\n' = one row.
	text := s.GetText(false)
	total := strings.Count(text, "\n") + 1
	if total <= h {
		return
	}

	offset, _ := s.GetScrollOffset()
	maxOffset := total - h
	if offset < 0 {
		offset = 0
	}
	if offset > maxOffset {
		offset = maxOffset
	}

	thumbH := min(max(h*h/total, 1), h)
	thumbY := 0
	if maxOffset > 0 && h > thumbH {
		thumbY = offset * (h - thumbH) / maxOffset
	}

	// Thumb tracks BorderColor so the completion green tint flows through.
	thumbColor := s.GetBorderColor()
	trackStyle := tcell.StyleDefault.Foreground(tcell.ColorGray).Background(tcell.ColorDefault)
	thumbStyle := tcell.StyleDefault.Foreground(thumbColor).Background(tcell.ColorDefault)

	sx := x + w - 1
	for i := range h {
		ch := '│'
		st := trackStyle
		if i >= thumbY && i < thumbY+thumbH {
			ch = '█'
			st = thumbStyle
		}
		screen.SetContent(sx, y+i, ch, nil, st)
	}
}

type ui struct {
	app         *tview.Application
	flex        *tview.Flex
	header      *tview.TextView
	body        *scrollBarView
	footer      *scrollBarView
	states      []*tableState
	byName      map[string]*tableState
	logRing     *ringBuffer
	logFile     *os.File
	logWriter   *logWriter
	startedAt   time.Time
	stopOnce    sync.Once
	stopped     atomic.Bool
	firstErr    atomic.Pointer[error]
	source      string
	dests       []string
	done        chan struct{}
	completed   atomic.Bool
	completedAt atomic.Int64

	// SetTables vs render-tick. Workers only read after SetTables completes.
	statesMu sync.RWMutex
}

func newUI(source string, dests []string) (*ui, error) {
	// Inherit terminal colors instead of tview's black-on-white default.
	// Explicit `[color]` tags in our output still override per-text.
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
		byName:    make(map[string]*tableState),
		logRing:   newRingBuffer(500),
		logFile:   logFile,
		startedAt: time.Now(),
		source:    source,
		dests:     dests,
		done:      make(chan struct{}),
	}
	u.logWriter = &logWriter{ring: u.logRing, file: logFile}

	_, current := moduleVersion()
	if current == "" {
		current = "dev"
	}

	u.header = tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	u.header.SetBorder(true).SetTitle(fmt.Sprintf("swoof %s", current))
	u.body = newScrollBarView()
	u.body.SetDynamicColors(true).SetWrap(false).SetScrollable(true)
	u.body.SetBorder(true).SetTitle(" tables ")
	u.footer = newScrollBarView()
	u.footer.SetDynamicColors(true).SetWrap(false).SetScrollable(true)
	u.footer.SetBorder(true).SetTitle(" log ")

	u.flex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(u.header, 4, 0, false).
		AddItem(u.body, 0, 1, true).
		AddItem(u.footer, 10, 0, false)

	u.app.SetRoot(u.flex, true).EnableMouse(false)

	u.app.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		if ev.Key() == tcell.KeyTab || ev.Key() == tcell.KeyBacktab {
			if u.app.GetFocus() == u.footer {
				u.app.SetFocus(u.body)
			} else {
				u.app.SetFocus(u.footer)
			}
			return nil
		}

		isExit := ev.Key() == tcell.KeyCtrlC ||
			(ev.Key() == tcell.KeyRune && (ev.Rune() == 'q' || ev.Rune() == 'Q'))
		if !isExit {
			return ev
		}
		// Post-completion Q/Ctrl+C is a clean exit, not an interrupt.
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

func (u *ui) SetTables(names []string) {
	if u == nil {
		return
	}
	u.statesMu.Lock()
	defer u.statesMu.Unlock()
	u.states = make([]*tableState, 0, len(names))
	u.byName = make(map[string]*tableState, len(names))
	for _, n := range names {
		st := newTableState(n)
		u.states = append(u.states, st)
		u.byName[n] = st
	}
}

func (u *ui) State(name string) *tableState {
	if u == nil {
		return nil
	}
	u.statesMu.RLock()
	defer u.statesMu.RUnlock()
	return u.byName[name]
}

func (u *ui) LogPath() string {
	if u.logFile == nil {
		return ""
	}
	return u.logFile.Name()
}

func (u *ui) Fatal(err error) {
	if err != nil {
		u.firstErr.CompareAndSwap(nil, &err)
	}
	u.Stop()
}

func (u *ui) FirstError() error {
	if p := u.firstErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (u *ui) Done() <-chan struct{} { return u.done }

// No QueueUpdateDraw here — if the user already pressed Q during finalize,
// the event loop is gone and the queued send would block forever. The
// 100ms tick picks up the flags within one frame.
func (u *ui) MarkCompleted() {
	if u == nil {
		return
	}
	u.completedAt.Store(time.Now().UnixNano())
	u.completed.Store(true)
}

func (u *ui) Stop() {
	u.stopOnce.Do(func() {
		u.app.Stop()
		if u.logFile != nil {
			_ = u.logFile.Sync()
			_ = u.logFile.Close()
		}
	})
}

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
	focused := u.app.GetFocus()
	bodyTitle := "  tables "
	footerTitle := "  log "
	if focused == u.footer {
		footerTitle = " ▸ log "
	} else {
		bodyTitle = " ▸ tables "
	}
	u.body.SetTitle(bodyTitle)
	u.footer.SetTitle(footerTitle)

	_, h, err := term.GetSize(int(os.Stdout.Fd()))
	if err == nil {
		footerH := min(10, max(3, h*3/10))
		u.flex.ResizeItem(u.footer, footerH, 0)
	}

	u.statesMu.RLock()
	snaps := make([]tableSnapshot, len(u.states))
	for i, s := range u.states {
		snaps[i] = s.snapshot()
	}
	u.statesMu.RUnlock()

	u.renderHeader(snaps)
	u.renderBody(snaps)
	u.renderFooter()

	if u.completed.Load() {
		// 4-phase yellow/green blink, then settle on green.
		const blinkPhase = 125 * time.Millisecond
		const blinkDuration = 4 * blinkPhase
		var sinceCompleted time.Duration
		if t := u.completedAt.Load(); t > 0 {
			sinceCompleted = time.Since(time.Unix(0, t))
		}
		c := tcell.ColorGreen
		if sinceCompleted < blinkDuration && int(sinceCompleted/blinkPhase)%2 == 0 {
			c = tcell.ColorYellow
		}
		u.header.SetBorderColor(c)
		u.body.SetBorderColor(c)
		u.footer.SetBorderColor(c)
	}
}

// Strings get staticColor, everything else gets [white::b].
func centeredLine(termW int, staticColor string, parts ...any) string {
	var plain, b strings.Builder
	for i, p := range parts {
		if i > 0 {
			plain.WriteByte(' ')
			b.WriteByte(' ')
		}
		switch v := p.(type) {
		case string:
			plain.WriteString(v)
			b.WriteString("[" + staticColor + "]")
			b.WriteString(v)
			b.WriteString("[-:-:-]")
		default:
			s := fmt.Sprint(v)
			plain.WriteString(s)
			b.WriteString("[white::b]")
			b.WriteString(s)
			b.WriteString("[-:-:-]")
		}
	}
	pad := max((termW-utf8.RuneCountInString(plain.String()))/2, 0)
	return strings.Repeat(" ", pad) + b.String()
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

	// Freeze elapsed at completion so the banner doesn't keep ticking.
	elapsedEnd := time.Now()
	if t := u.completedAt.Load(); t > 0 {
		elapsedEnd = time.Unix(0, t)
	}
	elapsed := elapsedEnd.Sub(u.startedAt).Round(time.Second)

	termW, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || termW < 20 {
		termW = 120
	}

	destStr := strings.Join(u.dests, ", ")
	var line1 string
	switch {
	case u.completed.Load():
		line1 = centeredLine(termW, "green::b",
			"Swoofed", len(snaps), "tables from", u.source, "to", destStr, "in", elapsed)
	case len(snaps) == 0:
		line1 = centeredLine(termW, "gray",
			"Connecting to", u.source, "and resolving tables - Elapsed time:", elapsed)
	default:
		line1 = centeredLine(termW, "gray",
			"Swoofing", len(snaps), "tables from", u.source, "to", destStr, "- Elapsed time:", elapsed)
	}

	type statItem struct {
		count int
		label string
		color string
	}
	// Left-to-right matches lifecycle order so a run's progress reads naturally.
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

	if len(snaps) == 0 && !u.completed.Load() {
		const msg = "Resolving tables on "
		const dotCadence = 125 * time.Millisecond
		// Trailing spaces keep total width constant so centering doesn't jiggle.
		dotCount := int(time.Since(u.startedAt)/dotCadence)%3 + 1
		tail := strings.Repeat(".", dotCount) + strings.Repeat(" ", 3-dotCount)
		plainLen := len(msg) + len(u.source) + 3
		pad := max((width-2-plainLen)/2, 0)
		u.body.SetText(strings.Repeat("\n", 2) + strings.Repeat(" ", pad) +
			"[gray]" + msg + "[white::b]" + tview.Escape(u.source) + "[-:-:-][gray]" + tail + "[-]")
		return
	}

	// 2 cols of border + 1 reserved for the scrollbar overlay.
	width -= 3

	// Fixed caps so column widths don't jiggle as states change.
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

	// Active rows float up; stable sort preserves size-descending within a bucket.
	sort.SliceStable(snaps, func(i, j int) bool {
		return statusRank(snaps[i].status) < statusRank(snaps[j].status)
	})

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

// Below this many usable cols, rows go 2-line so the bar has room.
const narrowWidth = 100

const (
	pctW    = 4
	countsW = 11
	rateW   = 7
)

func renderRow(s tableSnapshot, nameW, stateW, termW int, now time.Time) string {
	if termW < narrowWidth {
		return renderRowNarrow(s, nameW, termW, now)
	}
	return renderRowWide(s, nameW, stateW, termW, now)
}

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

	// 1 glyph + 1sp + nameW + 1sp + pctW + 1sp + [bar] + 2sp + countsW + 2sp + rateW + 2sp + stateW.
	barW := max(termW-(1+1+nameW+1+pctW+1+2+countsW+2+rateW+2+stateW), 10)
	bar := renderBar(s, barW)

	return fmt.Sprintf("%s %s %s %s  %s  %s  %s",
		glyph, namePadded, pct, bar, countsP, rateP, stateP)
}

func renderRowNarrow(s tableSnapshot, nameW, termW int, now time.Time) string {
	attrs := statusAttrs(s.status)

	glyph := fmt.Sprintf("[%s]%s[-:-:-]", attrs, statusRune(s.status))
	name := truncateMiddle(s.name, nameW)
	namePadded := fmt.Sprintf("[%s]%-*s[-:-:-]", attrs, nameW, name)
	pct := renderPct(s)

	indentW := 1 + 1 + nameW + 1 + pctW + 1

	barW := max(termW-indentW, 10)
	bar := renderBar(s, barW)
	line1 := fmt.Sprintf("%s %s %s %s", glyph, namePadded, pct, bar)

	counts := renderCounts(s)
	rate := renderRate(s, now)
	state := renderState(s, now)

	countsP := fmt.Sprintf("%-*s", countsW, truncateRight(counts, countsW))
	rateP := fmt.Sprintf("%-*s", rateW, truncateRight(rate, rateW))

	const leadIndent = 2
	// 2-space minimum after rate, otherwise short names let state overlap.
	pad := max(indentW-(leadIndent+countsW+2+rateW), 2)
	stateW := max(termW-(leadIndent+countsW+2+rateW+pad), 0)
	stateP := fmt.Sprintf("[%s]%s[-:-:-]", attrs, truncateRight(state, stateW))

	line2 := strings.Repeat(" ", leadIndent) + countsP + "  " + rateP + strings.Repeat(" ", pad) + stateP
	return line1 + "\n" + line2
}

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
		const prompt = "Press Q to exit"
		termW, _, err := term.GetSize(int(os.Stdout.Fd()))
		if err != nil || termW < 20 {
			termW = 120
		}
		// 2 cols border + 1 reserved for scrollbar = -3 of usable width.
		pad := max((termW-3-len(prompt))/2, 0)
		if text != "" {
			text += "\n\n"
		}
		text += strings.Repeat(" ", pad) + "[yellow::b]" + prompt + "[-:-:-]"
	}
	u.footer.SetText(text)
	// Skip auto-scroll when focused so user scroll isn't yanked back.
	if u.app.GetFocus() != u.footer {
		u.footer.ScrollToEnd()
	}
}

var logLevels = map[string]string{
	"DEBUG": "gray",
	"INFO":  "aqua",
	"WARN":  "yellow",
	"ERROR": "red",
}

// Matches `key=` so we can dim keys without parsing the arbitrary values.
var logKeyPattern = regexp.MustCompile(`(^|\s)([A-Za-z_][A-Za-z0-9_.-]*)=`)

// Mirrors the body's per-row palette so the table lifecycle reads the
// same way in both panes. Unmatched messages render plain.
var logMessageColors = map[string]string{
	"starting table":              "aqua",
	"finished table":              "blue",
	"finalized table":             "green",
	"recovered after retries":     "green",
	"finalizing table imports...": "blue",
	"table imports complete":      "green",
	"finished importing tables":   "green",
}

// All raw text is tview.Escape'd so brackets in SQL errors don't trip the
// color-tag parser.
func colorizeLogLine(raw string) string {
	// Min prefix "YYYY/MM/DD HH:MM:SS LEVEL " = 25 chars.
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

	msg, attrs := tail, ""
	if loc := logKeyPattern.FindStringIndex(tail); loc != nil {
		msg = strings.TrimRight(tail[:loc[0]], " ")
		attrs = tail[loc[0]:]
	}

	msgRendered := tview.Escape(msg)
	if c, ok := logMessageColors[msg]; ok {
		msgRendered = fmt.Sprintf("[%s::b]%s[-:-:-]", c, msgRendered)
	}

	attrsColored := logKeyPattern.ReplaceAllString(tview.Escape(attrs), "$1[gray]$2[-]=")

	return fmt.Sprintf("[gray]%s[-] [%s::b]%s[-:-:-] %s%s",
		ts, logLevels[level], level, msgRendered, attrsColored)
}

func statusColor(s tableStatus) string {
	switch s {
	case statusRunning:
		return "aqua"
	case statusRetrying:
		return "yellow"
	case statusDone:
		return "blue"
	case statusFinalized:
		return "green"
	case statusFailed:
		return "red"
	}
	return "gray"
}

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
		return "▶"
	case statusRetrying:
		// U+21BB; the hourglass U+23F3 renders double-width on macOS.
		return "↻"
	case statusDone, statusFinalized:
		return "✓"
	case statusFailed:
		return "✗"
	}
	return "·"
}

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
