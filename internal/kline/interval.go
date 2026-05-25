package kline

import "time"

// Interval defines a kline aggregation window.
type Interval struct {
	Name    string
	AlignFn func(t time.Time) time.Time // aligns t to the start of the interval
	Width   time.Duration               // fixed width; zero for calendar-based intervals
}

var Intervals = []Interval{
	{Name: "1m", AlignFn: alignN(time.Minute), Width: time.Minute},
	{Name: "10m", AlignFn: alignN(10 * time.Minute), Width: 10 * time.Minute},
	{Name: "30m", AlignFn: alignN(30 * time.Minute), Width: 30 * time.Minute},
	{Name: "1h", AlignFn: alignN(time.Hour), Width: time.Hour},
	{Name: "1w", AlignFn: alignWeek, Width: 7 * 24 * time.Hour},
	{Name: "1mon", AlignFn: alignMonth, Width: 0},
	{Name: "1y", AlignFn: alignYear, Width: 0},
}

// Lookup returns the interval definition by name.
func Lookup(name string) (Interval, bool) {
	for _, iv := range Intervals {
		if iv.Name == name {
			return iv, true
		}
	}
	return Interval{}, false
}

// alignN returns an aligner that truncates to the nearest interval boundary.
func alignN(d time.Duration) func(time.Time) time.Time {
	return func(t time.Time) time.Time {
		ms := t.UnixMilli()
		aligned := (ms / d.Milliseconds()) * d.Milliseconds()
		return time.UnixMilli(aligned).UTC()
	}
}

func alignWeek(t time.Time) time.Time {
	weekday := t.Weekday()
	if weekday == time.Sunday {
		weekday = 7
	}
	mon := t.AddDate(0, 0, -int(weekday-time.Monday))
	return time.Date(mon.Year(), mon.Month(), mon.Day(), 0, 0, 0, 0, time.UTC)
}

func alignMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func alignYear(t time.Time) time.Time {
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
}
