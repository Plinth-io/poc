package agent

import (
	"fmt"
	"testing"
	"time"
)

func TestLogBufferKeepsOnlyTheLastLines(t *testing.T) {
	b := NewLogBuffer(3)
	for i := 0; i < 5; i++ {
		fmt.Fprintf(b, "line %d\n", i)
	}
	got := b.Lines()
	want := []string{"line 2", "line 3", "line 4"}
	if len(got) != len(want) {
		t.Fatalf("Lines() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Lines()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLogBufferSplitsOnNewlinesOnly(t *testing.T) {
	b := NewLogBuffer(10)
	fmt.Fprint(b, "partial")
	if len(b.Lines()) != 0 {
		t.Fatalf("Lines() = %v, want empty until the line ends", b.Lines())
	}
	fmt.Fprint(b, " rest\n")
	if got := b.Lines(); len(got) != 1 || got[0] != "partial rest" {
		t.Fatalf("Lines() = %v, want [partial rest]", got)
	}
}

func TestSubscribeReceivesNewLines(t *testing.T) {
	b := NewLogBuffer(10)
	ch, stop := b.Subscribe()
	defer stop()

	fmt.Fprint(b, "erste\n")
	select {
	case line := <-ch:
		if line != "erste" {
			t.Fatalf("line = %q, want %q", line, "erste")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber never received the line")
	}
}

func TestSlowSubscriberDoesNotBlockWriters(t *testing.T) {
	b := NewLogBuffer(10)
	_, stop := b.Subscribe() // never drained
	defer stop()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			fmt.Fprintf(b, "line %d\n", i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a subscriber that stopped reading blocked the log writer")
	}
}
