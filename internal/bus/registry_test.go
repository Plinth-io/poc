package bus

import (
	"errors"
	"sync"
	"testing"
)

func TestOpenAllocatesIDsBySide(t *testing.T) {
	tests := []struct {
		name    string
		side    Side
		wantIDs []uint64
	}{
		{name: "hub uses odd ids", side: SideHub, wantIDs: []uint64{1, 3, 5}},
		{name: "agent uses even ids", side: SideAgent, wantIDs: []uint64{2, 4, 6}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRegistry(tc.side)
			for _, want := range tc.wantIDs {
				s, err := r.Open()
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				if s.ID != want {
					t.Fatalf("ID = %d, want %d", s.ID, want)
				}
			}
		})
	}
}

func TestCloseIsIdempotentAndRemovesFromRegistry(t *testing.T) {
	r := NewRegistry(SideHub)
	s, err := r.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := errors.New("boom")

	s.Close(want)
	s.Close(errors.New("second close must not overwrite"))

	if !errors.Is(s.Err(), want) {
		t.Fatalf("Err() = %v, want %v", s.Err(), want)
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("Done() not closed after Close")
	}
	if _, ok := r.Get(s.ID); ok {
		t.Fatal("stream still in registry after Close")
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}
}

func TestCloseIsSafeUnderConcurrency(t *testing.T) {
	r := NewRegistry(SideHub)
	s, err := r.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Close(errors.New("racing close"))
		}()
	}
	wg.Wait()
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}
}

func TestAcceptRejectsDuplicateID(t *testing.T) {
	r := NewRegistry(SideAgent)
	if _, err := r.Accept(7); err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	if _, err := r.Accept(7); !errors.Is(err, ErrDuplicateStream) {
		t.Fatalf("second Accept error = %v, want ErrDuplicateStream", err)
	}
}

func TestRegistryEnforcesStreamLimit(t *testing.T) {
	r := NewRegistry(SideHub)
	for i := 0; i < MaxStreams; i++ {
		if _, err := r.Open(); err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
	}
	if _, err := r.Open(); !errors.Is(err, ErrTooManyStreams) {
		t.Fatalf("Open over limit = %v, want ErrTooManyStreams", err)
	}
}

func TestCloseAllClosesEveryStream(t *testing.T) {
	r := NewRegistry(SideHub)
	var streams []*Stream
	for i := 0; i < 5; i++ {
		s, err := r.Open()
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		streams = append(streams, s)
	}
	want := errors.New("connection lost")
	r.CloseAll(want)

	for _, s := range streams {
		select {
		case <-s.Done():
		default:
			t.Fatalf("stream %d not closed", s.ID)
		}
		if !errors.Is(s.Err(), want) {
			t.Fatalf("stream %d Err() = %v, want %v", s.ID, s.Err(), want)
		}
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}
}
