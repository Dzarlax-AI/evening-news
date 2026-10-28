package callbacks

import (
	"errors"
	"sync"
	"testing"
)

func TestAdmissionReplayAndBounds(t *testing.T) {
	a := NewAdmission(1, 2, 4)
	if _, cached, err := a.Begin("a"); cached || err != nil {
		t.Fatalf("begin: %v %v", cached, err)
	}
	if _, _, err := a.Begin("a"); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, _, err := a.Begin("b"); !errors.Is(err, ErrAdmissionFull) {
		t.Fatalf("full: %v", err)
	}
	response := []byte("ab")
	a.Finish("a", response)
	response[0] = 'x'
	got, cached, err := a.Begin("a")
	if err != nil || !cached || string(got) != "ab" {
		t.Fatalf("replay: %q %v %v", got, cached, err)
	}
	got[0] = 'x'
	if got, _, _ = a.Begin("a"); string(got) != "ab" {
		t.Fatal("replay was mutated")
	}
	if _, _, err = a.Begin("b"); err != nil {
		t.Fatal(err)
	}
	a.Finish("b", []byte("cd"))
	_, _, _ = a.Begin("a") // make a most recent
	if _, _, err = a.Begin("c"); err != nil {
		t.Fatal(err)
	}
	a.Finish("c", []byte("ef"))
	if _, cached, err = a.Begin("b"); cached || err != nil {
		t.Fatalf("b should have been evicted: %v %v", cached, err)
	}
	a.Finish("b", nil)
	if _, _, err = a.Begin("large"); err != nil {
		t.Fatal(err)
	}
	a.Finish("large", []byte("12345"))
	if _, cached, err = a.Begin("large"); cached || err != nil {
		t.Fatalf("large response cached: %v %v", cached, err)
	}
	a.Finish("large", nil)
}

func TestAdmissionConcurrentDuplicate(t *testing.T) {
	a := NewAdmission(16, 64, 1024)
	var wg sync.WaitGroup
	results := make(chan error, 32)
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := a.Begin("same"); results <- err }()
	}
	wg.Wait()
	close(results)
	admitted := 0
	for err := range results {
		if err == nil {
			admitted++
		} else if !errors.Is(err, ErrAlreadyRunning) {
			t.Fatal(err)
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted %d", admitted)
	}
	a.Finish("same", nil)
}
