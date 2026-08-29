package raftstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/georgikolev17/MakaluKV/internal/raft"
)

func TestFreshStoreInitialisesCorrectly(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	hardState, err := store.LoadHardState()
	if err != nil {
		t.Fatalf("Store failed to load hard state: %v", err)
	}

	if hardState.CurrentTerm != 0 {
		t.Errorf("Incorrect initialisation of CurrentTerm: expected 0, got %d", hardState.CurrentTerm)
	}

	if hardState.VotedFor != "" {
		t.Errorf("Incorrect initialisation of VotedFor: expected %q, got %q", "", hardState.VotedFor)
	}
}

func TestSaveAndLoadWorkCorrectly(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	err := store.SaveHardState(raft.HardState{CurrentTerm: 4, VotedFor: "node-2"})
	if err != nil {
		t.Fatalf("Failed to save hard state in the Pebble db: %v", err)
	}

	hardState, err := store.LoadHardState()
	if err != nil {
		t.Fatalf("Store failed to load hard state: %v", err)
	}

	// Check Hard state
	if hardState.CurrentTerm != 4 {
		t.Errorf("Incorrect CurrentTerm: expected 4, got %d", hardState.CurrentTerm)
	}

	if hardState.VotedFor != "node-2" {
		t.Errorf("Incorrect VotedFor: expected %q, got %q", "node-2", hardState.VotedFor)
	}
}

func TestHardStateSurvivesReopening(t *testing.T) {
	dir := t.TempDir()
	// Open Pebble store
	store, err := OpenPebbleStore(dir)
	if err != nil {
		t.Fatalf("failed to open Pebble store: %v", err)
	}

	closed := false

	t.Cleanup(func() {
		if !closed {
			if err := store.Close(); err != nil {
				t.Errorf("failed to close Pebble store: %v", err)
			}
		}
	})

	// Save Hard state
	err = store.SaveHardState(raft.HardState{CurrentTerm: 4, VotedFor: "node-2"})
	if err != nil {
		t.Fatalf("Failed to save hard state in the Pebble db: %v", err)
	}

	// Close the Pebble store
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close Pebble store: %v", err)
	}
	closed = true

	// Reopen the Pebble store
	store = openTestStore(t, dir)

	// Check Hard State
	hardState, err := store.LoadHardState()
	if err != nil {
		t.Fatalf("Store failed to load hard state: %v", err)
	}

	if hardState.CurrentTerm != 4 {
		t.Errorf("Incorrect CurrentTerm: expected 4, got %d", hardState.CurrentTerm)
	}

	if hardState.VotedFor != "node-2" {
		t.Errorf("Incorrect VotedFor: expected %q, got %q", "node-2", hardState.VotedFor)
	}
}

func TestSavingReplacesOldValues(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	err := store.SaveHardState(raft.HardState{CurrentTerm: 4, VotedFor: "node-2"})
	if err != nil {
		t.Fatalf("Failed to save hard state in the Pebble db: %v", err)
	}

	// Save again
	err = store.SaveHardState(raft.HardState{CurrentTerm: 5, VotedFor: ""})
	if err != nil {
		t.Fatalf("Failed to save hard state in the Pebble db: %v", err)
	}

	// Check new values
	hardState, err := store.LoadHardState()
	if err != nil {
		t.Fatalf("Store failed to load hard state: %v", err)
	}

	// Check Hard state
	if hardState.CurrentTerm != 5 {
		t.Errorf("Incorrect CurrentTerm: expected 5, got %d", hardState.CurrentTerm)
	}

	if hardState.VotedFor != "" {
		t.Errorf("Incorrect VotedFor: expected %q, got %q", "", hardState.VotedFor)
	}
}

func TestLoadHardStateRejectsInvalidCurrentTermEncoding(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)

	// Corrupt CurrentTerm: a valid encoded Term must be exactly 8 bytes.
	if err := store.db.Set(
		[]byte(keyCurrentTerm),
		[]byte{1, 2, 3},
		pebble.Sync,
	); err != nil {
		t.Fatalf("failed to corrupt current term: %v", err)
	}

	_, err := store.LoadHardState()
	if err == nil {
		t.Fatal("expected LoadHardState to return an error for invalid current term encoding")
	}
}

func openTestStore(t *testing.T, directory string) *PebbleStore {
	t.Helper()

	store, err := OpenPebbleStore(directory)
	if err != nil {
		t.Fatalf("failed to open Pebble store: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("failed to close Pebble store: %v", err)
		}
	})

	return store
}
