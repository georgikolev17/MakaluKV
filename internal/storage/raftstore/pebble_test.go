package raftstore

import (
	"bytes"
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

// --- Log Tests ---
func TestLastLogInfoReturnsZeroTermAndIndexWhenLogEmpty(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	index, term, err := store.LastLogInfo()

	if err != nil {
		t.Fatalf("failed to get last log info: %v", err.Error())
	}

	if index != 0 {
		t.Errorf("expected index: 0, got %d", index)
	}

	if term != 0 {
		t.Errorf("expected term: 0, got %d", term)
	}
}

func TestLastLogInfoReturnsLastEntryInfo(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	logs := []raft.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}

	if err := store.ReplaceLogSuffix(1, logs); err != nil {
		t.Fatalf("failed to write log entries: %v", err)
	}

	index, term, err := store.LastLogInfo()
	if err != nil {
		t.Fatalf("failed to get last log info: %v", err)
	}

	if index != 4 {
		t.Errorf("expected index: 4, got %d", index)
	}

	if term != 3 {
		t.Errorf("expected term: 3, got %d", term)
	}
}

func TestLogEntriesReturnsExpectedRange(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	logs := []raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 2, Data: []byte("three")},
		{Index: 4, Term: 2, Data: []byte("four")},
		{Index: 5, Term: 3, Data: []byte("five")},
	}

	if err := store.ReplaceLogSuffix(1, logs); err != nil {
		t.Fatalf("failed to write log entries: %v", err)
	}

	t.Run("to zero returns entries until end", func(t *testing.T) {
		entries, err := store.LogEntries(3, 0)
		if err != nil {
			t.Fatalf("failed to get log entries: %v", err)
		}

		expected := []raft.LogEntry{
			{Index: 3, Term: 2, Data: []byte("three")},
			{Index: 4, Term: 2, Data: []byte("four")},
			{Index: 5, Term: 3, Data: []byte("five")},
		}

		checkEntries(t, entries, expected)
	})

	t.Run("to greater than zero returns half-open range", func(t *testing.T) {
		entries, err := store.LogEntries(2, 5)
		if err != nil {
			t.Fatalf("failed to get log entries: %v", err)
		}

		expected := []raft.LogEntry{
			{Index: 2, Term: 1, Data: []byte("two")},
			{Index: 3, Term: 2, Data: []byte("three")},
			{Index: 4, Term: 2, Data: []byte("four")},
		}

		checkEntries(t, entries, expected)
	})
}

func TestReplaceLogSuffixReplacesExistingSuffix(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	logs := []raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 2, Data: []byte("three")},
		{Index: 4, Term: 2, Data: []byte("four")},
		{Index: 5, Term: 3, Data: []byte("five")},
	}

	if err := store.ReplaceLogSuffix(1, logs); err != nil {
		t.Fatalf("failed to write initial log entries: %v", err)
	}

	replacement := []raft.LogEntry{
		{Index: 3, Term: 4, Data: []byte("new-three")},
		{Index: 4, Term: 4, Data: []byte("new-four")},
	}

	if err := store.ReplaceLogSuffix(3, replacement); err != nil {
		t.Fatalf("failed to replace log suffix: %v", err)
	}

	entries, err := store.LogEntries(1, 0)
	if err != nil {
		t.Fatalf("failed to get log entries: %v", err)
	}

	expected := []raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 4, Data: []byte("new-three")},
		{Index: 4, Term: 4, Data: []byte("new-four")},
	}

	checkEntries(t, entries, expected)
}

func TestReplaceLogSuffixWithEmptyLogsTruncatesSuffix(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	logs := []raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 2, Data: []byte("three")},
		{Index: 4, Term: 2, Data: []byte("four")},
		{Index: 5, Term: 3, Data: []byte("five")},
	}

	if err := store.ReplaceLogSuffix(1, logs); err != nil {
		t.Fatalf("failed to write initial log entries: %v", err)
	}

	if err := store.ReplaceLogSuffix(3, nil); err != nil {
		t.Fatalf("failed to truncate log suffix: %v", err)
	}

	entries, err := store.LogEntries(1, 0)
	if err != nil {
		t.Fatalf("failed to get log entries: %v", err)
	}

	expected := []raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
	}

	checkEntries(t, entries, expected)
}

func TestLogEntryReturnsExistingEntry(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	logs := []raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 2, Data: []byte("two")},
		{Index: 3, Term: 3, Data: []byte("three")},
	}

	if err := store.ReplaceLogSuffix(1, logs); err != nil {
		t.Fatalf("failed to write log entries: %v", err)
	}

	entry, exists, err := store.LogEntry(2)
	if err != nil {
		t.Fatalf("failed to get log entry: %v", err)
	}

	if !exists {
		t.Fatal("expected log entry to exist")
	}

	expected := []raft.LogEntry{
		{Index: 2, Term: 2, Data: []byte("two")},
	}

	checkEntries(t, []raft.LogEntry{entry}, expected)
}

func TestLogEntryReturnsFalseWhenEntryDoesNotExist(t *testing.T) {
	store := openTestStore(t, t.TempDir())

	logs := []raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 2, Data: []byte("two")},
	}

	if err := store.ReplaceLogSuffix(1, logs); err != nil {
		t.Fatalf("failed to write log entries: %v", err)
	}

	_, exists, err := store.LogEntry(3)
	if err != nil {
		t.Fatalf("failed to get log entry: %v", err)
	}

	if exists {
		t.Error("expected log entry not to exist")
	}
}

// --- Helper functions ---

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

func checkEntries(t *testing.T, entries, expected []raft.LogEntry) {
	t.Helper()

	if len(entries) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(entries))
	}

	for i := range expected {
		if entries[i].Index != expected[i].Index {
			t.Errorf(
				"entry %d: expected index %d, got %d",
				i,
				expected[i].Index,
				entries[i].Index,
			)
		}

		if entries[i].Term != expected[i].Term {
			t.Errorf(
				"entry %d: expected term %d, got %d",
				i,
				expected[i].Term,
				entries[i].Term,
			)
		}

		if !bytes.Equal(entries[i].Data, expected[i].Data) {
			t.Errorf(
				"entry %d: expected data %q, got %q",
				i,
				expected[i].Data,
				entries[i].Data,
			)
		}
	}
}
