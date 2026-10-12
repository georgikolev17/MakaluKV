package raftstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	raft "github.com/georgikolev17/MakaluKV/internal/raft"
)

// PebbleStore acts as a persistent storage for the Raft HardState.
type PebbleStore struct {
	db *pebble.DB
}

// OpenPebbleStore opens the Pebble db on the given path.
func OpenPebbleStore(path string) (*PebbleStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	// We defer a function that closes the db if some error occurs along the way
	success := false
	defer func() {
		if !success {
			_ = db.Close()
		}
	}()

	// Assign initial values to the Hard state if they dont exist

	// Assume currentTerm and votedFor are not initialised
	var currentTermExists, votedForExists bool

	_, closer, err := db.Get([]byte(keyCurrentTerm))
	if err != nil {
		// If error isn't Not Found, then we return the error
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
		// If the error is Not found, then the currentTerm is not initialised (which is expected) and we do nothing
	} else {
		// If there is no error, currentTerm exists, so we set the flag to true
		if err := closer.Close(); err != nil {
			return nil, err
		}
		currentTermExists = true
	}

	// We do the same logic for votedFor as for currentTerm
	_, closer, err = db.Get([]byte(keyVotedFor))

	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
	} else {
		if err := closer.Close(); err != nil {
			return nil, err
		}
		votedForExists = true
	}

	// If neither of currentTerm and votedFor are initialised, we initialise them
	if !currentTermExists && !votedForExists {
		// We put both commands in a batch as we want them to be executed atomically, i.e. either both or none of the commands execute on the db
		batch := db.NewBatch()
		defer batch.Close()

		// Assign 8 zero bytes to currentTerm
		if err := batch.Set([]byte(keyCurrentTerm), make([]byte, 8), nil); err != nil {
			return nil, err
		}

		// Assign empty byte slice to votedFor
		if err := batch.Set([]byte(keyVotedFor), make([]byte, 0), nil); err != nil {
			return nil, err
		}

		// Commit the batch
		if err := batch.Commit(pebble.Sync); err != nil {
			return nil, err
		}
	} else if currentTermExists != votedForExists {
		// If one of currentTerm and votedFor is initialsied and the other one is not, our hard state is inconsistent, so we return an error
		return nil, errors.New("inconsistent Raft hard state: current term and voted-for must either both exist or both be absent")
	}

	// If both currentTerm and votedFor are initialised, we mark the db opening as successful, and return reference to a PebbleStore object with the opened db
	success = true
	return &PebbleStore{db: db}, nil
}

// Close closes the Pebble db on the given path.
func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// --- HardState functionality ---

// LoadHardState loads votedFor and currentTerm from the db into a HardState object.
func (s *PebbleStore) LoadHardState() (raft.HardState, error) {
	value, closer, err := s.db.Get([]byte(keyCurrentTerm))
	if err != nil {
		return raft.HardState{}, err
	}

	if len(value) != 8 {
		if err := closer.Close(); err != nil {
			return raft.HardState{}, err
		}
		return raft.HardState{}, errors.New("invalid current term encoding")
	}

	currentTerm := raft.Term(binary.BigEndian.Uint64(value))

	if err := closer.Close(); err != nil {
		return raft.HardState{}, err
	}

	value, closer, err = s.db.Get([]byte(keyVotedFor))
	if err != nil {
		return raft.HardState{}, err
	}
	votedFor := raft.NodeID(value)

	if err := closer.Close(); err != nil {
		return raft.HardState{}, err
	}

	return raft.HardState{CurrentTerm: currentTerm, VotedFor: votedFor}, nil
}

// Persists votedFor and currentTerm in the Pebble db
func (s *PebbleStore) SaveHardState(hardState raft.HardState) error {
	termBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(termBytes, uint64(hardState.CurrentTerm))

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Set([]byte(keyCurrentTerm), termBytes, nil); err != nil {
		return err
	}

	if err := batch.Set([]byte(keyVotedFor), []byte(hardState.VotedFor), nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	return nil
}

// --- Log functionality ---

// LastLogInfo returns the index and term of the last entry of the log.
func (s *PebbleStore) LastLogInfo() (raft.LogIndex, raft.Term, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(keyLogPrefix),
		UpperBound: []byte(keyLogUpperBound),
	})

	if err != nil {
		return 0, 0, err
	}

	defer iter.Close()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}

	index, err := decodeLogKey(iter.Key())

	if err != nil {
		return 0, 0, err
	}

	entry, err := decodeLogValue(index, iter.Value())

	if err != nil {
		return 0, 0, err
	}

	return index, entry.Term, nil
}

// LogEntry returns the log entry with the provided index.
func (s *PebbleStore) LogEntry(index raft.LogIndex) (raft.LogEntry, bool, error) {
	key := encodeLogKey(index)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return raft.LogEntry{}, false, nil
		}
		return raft.LogEntry{}, false, err
	}
	defer closer.Close()

	entry, err := decodeLogValue(index, value)
	if err != nil {
		return raft.LogEntry{}, false, err
	}

	return entry, true, nil
}

// LogEntries returns log entries in the range [from, to).
// If to == 0, it returns all entries from from through the end of the log.
func (s *PebbleStore) LogEntries(from raft.LogIndex, to raft.LogIndex) ([]raft.LogEntry, error) {
	if to != 0 && from > to {
		return nil, errors.New("from index cannot be greater than to index")
	}

	startIndex := encodeLogKey(from)

	var endIndex []byte
	if to == 0 {
		endIndex = []byte(keyLogUpperBound)
	} else {
		endIndex = encodeLogKey(to)
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: startIndex,
		UpperBound: endIndex,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if !iter.First() {
		if err := iter.Error(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	var entries []raft.LogEntry

	for {
		index, err := decodeLogKey(iter.Key())
		if err != nil {
			return nil, err
		}

		entry, err := decodeLogValue(index, iter.Value())
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)

		if !iter.Next() {
			if err := iter.Error(); err != nil {
				return nil, err
			}
			break
		}
	}

	return entries, nil
}

// ReplaceLogSuffix deletes all log entries starting at from and replaces
// them with logs.
func (s *PebbleStore) ReplaceLogSuffix(from raft.LogIndex, logs []raft.LogEntry) error {
	if from == 0 {
		return errors.New("from index must be greater than zero")
	}

	// The replacement must form a contiguous suffix starting at from.
	for i, entry := range logs {
		expectedIndex := from + raft.LogIndex(i)

		if entry.Index != expectedIndex {
			return fmt.Errorf(
				"log entry index %d does not match expected index %d",
				entry.Index,
				expectedIndex,
			)
		}
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.DeleteRange(
		encodeLogKey(from),
		[]byte(keyLogUpperBound),
		nil,
	); err != nil {
		return err
	}

	for _, entry := range logs {
		if err := batch.Set(
			encodeLogKey(entry.Index),
			encodeLogValue(entry),
			nil,
		); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// --- Helper functions ---
func encodeLogKey(index raft.LogIndex) []byte {
	key := make([]byte, len(keyLogPrefix)+8)

	copy(key, keyLogPrefix)
	binary.BigEndian.PutUint64(key[len(keyLogPrefix):], uint64(index))

	return key
}

func decodeLogKey(key []byte) (raft.LogIndex, error) {
	if len(key) != len(keyLogPrefix)+8 {
		return 0, errors.New("invalid log key length")
	}

	if !bytes.HasPrefix(key, []byte(keyLogPrefix)) {
		return 0, errors.New("invalid log key prefix")
	}

	index := binary.BigEndian.Uint64(key[len(keyLogPrefix):])

	return raft.LogIndex(index), nil
}

func encodeLogValue(entry raft.LogEntry) []byte {
	value := make([]byte, 8+len(entry.Data))

	binary.BigEndian.PutUint64(value[:8], uint64(entry.Term))
	copy(value[8:], entry.Data)

	return value
}

func decodeLogValue(index raft.LogIndex, value []byte) (raft.LogEntry, error) {
	if len(value) < 8 {
		return raft.LogEntry{}, errors.New("invalid log value")
	}

	term := raft.Term(binary.BigEndian.Uint64(value[:8]))

	data := make([]byte, len(value)-8)
	copy(data, value[8:])

	return raft.LogEntry{
		Index: index,
		Term:  term,
		Data:  data,
	}, nil
}
