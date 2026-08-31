package raftstore

import (
	"encoding/binary"
	"errors"

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
