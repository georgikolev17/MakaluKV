package raft

type testStorage struct {
	hardState HardState

	// Used to simulate errors during testing
	loadErr error
	saveErr error

	saveCalls int
}

func (s *testStorage) LoadHardState() (HardState, error) {
	if s.loadErr != nil {
		return HardState{}, s.loadErr
	}

	return s.hardState, nil
}

func (s *testStorage) SaveHardState(state HardState) error {
	s.saveCalls++

	if s.saveErr != nil {
		return s.saveErr
	}

	s.hardState = state
	return nil
}

func (s *testStorage) LastLogInfo() (LogIndex, Term, error) {
	return 0, 0, nil
}

func (s *testStorage) LogEntry(index LogIndex) (LogEntry, bool, error) {
	return LogEntry{}, false, nil
}

func (s *testStorage) LogEntries(from LogIndex, to LogIndex) ([]LogEntry, error) {
	return nil, nil
}

func (s *testStorage) ReplaceLogSuffix(from LogIndex, logs []LogEntry) error {
	return nil
}
