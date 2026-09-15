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
