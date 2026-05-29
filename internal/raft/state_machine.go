package raft

import "encoding/json"

type applyResult struct {
	Value string
}

type kvSnapshot struct {
	Values map[string]string
	Seen   map[string]uint64
}

type stateMachine struct {
	values map[string]string
	seen   map[string]uint64
}

func newStateMachine() *stateMachine {
	return &stateMachine{
		values: map[string]string{},
		seen:   map[string]uint64{},
	}
}

func (sm *stateMachine) restore(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	var snap kvSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	sm.values = snap.Values
	sm.seen = snap.Seen
	if sm.values == nil {
		sm.values = map[string]string{}
	}
	if sm.seen == nil {
		sm.seen = map[string]uint64{}
	}
	return nil
}

func (sm *stateMachine) snapshot() ([]byte, error) {
	return json.Marshal(kvSnapshot{Values: sm.values, Seen: sm.seen})
}

func (sm *stateMachine) apply(cmd Command) applyResult {
	if cmd.ClientID != "" && sm.seen[cmd.ClientID] >= cmd.RequestID {
		return applyResult{}
	}
	switch cmd.Op {
	case OpPut:
		sm.values[cmd.Key] = cmd.Value
	case OpDelete:
		delete(sm.values, cmd.Key)
	}
	if cmd.ClientID != "" {
		sm.seen[cmd.ClientID] = cmd.RequestID
	}
	return applyResult{}
}

func (sm *stateMachine) get(key string) (string, bool) {
	v, ok := sm.values[key]
	return v, ok
}
