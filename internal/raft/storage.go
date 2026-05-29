package raft

import (
	"encoding/json"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

type PersistedState struct {
	CurrentTerm       uint64
	VotedFor          string
	Log               []LogEntry
	Snapshot          []byte
	SnapshotIndex     uint64
	SnapshotTerm      uint64
	LastApplied       uint64
	LastSnapshotState []byte
}

type StableStore interface {
	Load() (PersistedState, error)
	Save(PersistedState) error
	Close() error
}

type MemoryStore struct {
	state PersistedState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Load() (PersistedState, error) {
	return clonePersisted(s.state), nil
}

func (s *MemoryStore) Save(st PersistedState) error {
	s.state = clonePersisted(st)
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}

type BoltStore struct {
	db *bolt.DB
}

var stateBucket = []byte("raft")
var stateKey = []byte("state")

func OpenBoltStore(path string) (*BoltStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	return &BoltStore{db: db}, nil
}

func (s *BoltStore) Load() (PersistedState, error) {
	var out PersistedState
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(stateBucket)
		if b == nil {
			return nil
		}
		raw := b.Get(stateKey)
		if len(raw) == 0 {
			return nil
		}
		return json.Unmarshal(raw, &out)
	})
	return out, err
}

func (s *BoltStore) Save(st PersistedState) error {
	copyState := clonePersisted(st)
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(stateBucket)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(copyState)
		if err != nil {
			return err
		}
		return b.Put(stateKey, raw)
	})
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func clonePersisted(st PersistedState) PersistedState {
	out := st
	out.Log = append([]LogEntry(nil), st.Log...)
	out.Snapshot = append([]byte(nil), st.Snapshot...)
	out.LastSnapshotState = append([]byte(nil), st.LastSnapshotState...)
	return out
}
