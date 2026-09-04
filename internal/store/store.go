package store

// Store is the local read model updated by committed Raft entries.
type Store interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
}
