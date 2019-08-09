package ctxconv

import (
	"context"

	"github.com/SpeedyCoder/gokv"
)

// ToContextStore converts an instance of Store to ContextStore
// that ignores the provided context.
func ToContextStore(store gokv.Store) gokv.ContextStore {
	return ctxStore{store: store}
}

// ToStore converts an instance of ContextStore to Store
// that passes in context.Background to all actions.
func ToStore(store gokv.ContextStore) gokv.Store {
	return simpleStore{store: store}
}

type ctxStore struct {
	store gokv.Store
}

func (s ctxStore) Set(_ context.Context, k string, v interface{}) error {
	return s.store.Set(k, v)
}

func (s ctxStore) Get(_ context.Context, k string, v interface{}) (found bool, err error) {
	return s.store.Get(k, v)
}

func (s ctxStore) Delete(_ context.Context, k string) error {
	return s.store.Delete(k)
}

func (s ctxStore) Close() error {
	return s.store.Close()
}

type simpleStore struct {
	store gokv.ContextStore
}

func (s simpleStore) Set(k string, v interface{}) error {
	return s.store.Set(context.Background(), k, v)
}

func (s simpleStore) Get(k string, v interface{}) (found bool, err error) {
	return s.store.Get(context.Background(), k, v)
}

func (s simpleStore) Delete(k string) error {
	return s.store.Delete(context.Background(), k)
}

func (s simpleStore) Close() error {
	return s.store.Close()
}
