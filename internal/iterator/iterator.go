package iterator

import (
	"context"
	"sync"
)

// New returns a new keys Iterator. It takes a next function that returns the next
// key and an error. At the end of iteration this function should return ErrEndOfIteration.
func New(ctx context.Context) *Iterator {
	it := &Iterator{ctx: ctx, out: make(chan string)}

	return it
}

// Iterator is an implementation of the gokv.KeysIterator that's used
// by the various backends.
type Iterator struct {
	ctx      context.Context
	out      chan string
	errMutex sync.RWMutex
	done     bool
	err      error
}

func (it *Iterator) Ch() <-chan string {
	return it.out
}

func (it *Iterator) Err() error {
	it.errMutex.RLock()
	defer it.errMutex.RUnlock()
	if !it.done {
		panic("iteration did not complete yet")
	}

	return it.err
}

func (it *Iterator) Write(k string) error {
	select {
	case <-it.ctx.Done():
		return it.ctx.Err()
	case it.out <- k:
		return nil
	}
}

func (it *Iterator) Close(err error) {
	it.errMutex.Lock()
	defer it.errMutex.Unlock()

	it.done = true
	it.err = err
	close(it.out)
}
