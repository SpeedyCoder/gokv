package gomap_test

import (
	"testing"

	"github.com/SpeedyCoder/gokv/encoding"
	"github.com/SpeedyCoder/gokv/internal/test"
)

// TestStore tests if reading from, writing to and deleting from the store works properly.
// A struct is used as value. See TestTypes() for a test that is simpler but tests all types.
func TestStore(t *testing.T) {
	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		store := createStore(t, encoding.JSON)
		test.Store(store, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		store := createStore(t, encoding.Gob)
		test.Store(store, t)
	})
}

// TestTypes tests if setting and getting values works with all Go types.
func TestTypes(t *testing.T) {
	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		store := createStore(t, encoding.JSON)
		test.Types(store, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		store := createStore(t, encoding.Gob)
		test.Types(store, t)
	})
}

// TestStoreConcurrent launches a bunch of goroutines that concurrently work with one store.
// The store is Go map with manual locking via sync.RWMutex, so testing this is important.
func TestStoreConcurrent(t *testing.T) {
	store := createStore(t, encoding.JSON)

	goroutineCount := 1000

	test.ConcurrentInteractions(t, goroutineCount, store)
}

// TestErrors tests some error cases.
func TestErrors(t *testing.T) {
	// Test empty key
	store := createStore(t, encoding.JSON)
	err := store.Set("", "bar")
	if err == nil {
		t.Error("Expected an error")
	}
	_, err = store.Get("", new(string))
	if err == nil {
		t.Error("Expected an error")
	}
	err = store.Delete("")
	if err == nil {
		t.Error("Expected an error")
	}
}

// TestNil tests the behaviour when passing nil or pointers to nil values to some methods.
func TestNil(t *testing.T) {
	// Test setting nil

	t.Run("set nil with JSON marshalling", func(t *testing.T) {
		store := createStore(t, encoding.JSON)
		err := store.Set("foo", nil)
		if err == nil {
			t.Error("Expected an error")
		}
	})

	t.Run("set nil with Gob marshalling", func(t *testing.T) {
		store := createStore(t, encoding.Gob)
		err := store.Set("foo", nil)
		if err == nil {
			t.Error("Expected an error")
		}
	})

	// Test passing nil or pointer to nil value for retrieval

	createTest := func(codec encoding.Encoding) func(t *testing.T) {
		return func(t *testing.T) {
			store := createStore(t, codec)

			// Prep
			err := store.Set("foo", test.Foo{Bar: "baz"})
			if err != nil {
				t.Error(err)
			}

			_, err = store.Get("foo", nil) // actually nil
			if err == nil {
				t.Error("An error was expected")
			}

			var i interface{} // actually nil
			_, err = store.Get("foo", i)
			if err == nil {
				t.Error("An error was expected")
			}

			var valPtr *test.Foo // nil value
			_, err = store.Get("foo", valPtr)
			if err == nil {
				t.Error("An error was expected")
			}
		}
	}
	t.Run("get with nil / nil value parameter", createTest(encoding.JSON))
	t.Run("get with nil / nil value parameter", createTest(encoding.Gob))
}

// TestClose tests if the close method returns any errors.
func TestClose(t *testing.T) {
	store := createStore(t, encoding.JSON)
	err := store.Close()
	if err != nil {
		t.Error(err)
	}
}

func createStore(t *testing.T, codec encoding.Encoding) Store {
	options := Options{
		Codec: codec,
	}
	store := NewStore(options)
	return store
}
