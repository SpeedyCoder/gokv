package cockroachdb_test

import (
	"database/sql"
	"log"
	"testing"

	"github.com/SpeedyCoder/gokv/encoding"
	"github.com/SpeedyCoder/gokv/internal/test"
)

// TestClient tests if reading from, writing to and deleting from the store works properly.
// A struct is used as value. See TestTypes() for a test that is simpler but tests all types.
//
// Note: This test is only executed if the initial connection to CockroachDB works.
func TestClient(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to CockroachDB could be established. Probably not running in a proper test environment.")
	}

	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		defer client.Close()
		test.Store(client, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		defer client.Close()
		test.Store(client, t)
	})
}

// TestTypes tests if setting and getting values works with all Go types.
//
// Note: This test is only executed if the initial connection to CockroachDB works.
func TestTypes(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to CockroachDB could be established. Probably not running in a proper test environment.")
	}

	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		defer client.Close()
		test.Types(client, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		defer client.Close()
		test.Types(client, t)
	})
}

// TestClientConcurrent launches a bunch of goroutines that concurrently work with the CockroachDB client.
//
// Note: This test is only executed if the initial connection to CockroachDB works.
func TestClientConcurrent(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to CockroachDB could be established. Probably not running in a proper test environment.")
	}

	client := createClient(t, encoding.JSON)
	defer client.Close()

	goroutineCount := 1000

	test.ConcurrentInteractions(t, goroutineCount, client)
}

// TestErrors tests some error cases.
//
// Note: This test is only executed if the initial connection to CockroachDB works.
func TestErrors(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to CockroachDB could be established. Probably not running in a proper test environment.")
	}

	// Test empty key
	client := createClient(t, encoding.JSON)
	defer client.Close()
	err := client.Set("", "bar")
	if err == nil {
		t.Error("Expected an error")
	}
	_, err = client.Get("", new(string))
	if err == nil {
		t.Error("Expected an error")
	}
	err = client.Delete("")
	if err == nil {
		t.Error("Expected an error")
	}
}

// TestNil tests the behaviour when passing nil or pointers to nil values to some methods.
//
// Note: This test is only executed if the initial connection to CockroachDB works.
func TestNil(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to CockroachDB could be established. Probably not running in a proper test environment.")
	}

	// Test setting nil

	t.Run("set nil with JSON marshalling", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		defer client.Close()
		err := client.Set("foo", nil)
		if err == nil {
			t.Error("Expected an error")
		}
	})

	t.Run("set nil with Gob marshalling", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		defer client.Close()
		err := client.Set("foo", nil)
		if err == nil {
			t.Error("Expected an error")
		}
	})

	// Test passing nil or pointer to nil value for retrieval

	createTest := func(codec encoding.Encoding) func(t *testing.T) {
		return func(t *testing.T) {
			client := createClient(t, codec)
			defer client.Close()

			// Prep
			err := client.Set("foo", test.Foo{Bar: "baz"})
			if err != nil {
				t.Error(err)
			}

			_, err = client.Get("foo", nil) // actually nil
			if err == nil {
				t.Error("An error was expected")
			}

			var i interface{} // actually nil
			_, err = client.Get("foo", i)
			if err == nil {
				t.Error("An error was expected")
			}

			var valPtr *test.Foo // nil value
			_, err = client.Get("foo", valPtr)
			if err == nil {
				t.Error("An error was expected")
			}
		}
	}
	t.Run("get with nil / nil value parameter", createTest(encoding.JSON))
	t.Run("get with nil / nil value parameter", createTest(encoding.Gob))
}

// TestClose tests if the close method returns any errors.
//
// Note: This test is only executed if the initial connection to CockroachDB works.
func TestClose(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to CockroachDB could be established. Probably not running in a proper test environment.")
	}

	client := createClient(t, encoding.JSON)
	err := client.Close()
	if err != nil {
		t.Error(err)
	}
}

// checkConnection returns true if a connection could be made, false otherwise.
func checkConnection() bool {
	db, err := sql.Open("postgres", "postgres://root@localhost:26257/?sslmode=disable")
	if err != nil {
		log.Printf("An error occurred during testing the connection to the server: %v\n", err)
		return false
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Printf("An error occurred during testing the connection to the server: %v\n", err)
		return false
	}

	return true
}

func createClient(t *testing.T, codec encoding.Encoding) Client {
	options := Options{
		Codec: codec,
		// Higher values seem to lead to issues on Travis CI when using MySQL,
		// so let's just use the same value here.
		MaxOpenConnections: 25,
	}
	client, err := NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
