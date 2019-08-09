package postgresql_test

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
// Note: This test is only executed if the initial connection to PostgreSQL works.
func TestClient(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to PostgreSQL could be established. Probably not running in a proper test environment.")
	}

	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		test.Store(client, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		test.Store(client, t)
	})
}

// TestTypes tests if setting and getting values works with all Go types.
//
// Note: This test is only executed if the initial connection to PostgreSQL works.
func TestTypes(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to PostgreSQL could be established. Probably not running in a proper test environment.")
	}

	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		test.Types(client, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		test.Types(client, t)
	})
}

// TestClientConcurrent launches a bunch of goroutines that concurrently work with the PostgreSQL client.
//
// Note: This test is only executed if the initial connection to PostgreSQL works.
func TestClientConcurrent(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to PostgreSQL could be established. Probably not running in a proper test environment.")
	}

	client := createClient(t, encoding.JSON)

	goroutineCount := 1000

	test.ConcurrentInteractions(t, goroutineCount, client)
}

// TestErrors tests some error cases.
//
// Note: This test is only executed if the initial connection to PostgreSQL works.
func TestErrors(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to PostgreSQL could be established. Probably not running in a proper test environment.")
	}

	// Test empty key
	client := createClient(t, encoding.JSON)
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

	// Test default options.
	// Will lead to an error because our Docker container requires a password.
	//
	// TODO: Currently doesn't work because our PostgreSQL server runs in Docker in port 5433,
	// while Travis CI has a PostgreSQL server running on 5432, where the gokv DB doesn't exist.
	// We could create that DB in the Travis CI configuration, but then this test would still not work
	// because it expects an invalid password, but the Travis CI default configuration is configured
	// with an empty password.
	// client, err = postgresql.NewClient(postgresql.DefaultOptions)
	// pqErr, ok := err.(*pq.Error)
	// expectedErrorCode := "28P01" // invalid_password, see https://www.postgresql.org/docs/11/errcodes-appendix.html
	// if !ok {
	// 	t.Errorf("Expected a pq error, but wasn't. Type: %T, error: %v", err, err)
	// } else if string(pqErr.Code) != expectedErrorCode {
	// 	t.Errorf("Expected error code %v, but was %v. Error: %v", expectedErrorCode, pqErr.Code, pqErr)
	// }
}

// TestNil tests the behaviour when passing nil or pointers to nil values to some methods.
//
// Note: This test is only executed if the initial connection to PostgreSQL works.
func TestNil(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to PostgreSQL could be established. Probably not running in a proper test environment.")
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

	createTest := func(codec encoding.Codec) func(t *testing.T) {
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
// Note: This test is only executed if the initial connection to PostgreSQL works.
func TestClose(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to PostgreSQL could be established. Probably not running in a proper test environment.")
	}

	client := createClient(t, encoding.JSON)
	defer client.Close()
	err := client.Close()
	if err != nil {
		t.Error(err)
	}
}

// checkConnection returns true if a connection could be made, false otherwise.
func checkConnection() bool {
	// Need to use port 5433 because 5432 is already used by another service on Travis CI
	db, err := sql.Open("postgres", "postgres://postgres:secret@localhost:5433/?sslmode=disable")
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

func createClient(t *testing.T, codec encoding.Codec) Client {
	options := Options{
		// Need to use port 5433 because 5432 is already used by another service on Travis CI
		ConnectionURL: "postgres://postgres:secret@localhost:5433/gokv?sslmode=disable",
		Codec:         codec,
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
