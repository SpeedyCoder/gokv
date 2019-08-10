package bbolt

import (
	"context"

	bolt "github.com/etcd-io/bbolt"

	"github.com/SpeedyCoder/gokv"
	"github.com/SpeedyCoder/gokv/encoding"
	"github.com/SpeedyCoder/gokv/internal/check"
	"github.com/SpeedyCoder/gokv/internal/ctxconv"
	"github.com/SpeedyCoder/gokv/internal/iterator"
)

// Options are the options for the bbolt store.
type Options struct {
	// Bucket name for storing the key-value pairs.
	// Optional ("default" by default).
	BucketName string
	// Path of the DB file.
	// Optional ("bbolt.db" by default).
	Path string
	// Encoding format.
	// Optional (encoding.JSON by default).
	Encoding encoding.Encoding
}

const (
	DefaultBucketName = "default"
	DefaultPath       = "bbolt.db"
	DefaultEncoding   = encoding.JSON
)

// NewStore creates a new gokv.ContextStore backed by bbolt.
func NewStore(options *Options) (gokv.Store, error) {
	s, err := NewContextStore(options)
	if err != nil {
		return nil, err
	}
	return ctxconv.ToStore(s), nil
}

// NewContextStore creates a new gokv.ContextStore backed by bbolt.
// Note: bbolt uses an exclusive write lock on the database file so it cannot be shared by multiple processes.
// So when creating multiple clients you should always use a new database file (by setting a different Path in the options).
func NewContextStore(options *Options) (gokv.ContextStore, error) {
	result := store{}

	if options == nil {
		options = &Options{}
	}

	// Set default values
	if options.BucketName == "" {
		options.BucketName = DefaultBucketName
	}
	if options.Path == "" {
		options.Path = DefaultPath
	}
	if options.Encoding == nil {
		options.Encoding = DefaultEncoding
	}

	// Open DB
	db, err := bolt.Open(options.Path, 0600, nil)
	if err != nil {
		return result, err
	}

	// Create a bucket if it doesn't exist yet.
	// In bbolt key/value pairs are stored to and read from buckets.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(options.BucketName))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return result, err
	}

	result.db = db
	result.bucketName = options.BucketName
	result.codec = options.Encoding

	return result, nil
}

type store struct {
	db         *bolt.DB
	bucketName string
	codec      encoding.Encoding
}

// Set stores the given value for the given key.
// Values are automatically marshalled to JSON or gob (depending on the configuration).
// The key must not be "" and the value must not be nil.
func (s store) Set(_ context.Context, k string, v interface{}) error {
	if err := check.KeyAndValue(k, v); err != nil {
		return err
	}

	// First turn the passed object into something that bbolt can handle
	data, err := s.codec.Marshal(v)
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Put([]byte(k), data)
	})
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
// The key must not be "" and the pointer must not be nil.
func (s store) Get(_ context.Context, k string, v interface{}) (found bool, err error) {
	if err := check.KeyAndValue(k, v); err != nil {
		return false, err
	}

	var data []byte
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		txData := b.Get([]byte(k))
		// txData is only valid during the transaction.
		// Its value must be copied to make it valid outside of the tx.
		// TODO: Benchmark if it's faster to copy + close tx,
		// or to keep the tx open until unmarshalling is done.
		if txData != nil {
			// `data = append([]byte{}, txData...)` would also work, but the following is more explicit
			data = make([]byte, len(txData))
			copy(data, txData)
		}
		return nil
	})
	if err != nil {
		return false, nil
	}

	// If no value was found return false
	if data == nil {
		return false, nil
	}

	return true, s.codec.Unmarshal(data, v)
}

// Delete deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (s store) Delete(_ context.Context, k string) error {
	if err := check.Key(k); err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Delete([]byte(k))
	})
}

func (s store) Keys(ctx context.Context) gokv.KeysIterator {
	it := iterator.New(ctx)
	go func() {
		it.Close(s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(s.bucketName))
			return b.ForEach(func(k, v []byte) error {
				return it.Write(string(k))
			})
		}))
	}()
	return it
}

// Close closes the store.
// It must be called to make sure that all open transactions finish and to release all DB resources.
func (s store) Close() error {
	return s.db.Close()
}
