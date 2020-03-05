package bg

import (
	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/storerror"
	"github.com/dgraph-io/badger"
)

func New(name string) storage.DB {
	opts := badger.DefaultOptions(name)
	opts.SyncWrites = true
	if db, err := badger.Open(opts); err != nil {
		return nil
	} else {
		return &bgStore{db}
	}
}

func (db *bgStore) Close() error {
	return db.db.Close()
}

func (db *bgStore) NewBatch() (storage.Batch, error) {
	return &bgBatch{db.db.NewTransaction(true)}, nil
}

func (db *bgStore) Del(k []byte) error {
	tx := db.db.NewTransaction(true)
	defer tx.Discard()
	if err := del(tx, k); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *bgStore) Set(k, v []byte) error {
	tx := db.db.NewTransaction(true)
	defer tx.Discard()
	if err := set(tx, k, v); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *bgStore) Get(k []byte) ([]byte, error) {
	tx := db.db.NewTransaction(false)
	defer tx.Discard()
	return get(tx, k)
}

func (tx *bgBatch) Cancel() error {
	tx.tx.Discard()
	return nil
}

func (tx *bgBatch) Commit() error {
	return tx.tx.Commit()
}

func (tx *bgBatch) Del(k []byte) error {
	return del(tx.tx, k)
}

func (tx *bgBatch) Set(k, v []byte) error {
	return set(tx.tx, k, v)
}

func del(tx *badger.Txn, k []byte) error {
	return tx.Delete(k)
}

func set(tx *badger.Txn, k, v []byte) error {
	return tx.Set(k, v)
}

func get(tx *badger.Txn, k []byte) ([]byte, error) {
	it, err := tx.Get(k)
	if err == badger.ErrKeyNotFound {
		err = storerror.NotExist
	}
	if err != nil {
		return nil, err
	}
	return it.ValueCopy(nil)
}
