package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	table     map[string]string
	StorageDB *badger.DB
	dbpath    string
	txn       *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	standAlongStorage := StandAloneStorage{dbpath: conf.DBPath}
	return &standAlongStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.dbpath
	opts.ValueDir = s.dbpath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	s.StorageDB = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.StorageDB.Close()
	return nil
}
func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCF(s.StorageDB, cf, key)
	if err == badger.ErrKeyNotFound {
		err = nil
		value = nil
	}
	return value, err
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorage) Close() {
	s.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	s.txn = s.StorageDB.NewTransaction(false)
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, data := range batch {
		switch data.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.StorageDB, data.Cf(), data.Key(), data.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.StorageDB, data.Cf(), data.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
