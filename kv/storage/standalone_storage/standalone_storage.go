package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	en *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {

	kv_path := path.Join(conf.DBPath, "kv")
	raft_path := path.Join(conf.DBPath, "raft")

	kv_db := engine_util.CreateDB(kv_path, conf.Raft)
	raft_db := engine_util.CreateDB(raft_path, conf.Raft)

	return &StandAloneStorage{en: engine_util.NewEngines(kv_db, raft_db, kv_path, raft_path)}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.en.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	kvTxn := s.en.Kv.NewTransaction(false)
	return &StandAloneReader{kvTxn: kvTxn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			err := engine_util.PutCF(s.en.Kv, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			del := modify.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.en.Kv, del.Cf, del.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{kvTxn: kvTxn}
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.kvTxn, cf, key)
	if err != nil {
		return nil, nil
	}
	return value, nil
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.kvTxn)
}

func (r *StandAloneReader) Close() {
	r.kvTxn.Discard()
}
