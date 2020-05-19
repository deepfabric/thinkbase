package store

import (
	"bytes"
	"os"

	"github.com/deepfabric/thinkbase/pkg/vm/container"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkkv/pkg/engine"
	"github.com/deepfabric/thinkkv/pkg/engine/pb"
)

func New(name string) *store {
	return &store{
		name: name,
		db:   pb.New(name, nil, false, false),
	}
}

func (s *store) Sync() error {
	return s.db.Sync()
}

func (s *store) Destroy() error {
	s.db.Close()
	return os.RemoveAll(s.name)
}

func (s *store) Del(k []byte) error {
	return s.db.Del(k)
}

func (s *store) Set(k, v []byte) error {
	return s.db.Set(k, v)
}

func (s *store) Get(k []byte) ([]byte, error) {
	v, err := s.db.Get(k)
	if err == engine.NotExist {
		err = container.NotExist
	}
	return v, err
}

func (s *store) GetOrSet(k, v []byte) (bool, error) {
	v, err := s.db.Get(k)
	switch {
	case err == nil:
		return true, nil
	case err != nil && err != engine.NotExist:
		return false, err
	}
	return false, s.db.Set(k, v)
}

func (s *store) NewIterator(prefix []byte) (Iterator, error) {
	itr, err := s.db.NewIterator(prefix)
	if err != nil {
		return nil, err
	}
	return &iterator{itr}, nil
}

func (itr *iterator) Close() error {
	itr.itr.Close()
	return nil
}

func (itr *iterator) Next() error {
	itr.itr.Next()
	return nil
}

func (itr *iterator) Valid() bool {
	return itr.itr.Valid()
}

func (itr *iterator) Seek(k []byte) error {
	itr.itr.Seek(k)
	return nil
}

func (itr *iterator) Key() []byte {
	return itr.itr.Key()
}

func (itr *iterator) Value() ([]byte, error) {
	return itr.itr.Value()
}

func (s *store) Lkey() ([]byte, error) {
	itr, err := s.db.NewIterator([]byte{'l', 'm'})
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek([]byte{'l', 'm'})
	for itr.Valid() {
		return dListMetaKey(itr.Key()), nil
		itr.Next()
	}
	return nil, nil
}

func (s *store) Llen(k []byte) (uint64, error) {
	start, end, err := listStartEnd(s.db, k)
	if err != nil {
		return 0, err
	}
	return end - start, nil
}

func (s *store) Lhead(k []byte) ([]byte, error) {
	start, end, err := listStartEnd(s.db, k)
	if err != nil {
		return nil, err
	}
	if start == end {
		return nil, container.Empty
	}
	return s.db.Get(eListKey(k, start))
}

func (s *store) Lpop(k []byte) ([]byte, error) {
	start, end, err := listStartEnd(s.db, k)
	if err != nil {
		return nil, err
	}
	if start == end {
		return nil, container.Empty
	}
	v, err := s.db.Get(eListKey(k, start))
	if err != nil {
		return nil, err
	}
	switch {
	case start+1 == end:
		if err := s.db.Del(eListMetaKey(k)); err != nil {
			return nil, err
		}
	default:
		if err := s.db.Set(eListMetaKey(k), eListMetaValue(start+1, end)); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func (s *store) Lpops(k []byte, limit int) ([][]byte, error) {
	start, end, err := listStartEnd(s.db, k)
	if err != nil {
		return nil, err
	}
	if start == end {
		return nil, container.Empty
	}
	var rs [][]byte
	itr, err := s.db.NewIterator(eListPrefixKey(k))
	if err != nil {
		return nil, err
	}
	itr.Seek(eListKey(k, start))
	for size := 0; itr.Valid() && size < limit; start++ {
		v, err := itr.Value()
		if err != nil {
			return nil, err
		}
		size += len(v)
		rs = append(rs, v)
		itr.Next()
	}
	switch {
	case start == end:
		if err := s.db.Del(eListMetaKey(k)); err != nil {
			return nil, err
		}
	default:
		if err := s.db.Set(eListMetaKey(k), eListMetaValue(start, end)); err != nil {
			return nil, err
		}
	}
	return rs, nil
}

func (s *store) Lpush(k, v []byte) error {
	bat, err := s.db.NewBatch()
	if err != nil {
		return err
	}
	start, end, err := listStartEnd(s.db, k)
	switch {
	case err != nil:
		bat.Cancel()
		return err
	case start == end+1:
		bat.Cancel()
		return container.OutOfSize
	default:
		if err := bat.Set(eListKey(k, end), v); err != nil {
			bat.Cancel()
			return err
		}
		if err := bat.Set(eListMetaKey(k), eListMetaValue(start, end+1)); err != nil {
			bat.Cancel()
			return err
		}
		return bat.Commit()
	}
	return nil
}

func listStartEnd(db engine.DB, k []byte) (uint64, uint64, error) {
	v, err := db.Get(eListMetaKey(k))
	switch {
	case err == nil:
		start, end := dListMetaValue(v)
		return start, end, nil
	case err == engine.NotExist:
		return 0, 0, nil
	}
	return 0, 0, err
}

func eListPrefixKey(k []byte) []byte {
	var buf bytes.Buffer

	buf.WriteByte('l')
	buf.Write(k)
	return buf.Bytes()
}

// 'l' + k + index
func eListKey(k []byte, idx uint64) []byte {
	var buf bytes.Buffer

	buf.WriteByte('l')
	buf.Write(k)
	buf.Write(encoding.EncodeUint64(idx))
	return buf.Bytes()
}

func dListKey(buf []byte) []byte {
	n := len(buf)
	return buf[1 : n-8]
}

// 'lm' + k
func eListMetaKey(k []byte) []byte {
	return append([]byte{'l', 'm'}, k...)
}

func dListMetaKey(buf []byte) []byte {
	return buf[2:]
}

// start + end
func eListMetaValue(start, end uint64) []byte {
	return append(encoding.EncodeUint64(start), encoding.EncodeUint64(end)...)
}

func dListMetaValue(buf []byte) (uint64, uint64) {
	return encoding.DecodeUint64(buf[:8]), encoding.DecodeUint64(buf[8:16])
}
