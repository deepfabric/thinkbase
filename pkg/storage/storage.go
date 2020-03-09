package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/deepfabric/thinkbase/pkg/algebra/types"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/storage/storerror"
	"github.com/deepfabric/thinkbase/pkg/util/encoding"
)

func New(db DB) (*database, error) {
	r := new(database)
	v, err := db.Get(sysKey())
	switch {
	case err == nil:
		r.db = db
		r.tables = make(map[string]*table)
		if err := encoding.Decode(v, &r.ids); err != nil {
			return nil, err
		}
		for _, id := range r.ids {
			if tbl, err := r.openTable(id); err != nil {
				return nil, err
			} else {
				r.tables[id] = tbl
			}
		}
		return r, nil
	case err == storerror.NotExist:
		r.db = db
		r.tables = make(map[string]*table)
		return r, nil
	default:
		return nil, err
	}
}

func (db *database) Close() error {
	return db.db.Close()
}

func (db *database) Tables() ([]string, error) {
	return db.ids, nil
}

func (db *database) Table(id string) (Table, error) {
	if id == System {
		return nil, storerror.CannotOpenSystemTable
	}
	db.Lock()
	defer db.Unlock()
	if _, ok := db.tables[id]; !ok {
		if err := db.addTable(id); err != nil {
			return nil, err
		}
	}
	return db.tables[id], nil
}

func (tbl *table) Metadata() []string {
	tbl.RLock()
	defer tbl.RUnlock()
	return tbl.attrs
}

func (tbl *table) AddTuple(tuple map[string]interface{}) error {
	tbl.Lock()
	defer tbl.Unlock()
	attrs := tbl.updateAttributes([]map[string]interface{}{tuple})
	bat, err := tbl.db.NewBatch()
	if err != nil {
		return err
	}
	if err := tbl.addTuple(uint64(tbl.cnt), attrs, tuple, bat); err != nil {
		bat.Cancel()
		return err
	}
	return bat.Commit()
}

func (tbl *table) AddTuples(tuples []map[string]interface{}) error {
	tbl.Lock()
	defer tbl.Unlock()
	attrs := tbl.updateAttributes(tuples)
	bat, err := tbl.db.NewBatch()
	if err != nil {
		return err
	}
	for _, tuple := range tuples {
		if err := tbl.addTuple(uint64(tbl.cnt), attrs, tuple, bat); err != nil {
			bat.Cancel()
			return err
		}
	}
	return bat.Commit()
}

func (tbl *table) GetTupleCount() (int, error) {
	tbl.RLock()
	defer tbl.RUnlock()
	return int(tbl.cnt), nil
}

func (tbl *table) GetTuple(idx int, attrs []string) (value.Tuple, error) {
	tbl.RLock()
	cnt := int(tbl.cnt)
	tbl.RUnlock()
	if idx < 0 || idx >= cnt {
		return nil, errors.New("out of size")
	}
	if data, err := tbl.db.Get(rowKey(tbl.id, uint64(idx))); err != nil {
		return nil, err
	} else {
		return tbl.getTuple(data, attrs)
	}
}

func (tbl *table) GetTuples(start, end int, attrs []string) ([]value.Tuple, error) {
	var ts []value.Tuple

	tbl.RLock()
	cnt := int(tbl.cnt)
	tbl.RUnlock()
	if start < 0 {
		start = 0
	}
	if end > cnt || end < 0 {
		end = cnt
	}
	itr, err := tbl.db.NewIterator(rowPrefixKey(tbl.id))
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(rowKey(tbl.id, uint64(start)))
	for itr.Valid() {
		key := itr.Key()
		idx := int(encoding.DecodeUint64(key[len(key)-8:]))
		switch {
		case idx < start:
			continue
		case idx >= end:
			return ts, nil
		default:
			v, err := itr.Value()
			if err != nil {
				return nil, err
			}
			if t, err := tbl.getTuple(v, attrs); err != nil {
				return nil, err
			} else {
				ts = append(ts, t)
			}
		}
		itr.Next()
	}
	return ts, nil
}

// is is sorted
func (tbl *table) GetTuplesByIndex(is []int, attrs []string) ([]value.Tuple, error) {
	var ts []value.Tuple

	itr, err := tbl.db.NewIterator(rowPrefixKey(tbl.id))
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(rowKey(tbl.id, uint64(is[0])))
	for len(is) > 0 && itr.Valid() {
		key := itr.Key()
		idx := int(encoding.DecodeUint64(key[len(key)-8:]))
		switch {
		case idx < is[0]:
			break
		case idx > is[len(is)-1]:
			return ts, nil
		default:
			v, err := itr.Value()
			if err != nil {
				return nil, err
			}
			if t, err := tbl.getTuple(v, attrs); err != nil {
				return nil, err
			} else {
				ts = append(ts, t)
			}
			is = is[1:]
		}
		itr.Next()
	}
	return ts, nil

}

func (tbl *table) GetAttributeByLimit(attr string, start, end int) (value.Attribute, error) {
	var a value.Attribute

	tbl.RLock()
	mp := tbl.mp
	cnt := int(tbl.cnt)
	tbl.RUnlock()
	if _, ok := mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	if start < 0 {
		start = 0
	}
	if end > cnt || end < 0 {
		end = cnt
	}
	itr, err := tbl.db.NewIterator(colPrefixKey(tbl.id, attr))
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	curr := start
	itr.Seek(colKey(tbl.id, attr, uint64(curr)))
	for itr.Valid() {
		key := itr.Key()
		idx := int(encoding.DecodeUint64(key[len(key)-8:]))
		switch {
		case idx < start:
			break
		case idx >= end:
			for curr < end {
				curr++
				a = append(a, value.ConstNull)
			}
			return a, nil
		default:
			for curr < idx {
				curr++
				a = append(a, value.ConstNull)
			}
			v, err := itr.Value()
			if err != nil {
				return nil, err
			}
			if e, err := getElement(int(v[0]), v[1:]); err != nil {
				return nil, err
			} else {
				a = append(a, e)
			}
			curr++
		}
		itr.Next()
	}
	return a, nil
}

func (tbl *table) getTuple(data []byte, attrs []string) (value.Tuple, error) {
	v, err := getElement(int(data[0]), data[1:])
	if err != nil {
		return nil, err
	}
	t := value.Tuple(v.(value.Array))
	for i, j := len(t), len(attrs); i < j; i++ {
		t = append(t, value.ConstNull)
	}
	return t, nil
}

func (tbl *table) updateAttributes(tuples []map[string]interface{}) []string {
	attrs := tbl.attrs
	for _, tuple := range tuples {
		for attr, _ := range tuple {
			if _, ok := tbl.mp[attr]; !ok {
				attrs = append(attrs, attr)
			}
		}
	}
	return attrs
}

func (tbl *table) addTuple(row uint64, attrs []string, tuple map[string]interface{}, bat Batch) error {
	var xs []interface{}

	for _, attr := range attrs {
		if e, ok := tuple[attr]; ok {
			var v []byte
			switch t := e.(type) {
			case nil:
				xs = append(xs, value.ConstNull)
				v, _ = encoding.Encode(value.ConstNull)
				v = append([]byte{byte(types.T_null)}, v...)
			case bool:
				xs = append(xs, value.NewBool(t))
				v, _ = encoding.Encode(*value.NewBool(t))
				v = append([]byte{byte(types.T_bool)}, v...)
			case int64:
				xs = append(xs, value.NewInt(t))
				v, _ = encoding.Encode(*value.NewInt(t))
				v = append([]byte{byte(types.T_int)}, v...)
			case string:
				xs = append(xs, value.NewString(t))
				v, _ = encoding.Encode(*value.NewString(t))
				v = append([]byte{byte(types.T_string)}, v...)
			case float64:
				xs = append(xs, value.NewFloat(t))
				v, _ = encoding.Encode(*value.NewFloat(t))
				v = append([]byte{byte(types.T_float)}, v...)
			case time.Time:
				xs = append(xs, value.NewTime(t))
				v, _ = encoding.Encode(*value.NewTime(t))
				v = append([]byte{byte(types.T_time)}, v...)
			case []interface{}:
				if err := tbl.database.addTupleByArray(tbl.id+"."+attr, t, bat); err != nil {
					return err
				}
				xs = append(xs, getArray(t))
				v, _ = encoding.Encode(t)
				v = append([]byte{byte(types.T_array)}, v...)
			case map[string]interface{}:
				if err := tbl.database.addTupleBysubTable(tbl.id+"."+attr, t, bat); err != nil {
					return err
				}
				xs = append(xs, value.NewTable(tbl.id+"."+attr))
				v, _ = encoding.Encode(*value.NewTable(tbl.id + "." + attr))
				v = append([]byte{byte(types.T_table)}, v...)
			}
			if err := bat.Set(colKey(tbl.id, attr, row), v); err != nil {
				return err
			}
			if err := bat.Set(invertedKey(tbl.id, attr, string(v), row), []byte{}); err != nil {
				return err
			}
		} else {
			xs = append(xs, value.ConstNull)
		}
	}
	{
		v, _ := encoding.Encode(xs)
		if err := bat.Set(rowKey(tbl.id, row), append([]byte{byte(types.T_array)}, v...)); err != nil {
			return err
		}
	}
	{
		v, err := encoding.Encode(tbl.attrs)
		if err != nil {
			return err
		}
		if err := bat.Set(attrKey(tbl.id), v); err != nil {
			return err
		}
	}
	{
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(tbl.cnt+1))
		if err := bat.Set(countKey(tbl.id), v); err != nil {
			return err
		}
	}
	tbl.cnt++
	tbl.attrs = attrs
	for _, attr := range attrs {
		if _, ok := tbl.mp[attr]; !ok {
			tbl.mp[attr] = struct{}{}
		}
	}
	return nil
}

func (db *database) addTupleByArray(id string, xs []interface{}, bat Batch) error {
	for i, j := 0, len(xs); i < j; i++ {
		switch t := xs[i].(type) {
		case nil:
			xs[i] = value.ConstNull
		case bool:
			xs[i] = *value.NewBool(t)
		case int64:
			xs[i] = *value.NewInt(t)
		case string:
			xs[i] = *value.NewString(t)
		case float64:
			xs[i] = *value.NewFloat(t)
		case time.Time:
			xs[i] = *value.NewTime(t)
		case []interface{}:
			if err := db.addTupleByArray(id+"."+strconv.Itoa(i), t, bat); err != nil {
				return err
			}
		case map[string]interface{}:
			idx := strconv.Itoa(i)
			if err := db.addTupleBysubTable(id+"."+idx, t, bat); err != nil {
				return err
			}
			xs[i] = *value.NewTable(id + "." + idx)
		}
	}
	return nil
}

func (db *database) addTupleBysubTable(id string, tuple map[string]interface{}, bat Batch) error {
	t, err := db.Table(id)
	if err != nil {
		return err
	}
	tbl := t.(*table)
	attrs := tbl.updateAttributes([]map[string]interface{}{tuple})
	return tbl.addTuple(uint64(tbl.cnt), attrs, tuple, bat)
}

func (db *database) openTable(id string) (*table, error) {
	var tbl table

	tbl.id = id
	tbl.database = db
	tbl.mp = make(map[string]struct{})
	{
		v, err := db.db.Get(countKey(id))
		switch {
		case err == nil:
			tbl.cnt = int64(binary.BigEndian.Uint64(v))
		case err == storerror.NotExist:
		default:
			return nil, err
		}
	}
	{
		v, err := db.db.Get(attrKey(id))
		switch {
		case err == nil:
			if err := encoding.Decode(v, &tbl.attrs); err != nil {
				return nil, err
			}
			for _, attr := range tbl.attrs {
				tbl.mp[attr] = struct{}{}
			}
		case err == storerror.NotExist:
		default:
			return nil, err
		}
	}
	return &tbl, nil
}

func (db *database) addTable(id string) error {
	ids := append(db.ids, id)
	v, err := encoding.Encode(ids)
	if err != nil {
		return err
	}
	if err := db.db.Set(sysKey(), v); err != nil {
		return err
	}
	db.ids = append(db.ids, id)
	db.tables[id] = &table{database: db, cnt: 0, id: id, attrs: []string{}, mp: make(map[string]struct{})}
	return nil
}

func sysKey() []byte {
	return []byte(System)
}

func attrKey(id string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".A")
	return buf.Bytes()
}

func countKey(id string) []byte {
	return []byte(id)
}

func rowKey(id string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".R.")
	buf.Write(encoding.EncodeUint64(row))
	return buf.Bytes()
}

func rowPrefixKey(id string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".R.")
	return buf.Bytes()
}

func colKey(id, attr string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".C.")
	buf.WriteString(attr)
	buf.WriteString(".")
	buf.Write(encoding.EncodeUint64(row))
	return buf.Bytes()
}

func colPrefixKey(id, attr string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".C.")
	buf.WriteString(attr)
	buf.WriteString(".")
	return buf.Bytes()
}

func invertedKey(id, attr, value string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".S.")
	buf.WriteString(attr)
	buf.WriteString(".")
	buf.WriteString(value)
	buf.WriteString(".")
	buf.Write(encoding.EncodeUint64(row))
	return buf.Bytes()
}

func getElement(typ int, data []byte) (value.Value, error) {
	switch typ {
	case types.T_int:
		var v value.Int
		if err := encoding.Decode(data, &v); err != nil {
			return nil, err
		}
		return &v, nil
	case types.T_null:
		return value.ConstNull, nil
	case types.T_bool:
		var v value.Bool
		if err := encoding.Decode(data, &v); err != nil {
			return nil, err
		}
		return &v, nil
	case types.T_time:
		var v value.Time
		if err := encoding.Decode(data, &v); err != nil {
			return nil, err
		}
		return &v, nil
	case types.T_float:
		var v value.Float
		if err := encoding.Decode(data, &v); err != nil {
			return nil, err
		}
		return &v, nil
	case types.T_array:
		var xs []interface{}
		if err := encoding.Decode(data, &xs); err != nil {
			return nil, err
		}
		return getArray(xs), nil
	case types.T_table:
		var v value.Table
		if err := encoding.Decode(data, &v); err != nil {
			return nil, err
		}
		return &v, nil
	case types.T_string:
		var v value.String
		if err := encoding.Decode(data, &v); err != nil {
			return nil, err
		}
		return &v, nil
	}
	return nil, errors.New("unsupport type")
}

func getArray(xs []interface{}) value.Array {
	var a value.Array

	for _, x := range xs {
		switch t := x.(type) {
		case value.Int:
			a = append(a, &t)
		case value.Null:
			a = append(a, value.ConstNull)
		case value.Bool:
			a = append(a, &t)
		case value.Time:
			a = append(a, &t)
		case value.Float:
			a = append(a, &t)
		case value.Table:
			a = append(a, &t)
		case value.String:
			a = append(a, &t)
		case []interface{}:
			a = append(a, getArray(t))
		}
	}
	return a
}
