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
	row := strconv.FormatInt(tbl.cnt, 10)
	if err := tbl.addTuple(row, attrs, tuple, bat); err != nil {
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
	row := strconv.FormatInt(tbl.cnt, 10)
	for _, tuple := range tuples {
		if err := tbl.addTuple(row, attrs, tuple, bat); err != nil {
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
	return tbl.getTuple(idx, attrs)
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
	for i := start; i < end; i++ {
		if t, err := tbl.getTuple(i, attrs); err != nil {
			return nil, err
		} else {
			ts = append(ts, t)
		}
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
	for i := start; i < end; i++ {
		v, err := tbl.db.Get(colKey(tbl.id, strconv.Itoa(i), attr))
		switch {
		case err == nil:
			if e, err := getElement(int(v[0]), v[1:]); err != nil {
				return nil, err
			} else {
				a = append(a, e)
			}
		case err == storerror.NotExist:
			a = append(a, value.ConstNull)
		default:
			return nil, err
		}
	}
	return a, nil
}

func (tbl *table) getTuple(idx int, attrs []string) (value.Tuple, error) {
	var t value.Tuple

	row := strconv.Itoa(idx)
	for _, attr := range attrs {
		v, err := tbl.db.Get(rowKey(tbl.id, row, attr))
		switch {
		case err == nil:
			if e, err := getElement(int(v[0]), v[1:]); err != nil {
				return nil, err
			} else {
				t = append(t, e)
			}
		case err == storerror.NotExist:
			t = append(t, value.ConstNull)
		default:
			return nil, err
		}
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

func (tbl *table) addTuple(row string, attrs []string, tuple map[string]interface{}, bat Batch) error {
	for _, attr := range attrs {
		if e, ok := tuple[attr]; ok {
			var v []byte
			switch t := e.(type) {
			case nil:
				v, _ = encoding.Encode(value.ConstNull)
				v = append([]byte{byte(types.T_null)}, v...)
			case bool:
				v, _ = encoding.Encode(*value.NewBool(t))
				v = append([]byte{byte(types.T_bool)}, v...)
			case int64:
				v, _ = encoding.Encode(*value.NewInt(t))
				v = append([]byte{byte(types.T_int)}, v...)
			case string:
				v, _ = encoding.Encode(*value.NewString(t))
				v = append([]byte{byte(types.T_string)}, v...)
			case float64:
				v, _ = encoding.Encode(*value.NewFloat(t))
				v = append([]byte{byte(types.T_float)}, v...)
			case time.Time:
				v, _ = encoding.Encode(*value.NewTime(t))
				v = append([]byte{byte(types.T_time)}, v...)
			case []interface{}:
				if err := tbl.database.addTupleByArray(tbl.id+"."+attr, t, bat); err != nil {
					return err
				}
				v, _ = encoding.Encode(t)
				v = append([]byte{byte(types.T_array)}, v...)
			case map[string]interface{}:
				if err := tbl.database.addTupleBysubTable(tbl.id+"."+attr, t, bat); err != nil {
					return err
				}
				v, _ = encoding.Encode(*value.NewTable(tbl.id + "." + attr))
				v = append([]byte{byte(types.T_table)}, v...)
			}
			if err := bat.Set(rowKey(tbl.id, row, attr), v); err != nil {
				return err
			}
			if err := bat.Set(colKey(tbl.id, row, attr), v); err != nil {
				return err
			}
			if err := bat.Set(invertedKey(tbl.id, row, attr, string(v)), []byte{}); err != nil {
				return err
			}
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
	row := strconv.FormatInt(tbl.cnt, 10)
	return tbl.addTuple(row, attrs, tuple, bat)
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

func rowKey(id, row, attr string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".R.")
	buf.WriteString(row)
	buf.WriteString(".")
	buf.WriteString(attr)
	return buf.Bytes()
}

func colKey(id, row, attr string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".C.")
	buf.WriteString(attr)
	buf.WriteString(".")
	buf.WriteString(row)
	return buf.Bytes()
}

func invertedKey(id, row, attr, value string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString(".S.")
	buf.WriteString(attr)
	buf.WriteString(".")
	buf.WriteString(value)
	buf.WriteString(".")
	buf.WriteString(row)
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
