package storage

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/deepfabric/thinkbase/pkg/storage/cache/bitmap"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/rangebitmap"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/relation"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/srangebitmap"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/textranging"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary"
	vrelation "github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
	"github.com/deepfabric/thinkkv/pkg/engine"
)

func New(db engine.DB, bc bitmap.Cache, rc relation.Cache, rbc rangebitmap.Cache, srbc srangebitmap.Cache) *storage {
	return &storage{
		db:   db,
		bc:   bc,
		rc:   rc,
		rbc:  rbc,
		srbc: srbc,
	}
}

func (s *storage) Close() error {
	return s.db.Sync()
}

func (s *storage) Relation(id string) (vrelation.Relation, error) {
	if r, ok := s.rc.Get(id); ok {
		t := r.(*table)
		return &table{
			s:     s,
			id:    t.id,
			db:    t.db,
			mp:    t.mp,
			name:  t.name,
			rows:  t.rows,
			size:  t.size,
			attrs: t.attrs,
		}, nil
	}
	r, err := s.getRelation(id)
	if err == nil {
		s.rc.Set(id, r)
	}
	return r, err
}

func (r *table) Destroy() error {
	return r.db.Sync()
}

func (r *table) Size() float64 {
	return float64(r.size)
}

func (r *table) Cost() float64 {
	return float64(r.size)
}

func (r *table) Operate() int {
	return op.Relation
}

func (r *table) Dup() op.OP {
	var attrs []string

	mp := make(map[string]struct{})
	for _, attr := range r.attrs {
		mp[attr] = struct{}{}
		attrs = append(attrs, attr)
	}
	return &table{
		mp:    mp,
		s:     r.s,
		db:    r.db,
		id:    r.id,
		attrs: attrs,
		name:  r.name,
		rows:  r.rows,
		size:  r.size,
	}
}

func (r *table) Children() []op.OP {
	return nil
}

func (r *table) SetChild(_ op.OP, _ int) {}
func (r *table) IsOrdered() bool         { return false }

func (r *table) DataString() string {
	return r.name
}

func (r *table) AddTuples(_ []value.Array) error {
	return errors.New("not support now")
}

func (r *table) Id() string {
	return r.id
}

func (r *table) Rows() uint64 {
	return r.rows
}

func (r *table) String() string {
	return r.name
}

func (r *table) Name() (string, error) {
	return r.name, nil
}

func (r *table) AttributeList() ([]string, error) {
	return r.attrs, nil
}

func (r *table) StrMin(attr string, row uint64, fp *roaring.Bitmap) (string, error) {
	r.RLock()
	defer r.RUnlock()
	prefix := bsPrefixKey(r.id, attr, row)
	itr, err := r.db.NewIterator(prefix)
	if err != nil {
		return "", err
	}
	defer itr.Close()
	itr.Seek(prefix)
	for itr.Valid() {
		v, err := itr.Value()
		if err != nil {
			return "", err
		}
		mp := roaring.NewBitmap()
		if err := mp.Read(v); err != nil {
			return "", err
		}
		if mp.Intersect(fp).Count() != 0 {
			return string(itr.Key()[len(prefix):]), nil
		}
		itr.Next()
	}
	return "", NotExist
}

func (r *table) StrMax(attr string, row uint64, fp *roaring.Bitmap) (string, error) {
	var s string

	r.RLock()
	defer r.RUnlock()
	flg := true
	prefix := bsPrefixKey(r.id, attr, row)
	itr, err := r.db.NewIterator(prefix)
	if err != nil {
		return "", err
	}
	defer itr.Close()
	itr.Seek(prefix)
	for itr.Valid() {
		v, err := itr.Value()
		if err != nil {
			return "", err
		}
		mp := roaring.NewBitmap()
		if err := mp.Read(v); err != nil {
			return "", err
		}
		if mp.Intersect(fp).Count() != 0 {
			k := string(itr.Key()[len(prefix):])
			if flg || strings.Compare(k, s) > 0 {
				s = k
				flg = false
			}
		}
		itr.Next()
	}
	if flg {
		return "", NotExist
	}
	return s, nil
}

func (r *table) StrCount(attr string, row uint64, fp *roaring.Bitmap) (uint64, error) {
	var count uint64

	prefix := bsPrefixKey(r.id, attr, row)
	itr, err := r.db.NewIterator(prefix)
	if err != nil {
		return 0, err
	}
	defer itr.Close()
	itr.Seek(prefix)
	for itr.Valid() {
		v, err := itr.Value()
		if err != nil {
			return 0, err
		}
		mp := roaring.NewBitmap()
		if err := mp.Read(v); err != nil {
			return 0, err
		}
		count += mp.Intersect(fp).Count()
		itr.Next()
	}
	return count, nil
}

func (r *table) NullCount(attr string, row uint64, fp *roaring.Bitmap) (uint64, error) {
	r.RLock()
	defer r.RUnlock()
	mp, err := r.s.getBitmap(string(bnKey(r.id, attr, row)))
	if err != nil {
		return 0, err
	}
	return mp.Intersect(fp).Count(), nil
}

func (r *table) BoolCount(attr string, row uint64, fp *roaring.Bitmap) (uint64, error) {
	r.RLock()
	defer r.RUnlock()
	mp, err := r.s.getBitmap(string(bbKey(r.id, attr, true, row)))
	if err != nil {
		return 0, err
	}
	mq, err := r.s.getBitmap(string(bbKey(r.id, attr, false, row)))
	if err != nil {
		return 0, err
	}
	return mp.Intersect(fp).Count() + mq.Intersect(fp).Count(), nil
}

func (r *table) IntBitmapFold(attr string, row uint64, dict mdictionary.Mdictionary) error {
	r.RLock()
	defer r.RUnlock()
	rp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
	if err != nil {
		return err
	}
	return rp.Fold(ranging.Int, dict)
}

func (r *table) TimeBitmapFold(attr string, row uint64, dict mdictionary.Mdictionary) error {
	r.RLock()
	defer r.RUnlock()
	rp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
	if err != nil {
		return err
	}
	return rp.Fold(ranging.Time, dict)
}

func (r *table) FloatBitmapFold(attr string, row uint64, dict mdictionary.Mdictionary) error {
	r.RLock()
	defer r.RUnlock()
	rp, err := r.s.getRangebitmap(string(rbfKey(r.id, attr, row)))
	if err != nil {
		return err
	}
	return rp.Fold(ranging.Float, dict)
}

func (r *table) NullBitmapFold(attr string, row uint64, dict mdictionary.Mdictionary) error {
	r.RLock()
	defer r.RUnlock()
	mp, err := r.s.getBitmap(string(bnKey(r.id, attr, row)))
	if err != nil {
		return err
	}
	k, _ := encoding.EncodeValue(value.ConstNull)
	return dict.Set(string(k), mp)
}

func (r *table) BoolBitmapFold(attr string, row uint64, dict mdictionary.Mdictionary) error {
	r.RLock()
	defer r.RUnlock()
	{ // true
		mp, err := r.s.getBitmap(string(bbKey(r.id, attr, true, row)))
		if err != nil {
			return err
		}
		k, _ := encoding.EncodeValue(value.NewBool(true))
		if err := dict.Set(string(k), mp); err != nil {
			return err
		}
	}
	mp, err := r.s.getBitmap(string(bbKey(r.id, attr, false, row)))
	if err != nil {
		return err
	}
	k, _ := encoding.EncodeValue(value.NewBool(false))
	return dict.Set(string(k), mp)
}

func (r *table) StringBitmapFold(attr string, row uint64, dict mdictionary.Mdictionary) error {
	r.RLock()
	defer r.RUnlock()
	prefix := bsPrefixKey(r.id, attr, row)
	itr, err := r.db.NewIterator(prefix)
	if err != nil {
		return err
	}
	defer itr.Close()
	itr.Seek(prefix)
	for itr.Valid() {
		s := string(itr.Key()[len(prefix):])
		v, err := itr.Value()
		if err != nil {
			return err
		}
		mp := roaring.NewBitmap()
		if err := mp.Read(v); err != nil {
			return err
		}
		k, err := encoding.EncodeValue(value.NewString(s))
		if err != nil {
			return err
		}
		dict.Set(string(k), mp)
		itr.Next()
	}
	return nil
}

func (r *table) IntRangeBitmap(attr string, row uint64) (*ranging.Ranging, error) {
	r.RLock()
	defer r.RUnlock()
	return r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
}

func (r *table) TimeRangeBitmap(attr string, row uint64) (*ranging.Ranging, error) {
	r.RLock()
	defer r.RUnlock()
	return r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
}

func (r *table) FloatRangeBitmap(attr string, row uint64) (*ranging.Ranging, error) {
	r.RLock()
	defer r.RUnlock()
	return r.s.getRangebitmap(string(rbfKey(r.id, attr, row)))
}

func (r *table) Eq(attr string, v value.Value, row uint64) (*roaring.Bitmap, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	switch v.ResolvedType().Oid {
	case types.T_int:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Eq(value.MustBeInt(v))
	case types.T_null:
		mp, err := r.s.getBitmap(string(bnKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp, nil
	case types.T_bool:
		mp, err := r.s.getBitmap(string(bbKey(r.id, attr, value.MustBeBool(v), row)))
		if err != nil {
			return nil, err
		}
		return mp, nil
	case types.T_time:
		mp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Eq(value.MustBeTime(v).Unix())
	case types.T_float:
		mp, err := r.s.getRangebitmap(string(rbfKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Eq(int64(value.MustBeFloat(v) * Scale))
	case types.T_string:
		mp, err := r.s.getBitmap(string(bsKey(r.id, attr, value.MustBeString(v), row)))
		if err != nil {
			return nil, err
		}
		return mp, nil
	}
	return nil, fmt.Errorf("unsupport type '%s'", v.ResolvedType())
}

func (r *table) Ne(attr string, v value.Value, row uint64) (*roaring.Bitmap, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	switch v.ResolvedType().Oid {
	case types.T_int:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Ne(value.MustBeInt(v))
	case types.T_null:
		mp, err := r.s.getBitmap(string(bnKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		if row/Segment == r.rows/Segment {
			return mp.Flip(0, r.rows%Segment), nil
		} else {
			return mp.Flip(0, Segment), nil
		}
	case types.T_bool:
		mp, err := r.s.getBitmap(string(bbKey(r.id, attr, !value.MustBeBool(v), row)))
		if err != nil {
			return nil, err
		}
		return mp, nil
	case types.T_time:
		mp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Ne(value.MustBeTime(v).Unix())
	case types.T_float:
		mp, err := r.s.getRangebitmap(string(rbfKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Ne(int64(value.MustBeFloat(v) * Scale))
	case types.T_string:
		return stringNe(r.db, r.id, attr, value.MustBeString(v), row)
	}
	return nil, fmt.Errorf("unsupport type '%s'", v.ResolvedType())
}

func (r *table) Lt(attr string, v value.Value, row uint64) (*roaring.Bitmap, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	switch v.ResolvedType().Oid {
	case types.T_int:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Lt(value.MustBeInt(v))
	case types.T_time:
		mp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Lt(value.MustBeTime(v).Unix())
	case types.T_float:
		mp, err := r.s.getRangebitmap(string(rbfKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Lt(int64(value.MustBeFloat(v) * Scale))
	case types.T_string:
		return stringLt(r.db, r.id, attr, value.MustBeString(v), row)
	}
	return nil, fmt.Errorf("unsupport type '%s'", v.ResolvedType())
}

func (r *table) Le(attr string, v value.Value, row uint64) (*roaring.Bitmap, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	switch v.ResolvedType().Oid {
	case types.T_int:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Le(value.MustBeInt(v))
	case types.T_time:
		mp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Le(value.MustBeTime(v).Unix())
	case types.T_float:
		mp, err := r.s.getRangebitmap(string(rbfKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Le(int64(value.MustBeFloat(v) * Scale))
	case types.T_string:
		return stringLe(r.db, r.id, attr, value.MustBeString(v), row)
	}
	return nil, fmt.Errorf("unsupport type '%s'", v.ResolvedType())
}

func (r *table) Gt(attr string, v value.Value, row uint64) (*roaring.Bitmap, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	switch v.ResolvedType().Oid {
	case types.T_int:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Gt(value.MustBeInt(v))
	case types.T_time:
		mp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Gt(value.MustBeTime(v).Unix())
	case types.T_float:
		mp, err := r.s.getRangebitmap(string(rbfKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Gt(int64(value.MustBeFloat(v) * Scale))
	case types.T_string:
		return stringGt(r.db, r.id, attr, value.MustBeString(v), row)
	}
	return nil, fmt.Errorf("unsupport type '%s'", v.ResolvedType())
}

func (r *table) Ge(attr string, v value.Value, row uint64) (*roaring.Bitmap, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	switch v.ResolvedType().Oid {
	case types.T_int:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Ge(value.MustBeInt(v))
	case types.T_time:
		mp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Ge(value.MustBeTime(v).Unix())
	case types.T_float:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Ge(int64(value.MustBeFloat(v) * Scale))
	case types.T_string:
		return stringGe(r.db, r.id, attr, value.MustBeString(v), row)
	}
	return nil, fmt.Errorf("unsupport type '%s'", v.ResolvedType())
}

func (r *table) Between(attr string, x, y value.Value, row uint64) (*roaring.Bitmap, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	switch x.ResolvedType().Oid {
	case types.T_int:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Between(value.MustBeInt(x), value.MustBeInt(y))
	case types.T_time:
		mp, err := r.s.getRangebitmap(string(rbtKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Between(value.MustBeTime(x).Unix(), value.MustBeTime(y).Unix())
	case types.T_float:
		mp, err := r.s.getRangebitmap(string(rbiKey(r.id, attr, row)))
		if err != nil {
			return nil, err
		}
		return mp.Between(int64(value.MustBeFloat(x)*Scale), int64(value.MustBeFloat(y)*Scale))
	case types.T_string:
		return stringBetween(r.db, r.id, attr, value.MustBeString(x), value.MustBeString(y), row)
	}
	return nil, fmt.Errorf("unsupport type '%s'", x.ResolvedType())
}

func (r *table) AddTuplesByJson(ts []map[string]interface{}) error {
	if len(ts) == 0 {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	b, err := r.db.NewBatch()
	if err != nil {
		return err
	}
	rows := r.rows
	attrs := r.updateAttributes(ts)
	{
		data, err := encoding.Encode(attrs)
		if err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
		if err := b.Set(attrKey(r.id), data); err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
	}
	{
		if err := b.Set(rowsKey(r.id), encoding.EncodeUint64(r.rows+uint64(len(ts)))); err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
	}
	bmp := make(map[string]*roaring.Bitmap)
	rmp := make(map[string]*ranging.Ranging)
	srmp := make(map[string]*textranging.Ranging)
	for _, t := range ts {
		if err := r.addTuple(t, b, bmp, rmp, srmp); err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
	}
	for k, mp := range bmp {
		v, err := mp.Show()
		if err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
		if err := b.Set([]byte(k), v); err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
	}
	for k, mp := range rmp {
		v, err := mp.Show()
		if err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
		if err := b.Set([]byte(k), v); err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
	}
	for k, mp := range srmp {
		v, err := mp.Show()
		if err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
		if err := b.Set([]byte(k), v); err != nil {
			b.Cancel()
			r.rows = rows
			return err
		}
	}
	if err := b.Commit(); err != nil {
		b.Cancel()
		r.rows = rows
		return err
	}
	if err := r.db.Sync(); err != nil {
		r.rows = rows
		return err
	}
	r.attrs = attrs
	for _, attr := range r.attrs {
		r.mp[attr] = struct{}{}
	}
	for k, mp := range bmp {
		r.s.bc.Set(k, mp) // update cache
	}
	for k, mp := range rmp {
		r.s.rbc.Set(k, mp) // update cache
	}
	for k, mp := range srmp {
		r.s.srbc.Set(k, mp) // update cache
	}
	r.s.rc.Set(r.id, r)
	return nil
}

func (r *table) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if r.pos >= r.rows {
		return nil, nil
	}
	r.RLock()
	defer r.RUnlock()
	pos := r.pos
	for i, j := 0, len(attrs); i < j; i++ {
		a, err := r.getAttributeByLimit(attrs[i], limit/len(attrs))
		if err != nil {
			return nil, err
		}
		if len(a) > 0 {
			mp := make(map[string]value.Array)
			for k := 0; k < i; k++ {
				mp[attrs[k]] = fill(len(a))
			}
			mp[attrs[i]] = a
			for ; i < j; i++ {
				a, err = r.getAttributeByRange(attrs[i], pos, r.pos)
				if err != nil {
					return nil, err
				}
				mp[attrs[i]] = a
			}
			return mp, nil
		}
	}
	mp := make(map[string]value.Array)
	for i, j := 0, len(attrs); i < j; i++ {
		mp[attrs[i]] = fill(int(r.rows-r.pos) + 1)
	}
	r.pos = r.rows
	return mp, nil
}

func (r *table) GetAttributesByIndex(attrs []string, is []uint64, limit int) (map[string]value.Array, error) {
	if len(is) == 0 {
		return nil, nil
	}
	r.RLock()
	defer r.RUnlock()
	for i, j := 0, len(attrs); i < j; i++ {
		a, err := r.getAttributeByIndexAndLimit(attrs[i], is, limit/len(attrs))
		if err != nil {
			return nil, err
		}
		if len(a) > 0 {
			mp := make(map[string]value.Array)
			for k := 0; k < i; k++ {
				mp[attrs[k]] = fill(len(a))
			}
			mp[attrs[i]] = a
			for ; i < j; i++ {
				a, err = r.getAttributeByIndex(attrs[i], is[:len(a)])
				if err != nil {
					return nil, err
				}
				mp[attrs[i]] = a
			}
			return mp, nil
		}
	}
	mp := make(map[string]value.Array)
	for i, j := 0, len(attrs); i < j; i++ {
		mp[attrs[i]] = fill(len(is))
	}
	return mp, nil
}

func (r *table) getAttributeByLimit(attr string, limit int) (value.Array, error) {
	var size int
	var a value.Array

	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	itr, err := r.db.NewIterator(colPrefixKey(r.id, attr))
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(colKey(r.id, attr, r.pos))
	for itr.Valid() {
		key := itr.Key()
		row := encoding.DecodeUint64(key[len(key)-8:])
		switch {
		case row < r.pos: // skip
		default:
			for r.pos < row {
				r.pos++
				a = append(a, value.ConstEmpty)
				if size = size + 1; size > limit {
					return a, nil
				}
			}
			v, err := itr.Value()
			if err != nil {
				return nil, err
			}
			if e, _, err := encoding.DecodeValue(v); err != nil {
				return nil, err
			} else {
				r.pos++
				if t, ok := e.(value.Value); ok {
					a = append(a, t)
					if size = size + t.Size(); size > limit {
						return a, nil
					}
				}
			}
		}
		itr.Next()
	}
	return a, nil
}

func (r *table) getAttributeByRange(attr string, start, end uint64) (value.Array, error) {
	var a value.Array

	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	itr, err := r.db.NewIterator(colPrefixKey(r.id, attr))
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	pos := start
	itr.Seek(colKey(r.id, attr, pos))
	for itr.Valid() {
		key := itr.Key()
		row := encoding.DecodeUint64(key[len(key)-8:])
		switch {
		case row < start: // skip
		case row >= end:
			for pos < end {
				pos++
				a = append(a, value.ConstEmpty)
			}
			return a, nil
		default:
			for pos < row {
				pos++
				a = append(a, value.ConstEmpty)
			}
			v, err := itr.Value()
			if err != nil {
				return nil, err
			}
			if e, _, err := encoding.DecodeValue(v); err != nil {
				return nil, err
			} else {
				if t, ok := e.(value.Value); ok {
					a = append(a, t)
				}
			}
			pos++
		}
		itr.Next()
	}
	for pos < end {
		pos++
		a = append(a, value.ConstEmpty)
	}
	return a, nil
}

func (r *table) getAttributeByIndex(attr string, is []uint64) (value.Array, error) {
	var a value.Array

	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	itr, err := r.db.NewIterator(colPrefixKey(r.id, attr))
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(colKey(r.id, attr, is[0]))
	for len(is) > 0 && itr.Valid() {
		key := itr.Key()
		row := encoding.DecodeUint64(key[len(key)-8:])
		switch {
		case row < is[0]: // skip
		case row > is[len(is)-1]:
			for len(is) > 0 {
				is = is[1:]
				a = append(a, value.ConstEmpty)
			}
			return a, nil
		default:
			for len(is) > 0 && row > is[0] {
				is = is[1:]
				a = append(a, value.ConstEmpty)
			}
			if row == is[0] {
				v, err := itr.Value()
				if err != nil {
					return nil, err
				}
				if e, _, err := encoding.DecodeValue(v); err != nil {
					return nil, err
				} else {
					if t, ok := e.(value.Value); ok {
						a = append(a, t)
					}
				}
				is = is[1:]
			}
		}
		itr.Next()
	}
	for len(is) > 0 {
		is = is[1:]
		a = append(a, value.ConstEmpty)
	}
	return a, nil
}

func (r *table) getAttributeByIndexAndLimit(attr string, is []uint64, limit int) (value.Array, error) {
	var size int
	var a value.Array

	if _, ok := r.mp[attr]; !ok {
		return nil, fmt.Errorf("attribute '%s' not exist", attr)
	}
	itr, err := r.db.NewIterator(colPrefixKey(r.id, attr))
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(colKey(r.id, attr, is[0]))
	for len(is) > 0 && itr.Valid() {
		key := itr.Key()
		row := encoding.DecodeUint64(key[len(key)-8:])
		switch {
		case row < is[0]: // skip
		case row > is[len(is)-1]:
			for len(is) > 0 {
				is = is[1:]
				a = append(a, value.ConstEmpty)
				if size = size + 1; size > limit {
					return a, nil
				}
			}
			return a, nil
		default:
			for len(is) > 0 && row > is[0] {
				is = is[1:]
				a = append(a, value.ConstEmpty)
				if size = size + 1; size > limit {
					return a, nil
				}
			}
			if row == is[0] {
				v, err := itr.Value()
				if err != nil {
					return nil, err
				}
				if e, _, err := encoding.DecodeValue(v); err != nil {
					return nil, err
				} else {
					if t, ok := e.(value.Value); ok {
						a = append(a, t)
						if size = size + t.Size(); size > limit {
							return a, nil
						}
					}
				}
				is = is[1:]
			}
		}
		itr.Next()
	}
	for len(is) > 0 {
		is = is[1:]
		a = append(a, value.ConstEmpty)
		if size = size + 1; size > limit {
			return a, nil
		}
	}
	return a, nil
}

func (r *table) updateAttributes(ts []map[string]interface{}) []string {
	var attrs []string

	mp := make(map[string]struct{})
	for _, attr := range r.attrs {
		mp[attr] = struct{}{}
		attrs = append(attrs, attr)
	}
	for _, t := range ts {
		for attr, _ := range t {
			if _, ok := mp[attr]; !ok {
				mp[attr] = struct{}{}
				attrs = append(attrs, attr)
			}
		}
	}
	return attrs
}

func (r *table) addTuple(mp map[string]interface{}, b engine.Batch, bmp map[string]*roaring.Bitmap, rmp map[string]*ranging.Ranging, srmp map[string]*textranging.Ranging) error {
	for attr, e := range mp {
		var data []byte
		switch t := e.(type) {
		case nil:
			{
				id := bnKey(r.id, attr, r.rows)
				mp, ok := bmp[string(id)]
				if !ok {
					var err error
					if mp, err = r.s.getBitmap(string(id)); err != nil {
						return err
					}
					mp = mp.Clone()
					bmp[string(id)] = mp
				}
				mp.Add(r.rows % Segment)
			}
			data, _ = encoding.EncodeValue(value.ConstNull)
		case bool:
			{
				id := bbKey(r.id, attr, t, r.rows)
				mp, ok := bmp[string(id)]
				if !ok {
					var err error
					if mp, err = r.s.getBitmap(string(id)); err != nil {
						return err
					}
					mp = mp.Clone()
					bmp[string(id)] = mp
				}
				mp.Add(r.rows % Segment)
			}
			data, _ = encoding.EncodeValue(value.NewBool(t))
		case int64:
			{
				id := rbiKey(r.id, attr, r.rows)
				mp, ok := rmp[string(id)]
				if !ok {
					var err error
					if mp, err = r.s.getRangebitmap(string(id)); err != nil {
						return err
					}
					mp = mp.Clone()
					rmp[string(id)] = mp
				}
				if err := mp.Set(r.rows%Segment, t); err != nil {
					return err
				}
			}
			data, _ = encoding.EncodeValue(value.NewInt(t))
		case string:
			id := bsKey(r.id, attr, t, r.rows)
			mp, ok := bmp[string(id)]
			if !ok {
				var err error
				if mp, err = r.s.getBitmap(string(id)); err != nil {
					return err
				}
				mp = mp.Clone()
				bmp[string(id)] = mp
			}
			mp.Add(r.rows % Segment)
			data, _ = encoding.EncodeValue(value.NewString(t))
		case float64:
			{
				v := int64(t * Scale)
				id := rbfKey(r.id, attr, r.rows)
				mp, ok := rmp[string(id)]
				if !ok {
					var err error
					if mp, err = r.s.getRangebitmap(string(id)); err != nil {
						return err
					}
					mp = mp.Clone()
					rmp[string(id)] = mp
				}
				if err := mp.Set(r.rows%Segment, v); err != nil {
					return err
				}
			}
			data, _ = encoding.EncodeValue(value.NewFloat(t))
		case time.Time:
			{
				id := rbtKey(r.id, attr, r.rows)
				mp, ok := rmp[string(id)]
				if !ok {
					var err error
					if mp, err = r.s.getRangebitmap(string(id)); err != nil {
						return err
					}
					mp = mp.Clone()
					rmp[string(id)] = mp
				}
				if err := mp.Set(r.rows%Segment, t.Unix()); err != nil {
					return err
				}
			}
			data, _ = encoding.EncodeValue(value.NewTime(t))
		case []interface{}:
			if err := r.s.addTupleByArray(r.id+"."+attr, t, b, bmp, rmp, srmp); err != nil {
				return err
			}
			var xs value.Array
			for i, _ := range t {
				xs = append(xs, value.NewTable(r.id+"."+attr+"._"+strconv.Itoa(i)))
			}
			data, _ = encoding.EncodeValue(xs)
		case map[string]interface{}:
			if err := r.s.addTupleByTable(r.id+"."+attr, t, b, bmp, rmp, srmp); err != nil {
				return err
			}
			data, _ = encoding.EncodeValue(value.NewTable(r.id + "." + attr))
		}
		if err := b.Set(colKey(r.id, attr, r.rows), data); err != nil {
			return err
		}
	}
	r.rows++
	return nil
}

func (s *storage) addTupleByArray(id string, xs []interface{}, b engine.Batch, bmp map[string]*roaring.Bitmap, rmp map[string]*ranging.Ranging, srmp map[string]*textranging.Ranging) error {
	for i, x := range xs {
		rid := id + "_" + strconv.Itoa(i)
		switch t := x.(type) {
		case nil, bool, int64, float64, string, time.Time:
			vr, err := s.Relation(rid)
			if err != nil {
				return err
			}
			r := vr.(*table)
			mp := make(map[string]interface{})
			mp["_"] = t
			attrs := r.updateAttributes([]map[string]interface{}{mp})
			{
				data, err := encoding.Encode(attrs)
				if err != nil {
					return err
				}
				if err := b.Set(attrKey(r.id), data); err != nil {
					return err
				}
			}
			{
				if err := b.Set(rowsKey(r.id), encoding.EncodeUint64(r.rows+1)); err != nil {
					return err
				}
			}
			if err := r.addTuple(mp, b, bmp, rmp, srmp); err != nil {
				return err
			}
			r.attrs = attrs
			for _, attr := range r.attrs {
				r.mp[attr] = struct{}{}
			}
			r.s.rc.Set(r.id, r)
		case []interface{}:
			if err := s.addTupleByArray(rid, t, b, bmp, rmp, srmp); err != nil {
				return err
			}
		case map[string]interface{}:
			if err := s.addTupleByTable(rid, t, b, bmp, rmp, srmp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *storage) addTupleByTable(id string, mp map[string]interface{}, b engine.Batch, bmp map[string]*roaring.Bitmap, rmp map[string]*ranging.Ranging, srmp map[string]*textranging.Ranging) error {
	vr, err := s.Relation(id)
	if err != nil {
		return err
	}
	r := vr.(*table)
	attrs := r.updateAttributes([]map[string]interface{}{mp})
	{
		data, err := encoding.Encode(attrs)
		if err != nil {
			return err
		}
		if err := b.Set(attrKey(r.id), data); err != nil {
			return err
		}
	}
	{
		if err := b.Set(rowsKey(r.id), encoding.EncodeUint64(r.rows+1)); err != nil {
			return err
		}
	}
	if err := r.addTuple(mp, b, bmp, rmp, srmp); err != nil {
		return err
	}
	r.attrs = attrs
	for _, attr := range r.attrs {
		r.mp[attr] = struct{}{}
	}
	r.s.rc.Set(r.id, r)
	return nil
}

func (s *storage) getBitmap(id string) (*roaring.Bitmap, error) {
	if mp, ok := s.bc.Get(id); ok {
		return mp, nil
	}
	v, err := s.db.Get([]byte(id))
	switch {
	case err == nil:
		mp := roaring.NewBitmap()
		if err := mp.Read(v); err != nil {
			return nil, err
		}
		s.bc.Set(id, mp)
		return mp, nil
	case err == engine.NotExist:
		return roaring.NewBitmap(), nil
	default:
		return nil, err
	}
}

func (s *storage) getRangebitmap(id string) (*ranging.Ranging, error) {
	if mp, ok := s.rbc.Get(id); ok {
		return mp, nil
	}
	v, err := s.db.Get([]byte(id))
	switch {
	case err == nil:
		mp := ranging.New()
		if err := mp.Read(v); err != nil {
			return nil, err
		}
		s.rbc.Set(id, mp)
		return mp, nil
	case err == engine.NotExist:
		return ranging.New(), nil
	default:
		return nil, err
	}
}

func (s *storage) getRelation(id string) (*table, error) {
	var r table

	r.s = s
	r.id = id
	r.db = s.db
	r.mp = make(map[string]struct{})
	{
		v, err := s.db.Get(attrKey(id))
		switch {
		case err == nil:
			if err := encoding.Decode(v, &r.attrs); err != nil {
				return nil, err
			}
		case err != nil && err != engine.NotExist:
			return nil, err
		}
		for _, attr := range r.attrs {
			r.mp[attr] = struct{}{}
		}
	}
	{
		v, err := s.db.Get(rowsKey(id))
		switch {
		case err == nil:
			if len(v) != 8 {
				return nil, errors.New("illegal size of rows value")
			}
			r.rows = encoding.DecodeUint64(v)
		case err != nil && err != engine.NotExist:
			return nil, err
		}
	}
	{
		v, err := s.db.Get(sizeKey(id))
		switch {
		case err == nil:
			if len(v) != 8 {
				return nil, errors.New("illegal size of rows value")
			}
			r.size = encoding.DecodeUint64(v)
		case err != nil && err != engine.NotExist:
			return nil, err
		}
	}
	xs := strings.Split(id, ".")
	r.name = xs[len(xs)-1]
	return &r, nil
}

func fill(n int) value.Array {
	var a value.Array

	for i := 0; i < n; i++ {
		a = append(a, value.ConstEmpty)
	}
	return a
}

func attrKey(id string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._A")
	return buf.Bytes()
}

func rowsKey(id string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._C")
	return buf.Bytes()
}

func sizeKey(id string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._S")
	return buf.Bytes()
}

func colKey(id, attr string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._C.")
	buf.WriteString(attr)
	buf.WriteByte('.')
	buf.Write(encoding.EncodeUint64(row))
	return buf.Bytes()
}

func colPrefixKey(id, attr string) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._C.")
	buf.WriteString(attr)
	buf.WriteByte('.')
	return buf.Bytes()
}

func bnKey(id, attr string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._I.")
	buf.WriteString(attr)
	buf.WriteString("._BN.")
	buf.Write(encoding.EncodeUint32(uint32(row / Segment)))
	return buf.Bytes()
}

func bbKey(id, attr string, v bool, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._I.")
	buf.WriteString(attr)
	buf.WriteString("._BB.")
	buf.Write(encoding.EncodeUint32(uint32(row / Segment)))
	{
		buf.WriteByte('.')
		buf.WriteByte(byte(types.T_bool & 0xFF))
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}
	return buf.Bytes()
}

func bsKey(id, attr string, v string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._I.")
	buf.WriteString(attr)
	buf.WriteString("._BS.")
	buf.Write(encoding.EncodeUint32(uint32(row / Segment)))
	{
		buf.WriteByte('.')
		buf.WriteByte(byte(types.T_string & 0xFF))
		buf.WriteString(v)
	}
	return buf.Bytes()
}

func rbiKey(id, attr string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._I.")
	buf.WriteString(attr)
	buf.WriteString("._RBI.")
	buf.Write(encoding.EncodeUint32(uint32(row / Segment)))
	return buf.Bytes()
}

func rbfKey(id, attr string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._I.")
	buf.WriteString(attr)
	buf.WriteString("._RBF.")
	buf.Write(encoding.EncodeUint32(uint32(row / Segment)))
	return buf.Bytes()
}

func rbtKey(id, attr string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._I.")
	buf.WriteString(attr)
	buf.WriteString("._RBT.")
	buf.Write(encoding.EncodeUint32(uint32(row / Segment)))
	return buf.Bytes()
}

func bsPrefixKey(id, attr string, row uint64) []byte {
	var buf bytes.Buffer

	buf.WriteString(id)
	buf.WriteString("._I.")
	buf.WriteString(attr)
	buf.WriteString("._BS.")
	buf.Write(encoding.EncodeUint32(uint32(row / Segment)))
	buf.WriteByte('.')
	buf.WriteByte(byte(types.T_string & 0xFF))
	return buf.Bytes()
}

func stringNe(db engine.DB, id, attr string, v string, row uint64) (*roaring.Bitmap, error) {
	prefix := bsPrefixKey(id, attr, row)
	itr, err := db.NewIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(prefix)
	mp := roaring.NewBitmap()
	for itr.Valid() {
		key := itr.Key()
		if bytes.Compare(key[len(prefix):], []byte(v)) != 0 {
			v, err := itr.Value()
			if err != nil {
				return nil, err
			}
			mq := roaring.NewBitmap()
			if err := mq.Read(v); err != nil {
				return nil, err
			}
			mp = mp.Union(mq)
		}
		itr.Next()
	}
	return mp, nil
}

func stringLt(db engine.DB, id, attr string, v string, row uint64) (*roaring.Bitmap, error) {
	prefix := bsPrefixKey(id, attr, row)
	itr, err := db.NewIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(prefix)
	mp := roaring.NewBitmap()
	for itr.Valid() {
		key := itr.Key()
		if bytes.Compare(key[len(prefix):], []byte(v)) >= 0 {
			break
		}
		v, err := itr.Value()
		if err != nil {
			return nil, err
		}
		mq := roaring.NewBitmap()
		if err := mq.Read(v); err != nil {
			return nil, err
		}
		mp = mp.Union(mq)
		itr.Next()
	}
	return mp, nil
}

func stringLe(db engine.DB, id, attr string, v string, row uint64) (*roaring.Bitmap, error) {
	prefix := bsPrefixKey(id, attr, row)
	itr, err := db.NewIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(prefix)
	mp := roaring.NewBitmap()
	for itr.Valid() {
		key := itr.Key()
		if bytes.Compare(key[len(prefix):], []byte(v)) > 0 {
			break
		}
		v, err := itr.Value()
		if err != nil {
			return nil, err
		}
		mq := roaring.NewBitmap()
		if err := mq.Read(v); err != nil {
			return nil, err
		}
		mp = mp.Union(mq)
		itr.Next()
	}
	return mp, nil
}

func stringGt(db engine.DB, id, attr string, v string, row uint64) (*roaring.Bitmap, error) {
	prefix := bsPrefixKey(id, attr, row)
	itr, err := db.NewIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(bsKey(id, attr, v, row))
	mp := roaring.NewBitmap()
	for itr.Valid() {
		key := itr.Key()
		if bytes.Compare(key[len(prefix):], []byte(v)) <= 0 {
			itr.Next()
			continue
		}
		v, err := itr.Value()
		if err != nil {
			return nil, err
		}
		mq := roaring.NewBitmap()
		if err := mq.Read(v); err != nil {
			return nil, err
		}
		mp = mp.Union(mq)
		itr.Next()
	}
	return mp, nil
}

func stringGe(db engine.DB, id, attr string, v string, row uint64) (*roaring.Bitmap, error) {
	prefix := bsPrefixKey(id, attr, row)
	itr, err := db.NewIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(bsKey(id, attr, v, row))
	mp := roaring.NewBitmap()
	for itr.Valid() {
		v, err := itr.Value()
		if err != nil {
			return nil, err
		}
		mq := roaring.NewBitmap()
		if err := mq.Read(v); err != nil {
			return nil, err
		}
		mp = mp.Union(mq)
		itr.Next()
	}
	return mp, nil
}

func stringBetween(db engine.DB, id, attr string, x, y string, row uint64) (*roaring.Bitmap, error) {
	prefix := bsPrefixKey(id, attr, row)
	itr, err := db.NewIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer itr.Close()
	itr.Seek(bsKey(id, attr, x, row))
	mp := roaring.NewBitmap()
	for itr.Valid() {
		key := itr.Key()
		if bytes.Compare(key[len(prefix):], []byte(y)) > 0 {
			break
		}
		v, err := itr.Value()
		if err != nil {
			return nil, err
		}
		mq := roaring.NewBitmap()
		if err := mq.Read(v); err != nil {
			return nil, err
		}
		mp = mp.Union(mq)
		itr.Next()
	}
	return mp, nil
}
