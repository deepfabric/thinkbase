package mem

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/storage/ranging"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(name string, attrs []string) *mem {
	mp := make(map[string]int)
	for i, attr := range attrs {
		mp[attr] = i
	}
	return &mem{
		mp:    mp,
		name:  name,
		attrs: attrs,
	}
}

func (r *mem) Destroy() error {
	return nil
}

func (r *mem) Size() float64 {
	size := 0
	for _, t := range r.ts {
		size += t.Size()
	}
	return float64(size)
}

func (r *mem) Cost() float64 {
	return r.Size()
}

func (r *mem) Dup() op.OP {
	return &mem{
		ts:    r.ts,
		mp:    r.mp,
		name:  r.name,
		start: r.start,
		attrs: r.attrs,
	}
}

func (r *mem) Operate() int {
	return op.Relation
}

func (r *mem) Children() []op.OP {
	return nil
}

func (r *mem) SetChild(_ op.OP, _ int) {}
func (r *mem) IsOrdered() bool         { return false }

func (r *mem) Id() string {
	return r.name
}

func (r *mem) Rows() uint64 {
	return uint64(len(r.ts))
}

func (r *mem) String() string {
	return r.name
}

func (r *mem) Name() (string, error) {
	return r.name, nil
}

func (r *mem) AttributeList() ([]string, error) {
	return r.attrs, nil
}

func (r *mem) AddTuples(ts []value.Array) error {
	r.ts = append(r.ts, ts...)
	return nil
}

func (r *mem) AddTuplesByJson(_ []map[string]interface{}) error {
	return errors.New("not support now")
}

func (r *mem) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	rq := make(map[string]value.Array)
	for size := 0; size < limit && r.start < len(r.ts); r.start++ {
		for _, attr := range attrs {
			v := r.ts[r.start][r.mp[attr]]
			size += v.Size()
			rq[attr] = append(rq[attr], v)
		}
	}
	return rq, nil
}

func (r *mem) GetAttributesByIndex(_ []string, _ []uint64, _ int) (map[string]value.Array, error) {
	return nil, errors.New("not support now")
}

func (r *mem) StrMin(_ string, _ uint64, _ *roaring.Bitmap) (string, error) {
	return "", errors.New("not support now")
}

func (r *mem) StrMax(_ string, _ uint64, _ *roaring.Bitmap) (string, error) {
	return "", errors.New("not support now")
}

func (r *mem) StrCount(_ string, _ uint64, _ *roaring.Bitmap) (uint64, error) {
	return 0, errors.New("not support now")
}

func (r *mem) NullCount(_ string, _ uint64, _ *roaring.Bitmap) (uint64, error) {
	return 0, errors.New("not support now")
}

func (r *mem) BoolCount(_ string, _ uint64, _ *roaring.Bitmap) (uint64, error) {
	return 0, errors.New("not support now")
}

func (r *mem) IntRangeBitmap(_ string, _ uint64) (*ranging.Ranging, error) {
	return nil, errors.New("not support now")
}

func (r *mem) TimeRangeBitmap(_ string, _ uint64) (*ranging.Ranging, error) {
	return nil, errors.New("not support now")
}

func (r *mem) FloatRangeBitmap(_ string, _ uint64) (*ranging.Ranging, error) {
	return nil, errors.New("not support now")
}

func (r *mem) IntBitmapFold(_ string, _ uint64, _ mdictionary.Mdictionary) error {
	return errors.New("not support now")
}

func (r *mem) NullBitmapFold(_ string, _ uint64, _ mdictionary.Mdictionary) error {
	return errors.New("not support now")
}

func (r *mem) BoolBitmapFold(_ string, _ uint64, _ mdictionary.Mdictionary) error {
	return errors.New("not support now")
}

func (r *mem) TimeBitmapFold(_ string, _ uint64, _ mdictionary.Mdictionary) error {
	return errors.New("not support now")
}

func (r *mem) FloatBitmapFold(_ string, _ uint64, _ mdictionary.Mdictionary) error {
	return errors.New("not support now")
}

func (r *mem) StringBitmapFold(_ string, _ uint64, _ mdictionary.Mdictionary) error {
	return errors.New("not support now")
}

func (r *mem) Eq(_ string, _ value.Value, _ uint64) (*roaring.Bitmap, error) {
	return nil, errors.New("not support now")
}

func (r *mem) Ne(_ string, _ value.Value, _ uint64) (*roaring.Bitmap, error) {
	return nil, errors.New("not support now")
}

func (r *mem) Lt(_ string, _ value.Value, _ uint64) (*roaring.Bitmap, error) {
	return nil, errors.New("not support now")
}

func (r *mem) Le(_ string, _ value.Value, _ uint64) (*roaring.Bitmap, error) {
	return nil, errors.New("not support now")
}

func (r *mem) Gt(_ string, _ value.Value, _ uint64) (*roaring.Bitmap, error) {
	return nil, errors.New("not support now")
}

func (r *mem) Ge(_ string, _ value.Value, _ uint64) (*roaring.Bitmap, error) {
	return nil, errors.New("not support now")
}
