package relation

import (
	"github.com/deepfabric/thinkbase/pkg/storage/ranging"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type Relation interface {
	Destroy() error

	Size() float64
	Cost() float64

	Operate() int

	Dup() op.OP
	Children() []op.OP
	SetChild(op.OP, int)

	IsOrdered() bool

	DataString() string            // used for memory table
	AddTuples([]value.Array) error // used for memory table

	Id() string
	Rows() uint64
	String() string
	Name() (string, error)
	AttributeList() ([]string, error)
	AddTuplesByJson([]map[string]interface{}) error
	GetAttributes([]string, int) (map[string]value.Array, error)
	GetAttributesByIndex([]string, []uint64, int) (map[string]value.Array, error)

	StrMin(string, uint64, *roaring.Bitmap) (string, error)
	StrMax(string, uint64, *roaring.Bitmap) (string, error)

	StrCount(string, uint64, *roaring.Bitmap) (uint64, error)
	NullCount(string, uint64, *roaring.Bitmap) (uint64, error)
	BoolCount(string, uint64, *roaring.Bitmap) (uint64, error)

	IntRangeBitmap(string, uint64) (*ranging.Ranging, error)
	TimeRangeBitmap(string, uint64) (*ranging.Ranging, error)
	FloatRangeBitmap(string, uint64) (*ranging.Ranging, error)

	IntBitmapFold(string, uint64, mdictionary.Mdictionary) error
	NullBitmapFold(string, uint64, mdictionary.Mdictionary) error
	BoolBitmapFold(string, uint64, mdictionary.Mdictionary) error
	TimeBitmapFold(string, uint64, mdictionary.Mdictionary) error
	FloatBitmapFold(string, uint64, mdictionary.Mdictionary) error
	StringBitmapFold(string, uint64, mdictionary.Mdictionary) error

	Eq(string, value.Value, uint64) (*roaring.Bitmap, error)
	Ne(string, value.Value, uint64) (*roaring.Bitmap, error)
	Lt(string, value.Value, uint64) (*roaring.Bitmap, error)
	Le(string, value.Value, uint64) (*roaring.Bitmap, error)
	Gt(string, value.Value, uint64) (*roaring.Bitmap, error)
	Ge(string, value.Value, uint64) (*roaring.Bitmap, error)
}
