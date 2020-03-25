package value

import (
	"time"

	"github.com/deepfabric/thinkbase/pkg/vm/types"
)

type Value interface {
	Size() int

	String() string
	Compare(Value) int
	ResolvedType() *types.T

	IsLogical() bool
	Attributes() []string
	Eval(map[string]Value) (Value, error)
}

type Bool bool
type Int int64
type Float float64
type String string

type Null struct{}
type Array []Value

type Table struct {
	Id string
}

type Time struct {
	time.Time
}

var (
	ConstTrue  Bool  = true
	ConstFalse Bool  = false
	ConstNull  Null  = Null{}
	ConstTable Table = Table{}
)

// time.Time formats.
const (
	// TimeOutputFormat is used to output all time.
	TimeOutputFormat = "2006-01-02 15:04:05"
)

func (a Array) Len() int           { return len(a) }
func (a Array) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Array) Less(i, j int) bool { return a[i].Compare(a[j]) < 0 }
