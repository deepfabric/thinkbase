package value

import (
	"time"

	"github.com/deepfabric/thinkbase/pkg/algebra/types"
)

type Value interface {
	String() string
	Compare(Value) int
	ResolvedType() *types.T

	IsLogical() bool
	Attributes() map[int][]string
	Eval([]Tuple, map[int]map[string]int) (Value, error)
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

type Tuple Array
type Attribute Array

type Tuples []Tuple

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

func (ts Tuples) Len() int           { return len(ts) }
func (ts Tuples) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts Tuples) Less(i, j int) bool { return ts[i].Compare(ts[j]) < 0 }
