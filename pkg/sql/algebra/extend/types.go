package extend

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

type Extend interface {
	IsLogical() bool
	Eval([]value.Tuple) (value.Value, error)
}

type UnaryExtend struct {
	Op int
	E  Extend
}

type BinaryExtend struct {
	Op          int
	Left, Right Extend
}

type FuncExtend struct {
	Op   int
	Args []Extend
}

type Attribute struct {
	idx  int
	Name string
}
