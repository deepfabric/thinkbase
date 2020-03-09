package extend

import "github.com/deepfabric/thinkbase/pkg/algebra/value"

type Extend interface {
	IsLogical() bool
	Attributes() map[int][]string // placeholder -> attribute list
	Eval([]value.Tuple, map[int]map[string]int) (value.Value, error)
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
	Placeholder int
	Name        string
}
