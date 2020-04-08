package extend

import "github.com/deepfabric/thinkbase/pkg/vm/value"

type Extend interface {
	String() string
	IsLogical() bool
	Attributes() []string
	Eval(map[string]value.Value) (value.Value, error)
}

type UnaryExtend struct {
	Op int
	E  Extend
}

type BinaryExtend struct {
	Op          int
	Left, Right Extend
}

type MultiExtend struct {
	Op   int
	Args []Extend
}

type Attribute struct {
	Name string
}
