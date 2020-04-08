package op

import (
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

const (
	A2t = iota
	T2a
	Nub
	Order
	Group
	Fetch
	Rename
	Product
	Relation
	Restrict
	Summarize
	Projection
	SetUnion
	SetIntersect
	SetDifference
	MultisetUnion
	MultisetIntersect
	MultisetDifference
	SemiJoin
	InnerJoin
	NaturalJoin
)

type OP interface {
	Size() float64
	Cost() float64

	Dup() OP
	Operate() int
	Children() []OP
	SetChild(OP, int)

	IsOrdered() bool

	String() string
	Name() (string, error)
	AttributeList() ([]string, error)
	GetTuples(int) (value.Array, error)
	GetAttributes([]string, int) (map[string]value.Array, error)
}

type OrderOP interface {
	OP
	NewLT() func(value.Value, value.Value) bool
}

type SetUnionOP interface {
	OP
	NewHashUnion(OP, OP) OP
	NewOrderUnion(OP, OP) OP
}
