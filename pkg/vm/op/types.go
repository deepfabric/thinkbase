package op

import (
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

const (
	Nub = iota
	Order
	Group
	Fetch
	Rename
	Relation
	Restrict
	Summarize
	Projection

	GroupWithIndex
	RestrictWithIndex
	SummarizeWithIndex
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
	GetAttributes([]string, int) (map[string]value.Array, error)
}
