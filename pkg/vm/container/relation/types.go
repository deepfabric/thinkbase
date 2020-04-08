package relation

import (
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

	Split(int) ([]Relation, error)

	String() string
	DataString() string
	Name() (string, error)
	AttributeList() ([]string, error)
	GetTuples(int) (value.Array, error)
	GetAttributes([]string, int) (map[string]value.Array, error)

	AddTuples([]value.Array) error
}
