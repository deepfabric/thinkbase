package relation

import "github.com/deepfabric/thinkbase/pkg/algebra/value"

type Relation interface {
	Name() string
	Metadata() []string

	Nub() error

	Split(int) ([]Relation, error)

	Rename(string) error
	RenameAttribute(string, string) error

	AddTuple(value.Tuple) error
	AddTuples([]value.Tuple) error

	GetTupleCount() (int, error)
	GetTuple(int) (value.Tuple, error)
	GetTuples(int, int) ([]value.Tuple, error)

	GetAttributeIndex(string) (int, error)
	GetAttribute(string) (value.Attribute, error)
	GetAttributeByLimit(string, int, int) (value.Attribute, error)

	Sort([]string, []bool) error
}
