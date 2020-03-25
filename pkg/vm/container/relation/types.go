package relation

import "github.com/deepfabric/thinkbase/pkg/vm/value"

type Relation interface {
	Destroy() error

	Size() (int, error)

	Split(int) ([]Relation, error)

	AttributeList() ([]string, error)
	GetTuples(int) (value.Array, error)
	GetAttributes([]string, int) (map[string]value.Array, error)

	AddTuples([]value.Array) error
}
