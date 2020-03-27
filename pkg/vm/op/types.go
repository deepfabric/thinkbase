package op

import "github.com/deepfabric/thinkbase/pkg/vm/value"

type OP interface {
	Name() (string, error)
	AttributeList() ([]string, error)
	GetTuples(int) (value.Array, error)
	GetAttributes([]string, int) (map[string]value.Array, error)
}
