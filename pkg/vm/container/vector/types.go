package vector

import "github.com/deepfabric/thinkbase/pkg/vm/value"

type Vector interface {
	Destroy() error

	Len() (int, error)

	IsEmpty() (bool, error)

	Get(int) (value.Value, error)

	Pop() (value.Value, error)
	Head() (value.Value, error)
	Pops(int, int) (value.Array, error)

	Append(value.Array) error
}
