package vector

import "github.com/deepfabric/thinkbase/pkg/vm/value"

type Vector interface {
	Destroy() error

	IsEmpty() (bool, error)

	Pop() (value.Value, error)
	Head() (value.Value, error)
	Pops(int, int) (value.Array, error)

	Append(value.Array) error
}
