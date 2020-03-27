package dictVec

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

var (
	NotExist = errors.New("Not Exist")
)

type DictVector interface {
	Destroy() error

	PopKey() (string, error)

	Pop(string) (value.Value, error)
	Head(string) (value.Value, error)
	Pops(string, int, int) (value.Array, error)

	Push(string, value.Array) error
}
