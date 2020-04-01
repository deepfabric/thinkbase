package hash

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

var (
	NotExist = errors.New("Not Exist")
)

type Hash interface {
	Destroy() error

	Set(value.Value) error

	Pop(int) (vector.Vector, error)
}
