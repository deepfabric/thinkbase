package dictionary

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

var (
	NotExist = errors.New("Not Exist")
)

type Dictionary interface {
	Destroy() error

	IsExit(value.Value) error

	Set(value.Value, interface{}) error
	Get(value.Value) (interface{}, error)
	GetOrSet(value.Value, interface{}) (bool, interface{}, error) // 如果value是set的则返回false，如果是加载的则返回true
}
