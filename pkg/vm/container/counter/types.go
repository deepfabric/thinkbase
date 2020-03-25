package counter

import "github.com/deepfabric/thinkbase/pkg/vm/value"

type Counter interface {
	Destroy() error

	Inc(value.Value) error         // 如果不存在则设为1，存在则加1
	Dec(value.Value) (bool, error) // 如果存在则减1并返回true，不存在则返回false
}
