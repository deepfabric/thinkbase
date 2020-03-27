package counter

import "github.com/deepfabric/thinkbase/pkg/vm/value"

type Counter interface {
	Destroy() error

	Pops(int) (value.Array, error) // 根据计数弹出值

	Set(value.Value) error // 如果不存在则设为1, 存在则什么也不错
	Del(value.Value) error
	Get(value.Value) (int, error)

	Inc(value.Value) error              // 如果不存在则设为1，存在则加1
	Dec(value.Value) (bool, error)      // 如果存在则减1并返回true，不存在则返回false
	IncAndGet(value.Value) (int, error) // +1并且返回原值
	DecAndGet(value.Value) (int, error) // -1并且返回原值
}
