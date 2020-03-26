package overload

import "github.com/deepfabric/thinkbase/pkg/vm/value"

const (
	Avg = iota
	Max
	Min
	Sum
	Count
)

var AggName = [...]string{
	Avg:   "avg",
	Max:   "max",
	Min:   "min",
	Sum:   "sum",
	Count: "count",
}

type Aggregation interface {
	Reset()
	Fill(value.Array) error
	Eval() (value.Value, error)
}
