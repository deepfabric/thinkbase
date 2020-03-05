package overload

import "github.com/deepfabric/thinkbase/pkg/algebra/value"

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
	Eval() (value.Value, error)
	Fill(value.Attribute) error
}
