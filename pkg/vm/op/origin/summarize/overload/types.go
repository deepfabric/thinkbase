package overload

import "github.com/deepfabric/thinkbase/pkg/vm/value"

const (
	Avg = iota
	Max
	Min
	Sum
	Count
	AvgI
	MaxI
	MinI
	SumI
	CountI
	AvgIt
	MaxIt
	MinIt
	SumIt
	CountIt
)

var AggName = [...]string{
	Avg:     "avg",
	Max:     "max",
	Min:     "min",
	Sum:     "sum",
	Count:   "count",
	AvgI:    "avgi",
	MaxI:    "maxi",
	MinI:    "mini",
	SumI:    "sumi",
	CountI:  "counti",
	AvgIt:   "avgit",
	MaxIt:   "maxit",
	MinIt:   "minit",
	SumIt:   "sumit",
	CountIt: "countit",
}

type Aggregation interface {
	Reset()
	Fill(value.Array) error
	Eval() (value.Value, error)
}
