package avg

import "github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"

type avg struct {
	cnt float64
	agg overload.Aggregation
}
