package avg

import "github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"

type avg struct {
	typ int32
	cnt float64
	agg overload.Aggregation
}
