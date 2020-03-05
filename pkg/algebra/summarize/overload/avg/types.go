package avg

import "github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"

type avg struct {
	cnt float64
	agg overload.Aggregation
}
