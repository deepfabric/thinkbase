package avg

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/summarize/overload"

type avg struct {
	cnt float64
	agg overload.Aggregation
}
