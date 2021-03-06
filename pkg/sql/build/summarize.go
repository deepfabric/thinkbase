package build

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
)

func (b *build) addSummarize(e *summarize.Extend) error {
	if _, ok := b.ss.mp[e.Alias]; !ok {
		b.ss.es = append(b.ss.es, e)
		b.ss.mp[e.Alias] = struct{}{}
	}
	return nil
}

var AggFuncs map[string]int = map[string]int{
	"avg":   overload.Avg,
	"max":   overload.Max,
	"min":   overload.Min,
	"sum":   overload.Sum,
	"count": overload.Count,
}
