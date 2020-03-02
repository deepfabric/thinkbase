package avg

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/summarize/overload/sum"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func New() *avg {
	return &avg{agg: sum.New()}
}

func (av *avg) Reset() {
	av.cnt = 0
	av.agg.Reset()
}

func (av *avg) Fill(a value.Attribute) error {
	if len(a) == 0 {
		return nil
	}
	if err := av.agg.Fill(a); err != nil {
		return err
	}
	av.cnt += float64(len(a))
	return nil
}

func (av *avg) Eval() (value.Value, error) {
	v, err := av.agg.Eval()
	if err != nil {
		return nil, err
	}
	if _, ok := value.AsInt(v); ok {
		return value.NewFloat(float64(value.MustBeInt(v)) / av.cnt), nil
	}
	return value.NewFloat(value.MustBeFloat(v) / av.cnt), nil
}
