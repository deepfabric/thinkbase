package avg

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/sum"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *avg {
	return &avg{agg: sum.New()}
}

func (av *avg) Reset() {
	av.cnt = 0
	av.agg.Reset()
}

func (av *avg) Fill(a value.Array) error {
	if len(a) == 0 {
		return nil
	}
	if err := av.agg.Fill(a); err != nil {
		return err
	}
	for _, v := range a {
		if oid := v.ResolvedType().Oid; oid == types.T_int || oid == types.T_float {
			av.cnt++
		}
	}
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
