package avg

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/sum"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(typ int32) *avg {
	return &avg{typ: typ, agg: sum.New(typ)}
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
		switch av.typ {
		case types.T_any:
			if typ := v.ResolvedType().Oid; typ != types.T_int && typ != types.T_float {
				continue
			}
		default:
			if v.ResolvedType().Oid != av.typ {
				continue
			}
		}
		av.cnt++
	}
	return nil
}

func (av *avg) Eval() (value.Value, error) {
	if av.cnt == 0 {
		return value.NewFloat(0.0), nil
	}
	v, err := av.agg.Eval()
	if err != nil {
		return nil, err
	}
	if _, ok := value.AsInt(v); ok {
		return value.NewFloat(float64(value.MustBeInt(v)) / av.cnt), nil
	}
	return value.NewFloat(value.MustBeFloat(v) / av.cnt), nil
}
