package extend

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func (e *UnaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *UnaryExtend) Eval(ts []value.Tuple) (value.Value, error) {
	v, err := e.E.Eval(ts[:1])
	if err != nil {
		return nil, err
	}
	return overload.UnaryEval(e.Op, v)
}

func (e *BinaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *BinaryExtend) Eval(ts []value.Tuple) (value.Value, error) {
	l, err := e.Left.Eval(ts[:1])
	if err != nil {
		return nil, err
	}
	r, err := e.Right.Eval(ts[1:2])
	if err != nil {
		return nil, err
	}
	return overload.BinaryEval(e.Op, l, r)
}

func (e *FuncExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *FuncExtend) Eval(ts []value.Tuple) (value.Value, error) {
	var args []value.Value

	for i, v := range e.Args {
		arg, err := v.Eval(ts[i : i+1])
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return overload.FuncEval(e.Op, args)
}

func NewAttribute(name string, r relation.Relation) (*Attribute, error) {
	idx, err := r.GetAttributeIndex(name)
	if err != nil {
		return nil, err
	}
	return &Attribute{idx, name}, nil
}

func (a *Attribute) IsLogical() bool {
	return false
}

func (a *Attribute) Eval(ts []value.Tuple) (value.Value, error) {
	return ts[0][a.idx], nil
}
