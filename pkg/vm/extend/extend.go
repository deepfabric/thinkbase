package extend

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func (e *UnaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *UnaryExtend) Attributes() []string {
	return e.E.Attributes()
}

func (e *UnaryExtend) Eval(mp map[string]value.Value) (value.Value, error) {
	v, err := e.E.Eval(mp)
	if err != nil {
		return nil, err
	}
	return overload.UnaryEval(e.Op, v)
}

func (e *BinaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *BinaryExtend) Attributes() []string {
	return util.MergeAttributes(e.Left.Attributes(), e.Right.Attributes())
}

func (e *BinaryExtend) Eval(mp map[string]value.Value) (value.Value, error) {
	l, err := e.Left.Eval(mp)
	if err != nil {
		return nil, err
	}
	r, err := e.Right.Eval(mp)
	if err != nil {
		return nil, err
	}
	return overload.BinaryEval(e.Op, l, r)
}

func (e *MultiExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *MultiExtend) Attributes() []string {
	var rs []string

	mp := make(map[string]struct{})
	for _, arg := range e.Args {
		attrs := arg.Attributes()
		for i, j := 0, len(attrs); i < j; i++ {
			if _, ok := mp[attrs[i]]; !ok {
				mp[attrs[i]] = struct{}{}
				rs = append(rs, attrs[i])
			}
		}
	}
	return rs
}

func (e *MultiExtend) Eval(mp map[string]value.Value) (value.Value, error) {
	var args []value.Value

	for _, v := range e.Args {
		arg, err := v.Eval(mp)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return overload.MultiEval(e.Op, args)
}

func (a *Attribute) IsLogical() bool {
	return false
}

func (a *Attribute) Attributes() []string {
	return []string{a.Name}
}

func (a *Attribute) Eval(mp map[string]value.Value) (value.Value, error) {
	if v, ok := mp[a.Name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("attribute '%s' not exist", a.Name)
}
