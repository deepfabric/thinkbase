package extend

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func Dup(a Extend, mp map[int]int) Extend {
	switch e := a.(type) {
	case *UnaryExtend:
		return &UnaryExtend{
			Op: e.Op,
			E:  Dup(e.E, mp),
		}
	case *BinaryExtend:
		return &BinaryExtend{
			Op:    e.Op,
			Left:  Dup(e.Left, mp),
			Right: Dup(e.Right, mp),
		}
	case *FuncExtend:
		tm := &FuncExtend{Op: e.Op}
		for i, j := 0, len(e.Args); i < j; i++ {
			tm.Args = append(tm.Args, Dup(e.Args[i], mp))
		}
		return tm
	case *Attribute:
		if plh, ok := mp[e.Placeholder]; ok {
			return &Attribute{plh, e.Name}
		}
	}
	return a
}

func (e *UnaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *UnaryExtend) Attributes() map[int][]string {
	return e.E.Attributes()
}

func (e *UnaryExtend) Eval(ts []value.Tuple, mp map[int]map[string]int) (value.Value, error) {
	v, err := e.E.Eval(ts[:1], mp)
	if err != nil {
		return nil, err
	}
	return overload.UnaryEval(e.Op, v)
}

func (e *BinaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *BinaryExtend) Attributes() map[int][]string {
	mp := make(map[int][]string)
	lmp := e.Left.Attributes()
	rmp := e.Right.Attributes()
	for k, v := range lmp {
		mp[k] = v
	}
	for k, v := range rmp {
		if _, ok := mp[k]; ok {
			mp[k] = append(mp[k], v...)
		} else {
			mp[k] = v
		}
	}
	return mp
}

func (e *BinaryExtend) Eval(ts []value.Tuple, mp map[int]map[string]int) (value.Value, error) {
	l, err := e.Left.Eval(ts[:1], mp)
	if err != nil {
		return nil, err
	}
	r, err := e.Right.Eval(ts[1:2], mp)
	if err != nil {
		return nil, err
	}
	return overload.BinaryEval(e.Op, l, r)
}

func (e *FuncExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (e *FuncExtend) Attributes() map[int][]string {
	mp := make(map[int][]string)
	for _, arg := range e.Args {
		amp := arg.Attributes()
		for k, v := range amp {
			if _, ok := mp[k]; ok {
				mp[k] = append(mp[k], v...)
			} else {
				mp[k] = v
			}
		}
	}
	return mp
}

func (e *FuncExtend) Eval(ts []value.Tuple, mp map[int]map[string]int) (value.Value, error) {
	var args []value.Value

	for i, v := range e.Args {
		arg, err := v.Eval(ts[i:i+1], mp)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return overload.FuncEval(e.Op, args)
}

func (a *Attribute) IsLogical() bool {
	return false
}

func (a *Attribute) Attributes() map[int][]string {
	mp := make(map[int][]string)
	mp[a.Placeholder] = []string{a.Name}
	return mp
}

func (a *Attribute) Eval(ts []value.Tuple, mp map[int]map[string]int) (value.Value, error) {
	return ts[0][mp[a.Placeholder][a.Name]], nil
}
