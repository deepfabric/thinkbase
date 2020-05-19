package main

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func main() {
	e0 := &extend.BinaryExtend{
		Op:    overload.LT,
		Left:  &extend.Attribute{"r.b"},
		Right: value.NewInt(3),
	}
	e1 := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"r.a"},
		Right: value.NewString("x"),
	}
	e2 := &extend.BinaryExtend{
		Op:    overload.Or,
		Left:  e0,
		Right: e1,
	}
	e3 := &extend.BinaryExtend{
		Op:    overload.And,
		Left:  e2,
		Right: value.NewBool(true),
	}
	e, cs, err := disintegration(e3)
	fmt.Printf("%v, %v: %v\n", e, filter.New(cs), err)
}

func disintegration(e extend.Extend) (extend.Extend, []*filter.Condition, error) {
	if !e.IsLogical() {
		return nil, nil, errors.New("extend must be a boolean expression")
	}
	switch v := e.(type) {
	case *value.Bool:
		return e, nil, nil
	case *extend.UnaryExtend:
		return disintegrationUnary(v)
	case *extend.BinaryExtend:
		return disintegrationBinary(v)
	}
	return nil, nil, errors.New("extend must be a boolean expression")
}

func disintegrationUnary(e *extend.UnaryExtend) (extend.Extend, []*filter.Condition, error) {
	if e.Op == overload.Not {
		return e, nil, nil
	}
	return nil, nil, errors.New("extend must be a boolean expression")
}

func disintegrationBinary(e *extend.BinaryExtend) (extend.Extend, []*filter.Condition, error) {
	switch e.Op {
	case overload.EQ:
		return buildEQ(e)
	case overload.LT:
		return buildLT(e)
	case overload.GT:
		return buildGT(e)
	case overload.LE:
		return buildLE(e)
	case overload.GE:
		return buildGE(e)
	case overload.NE:
		return buildNE(e)
	case overload.Or:
		le, lc, err := disintegration(e.Left)
		if err != nil {
			return nil, nil, err
		}
		re, rc, err := disintegration(e.Right)
		if err != nil {
			return nil, nil, err
		}
		if le == nil && re == nil {
			rc[0].IsOr = true
			return nil, append(lc, rc...), nil
		}
		return e, nil, nil
	case overload.And:
		le, lc, err := disintegration(e.Left)
		if err != nil {
			return nil, nil, err
		}
		re, rc, err := disintegration(e.Right)
		if err != nil {
			return nil, nil, err
		}
		switch {
		case le == nil && re == nil:
			return nil, append(lc, rc...), nil
		case le != nil && re == nil:
			return le, rc, nil
		case le == nil && re != nil:
			return re, lc, nil
		}
	}
	return nil, nil, errors.New("extend must be a boolean expression")
}

func buildEQ(e *extend.BinaryExtend) (extend.Extend, []*filter.Condition, error) {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(value.Value); ok {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.EQ, Name: lv.Name, Val: rv}}, nil
		}
	}
	return e, nil, nil
}

func buildNE(e *extend.BinaryExtend) (extend.Extend, []*filter.Condition, error) {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(value.Value); ok {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.NE, Name: lv.Name, Val: rv}}, nil
		}
	}
	return e, nil, nil
}

func buildLT(e *extend.BinaryExtend) (extend.Extend, []*filter.Condition, error) {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(value.Value); ok {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.LT, Name: lv.Name, Val: rv}}, nil
		}
	}
	return e, nil, nil
}

func buildLE(e *extend.BinaryExtend) (extend.Extend, []*filter.Condition, error) {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(value.Value); ok {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.LE, Name: lv.Name, Val: rv}}, nil
		}
	}
	return e, nil, nil
}

func buildGT(e *extend.BinaryExtend) (extend.Extend, []*filter.Condition, error) {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(value.Value); ok {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.GT, Name: lv.Name, Val: rv}}, nil
		}
	}
	return e, nil, nil
}

func buildGE(e *extend.BinaryExtend) (extend.Extend, []*filter.Condition, error) {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(value.Value); ok {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.GE, Name: lv.Name, Val: rv}}, nil
		}
	}
	return e, nil, nil
}
