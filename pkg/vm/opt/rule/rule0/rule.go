package rule0

import (
	"errors"
	"strings"
	"time"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	irestrict "github.com/deepfabric/thinkbase/pkg/vm/op/index/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/typefilter"
	Rule "github.com/deepfabric/thinkbase/pkg/vm/opt/rule"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(c context.Context) Rule.Rule {
	return &rule{c}
}

func (r *rule) Match(o op.OP, _ map[string]op.OP) bool {
	if o.Operate() != op.Restrict {
		return false
	}
	if _, ok := o.(*typefilter.Typefilter); ok {
		return false
	}
	_, ok := o.Children()[0].(relation.Relation)
	return ok
}

func (r *rule) Rewrite(o op.OP, mp map[string]op.OP, gmp, gmq map[string]int32) (op.OP, bool) {
	prev := o.Children()[0].(relation.Relation)
	tmp, imp := make(map[string]int32), make(map[string]int32)
	e, cs, err := r.disintegration(o.(restrict.RestrictOP).Extend(), prev, tmp, imp, gmp, gmq)
	if err == nil {
		var no op.OP
		switch {
		case e == nil && len(cs) > 0:
			no = irestrict.New(prev, filter.New(cs), r.c)
		case e != nil && len(cs) > 0:
			no = restrict.New(irestrict.New(prev, filter.New(cs), r.c), e, r.c)
		case e == nil && len(cs) == 0:
			no = typefilter.New(o.(restrict.RestrictOP))
		default:
			return o, false
		}
		if parent, ok := mp[o.String()]; ok {
			ps := parent.String()
			children := parent.Children()
			for i, child := range children {
				if child == o {
					parent.SetChild(no, i)
					break
				}
			}
			mp[no.String()] = parent
			if gparent, ok := mp[ps]; ok {
				mp[parent.String()] = gparent
			}
		} else {
			mp[""] = no
		}
		return no, true
	}
	return o, false
}

func (r *rule) disintegration(e extend.Extend, prev relation.Relation, tmp, imp, gmp, gmq map[string]int32) (extend.Extend, []*filter.Condition, error) {
	if !e.IsLogical() {
		return nil, nil, errors.New("extend must be a boolean expression")
	}
	switch v := e.(type) {
	case *value.Bool:
		return e, nil, nil
	case *extend.UnaryExtend:
		return r.disintegrationUnary(v, prev, tmp, imp, gmp, gmq)
	case *extend.BinaryExtend:
		return r.disintegrationBinary(v, prev, tmp, imp, gmp, gmq)
	}
	return nil, nil, errors.New("extend must be a boolean expression")
}

func (r *rule) disintegrationUnary(e *extend.UnaryExtend, prev relation.Relation, tmp, imp, gmp, gmq map[string]int32) (extend.Extend, []*filter.Condition, error) {
	if e.Op == overload.Not {
		return e, nil, nil
	}
	return nil, nil, errors.New("extend must be a boolean expression")
}

func (r *rule) disintegrationBinary(e *extend.BinaryExtend, prev relation.Relation, tmp, imp, gmp, gmq map[string]int32) (extend.Extend, []*filter.Condition, error) {
	switch e.Op {
	case overload.EQ:
		return r.buildEQ(e, prev, tmp, imp)
	case overload.LT:
		return r.buildLT(e, prev, tmp, imp)
	case overload.GT:
		return r.buildGT(e, prev, tmp, imp)
	case overload.LE:
		return r.buildLE(e, prev, tmp, imp)
	case overload.GE:
		return r.buildGE(e, prev, tmp, imp)
	case overload.NE:
		return r.buildNE(e, prev, tmp, imp)
	case overload.Group:
		return r.buildGroup(e, prev, gmq)
	case overload.GroupTry:
		return r.buildGroup(e, prev, gmp)
	case overload.Or:
		le, lc, err := r.disintegration(e.Left, prev, tmp, imp, gmp, gmq)
		if err != nil {
			return nil, nil, err
		}
		re, rc, err := r.disintegration(e.Right, prev, tmp, imp, gmp, gmq)
		if err != nil {
			return nil, nil, err
		}
		switch {
		case le == nil && len(lc) == 0:
			return re, rc, nil
		case re == nil && len(rc) == 0:
			return le, lc, nil
		case len(lc) > 0 && len(rc) > 0:
			rc[0].IsOr = true
			return nil, append(lc, rc...), nil
		}
		return e, nil, nil
	case overload.And:
		le, lc, err := r.disintegration(e.Left, prev, tmp, imp, gmp, gmq)
		if err != nil {
			return nil, nil, err
		}
		re, rc, err := r.disintegration(e.Right, prev, tmp, imp, gmp, gmq)
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
		return e, nil, nil
	case overload.Like:
		return e, nil, nil
	}
	return nil, nil, errors.New("extend must be a boolean expression")
}

func (r *rule) buildEQ(e *extend.BinaryExtend, prev relation.Relation, tmp, imp map[string]int32) (extend.Extend, []*filter.Condition, error) {
	left, right := r.reduce(e.Left, tmp, imp), r.reduce(e.Right, tmp, imp)
	if lv, ok := left.(*extend.Attribute); ok {
		if rv, ok := right.(value.Value); ok && r.typeAssert(lv.Name, tmp, imp, rv) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.EQ, Name: lv.Name, Val: rv}}, nil
		}
	}
	if rv, ok := right.(*extend.Attribute); ok {
		if lv, ok := left.(value.Value); ok && r.typeAssert(rv.Name, tmp, imp, lv) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.EQ, Name: rv.Name, Val: lv}}, nil
		}
	}
	return e, nil, nil
}

func (r *rule) buildNE(e *extend.BinaryExtend, prev relation.Relation, tmp, imp map[string]int32) (extend.Extend, []*filter.Condition, error) {
	left, right := r.reduce(e.Left, tmp, imp), r.reduce(e.Right, tmp, imp)
	if lv, ok := left.(*extend.Attribute); ok {
		if rv, ok := right.(value.Value); ok && r.typeAssert(lv.Name, tmp, imp, rv) && r.stringCost(lv.Name, rv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.NE, Name: lv.Name, Val: rv}}, nil
		}
	}
	if rv, ok := right.(*extend.Attribute); ok {
		if lv, ok := left.(value.Value); ok && r.typeAssert(rv.Name, tmp, imp, lv) && r.stringCost(rv.Name, lv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.NE, Name: rv.Name, Val: lv}}, nil
		}
	}
	return e, nil, nil
}

func (r *rule) buildLT(e *extend.BinaryExtend, prev relation.Relation, tmp, imp map[string]int32) (extend.Extend, []*filter.Condition, error) {
	left, right := r.reduce(e.Left, tmp, imp), r.reduce(e.Right, tmp, imp)
	if lv, ok := left.(*extend.Attribute); ok {
		if rv, ok := right.(value.Value); ok && r.typeAssert(lv.Name, tmp, imp, rv) && r.stringCost(lv.Name, rv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.LT, Name: lv.Name, Val: rv}}, nil
		}
	}
	if rv, ok := right.(*extend.Attribute); ok {
		if lv, ok := left.(value.Value); ok && r.typeAssert(rv.Name, tmp, imp, lv) && r.stringCost(rv.Name, lv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.GE, Name: rv.Name, Val: lv}}, nil
		}
	}
	return e, nil, nil
}

func (r *rule) buildLE(e *extend.BinaryExtend, prev relation.Relation, tmp, imp map[string]int32) (extend.Extend, []*filter.Condition, error) {
	left, right := r.reduce(e.Left, tmp, imp), r.reduce(e.Right, tmp, imp)
	if lv, ok := left.(*extend.Attribute); ok {
		if rv, ok := right.(value.Value); ok && r.typeAssert(lv.Name, tmp, imp, rv) && r.stringCost(lv.Name, rv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.LE, Name: lv.Name, Val: rv}}, nil
		}
	}
	if rv, ok := right.(*extend.Attribute); ok {
		if lv, ok := left.(value.Value); ok && r.typeAssert(rv.Name, tmp, imp, lv) && r.stringCost(rv.Name, lv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.GT, Name: rv.Name, Val: lv}}, nil
		}
	}
	return e, nil, nil
}

func (r *rule) buildGT(e *extend.BinaryExtend, prev relation.Relation, tmp, imp map[string]int32) (extend.Extend, []*filter.Condition, error) {
	left, right := r.reduce(e.Left, tmp, imp), r.reduce(e.Right, tmp, imp)
	if lv, ok := left.(*extend.Attribute); ok {
		if rv, ok := right.(value.Value); ok && r.typeAssert(lv.Name, tmp, imp, rv) && r.stringCost(lv.Name, rv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.GT, Name: lv.Name, Val: rv}}, nil
		}
	}
	if rv, ok := right.(*extend.Attribute); ok {
		if lv, ok := left.(value.Value); ok && r.typeAssert(rv.Name, tmp, imp, lv) && r.stringCost(rv.Name, lv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.LE, Name: rv.Name, Val: lv}}, nil
		}
	}
	return e, nil, nil
}

func (r *rule) buildGE(e *extend.BinaryExtend, prev relation.Relation, tmp, imp map[string]int32) (extend.Extend, []*filter.Condition, error) {
	left, right := r.reduce(e.Left, tmp, imp), r.reduce(e.Right, tmp, imp)
	if lv, ok := left.(*extend.Attribute); ok {
		if rv, ok := right.(value.Value); ok && r.typeAssert(lv.Name, tmp, imp, rv) && r.stringCost(lv.Name, rv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.GE, Name: lv.Name, Val: rv}}, nil
		}
	}
	if rv, ok := right.(*extend.Attribute); ok {
		if lv, ok := left.(value.Value); ok && r.typeAssert(rv.Name, tmp, imp, lv) && r.stringCost(rv.Name, lv, imp, prev) {
			return nil, []*filter.Condition{&filter.Condition{Op: filter.LT, Name: rv.Name, Val: lv}}, nil
		}
	}
	return e, nil, nil
}

func (r *rule) buildGroup(e *extend.BinaryExtend, prev relation.Relation, mp map[string]int32) (extend.Extend, []*filter.Condition, error) {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(*value.String); ok {
			switch strings.ToLower(value.MustBeString(rv)) {
			case "int":
				mp[lv.Name] = types.T_int
				return nil, nil, nil
			case "null":
				mp[lv.Name] = types.T_null
				return nil, nil, nil
			case "time":
				mp[lv.Name] = types.T_time
				return nil, nil, nil
			case "bool":
				mp[lv.Name] = types.T_bool
				return nil, nil, nil
			case "float":
				mp[lv.Name] = types.T_float
				return nil, nil, nil
			case "string":
				mp[lv.Name] = types.T_string
				return nil, nil, nil
			}
		}
	}
	return e, nil, nil
}

func (r *rule) typeAssert(name string, tmp, imp map[string]int32, v value.Value) bool {
	if typ, ok := tmp[name]; ok {
		return typ == v.ResolvedType().Oid
	}
	if typ, ok := imp[name]; ok && typ == v.ResolvedType().Oid {
		return true
	}
	return false
}

func (r *rule) stringCost(name string, v value.Value, imp map[string]int32, prev relation.Relation) bool {
	if v.ResolvedType().Oid != types.T_string {
		return true
	}
	if typ, ok := imp[name]; ok && typ == v.ResolvedType().Oid {
		return true
	}
	return false
}

func (r *rule) reduce(e extend.Extend, tmp, imp map[string]int32) extend.Extend {
	if be, ok := e.(*extend.BinaryExtend); ok {
		switch be.Op {
		case overload.Index:
			return buildIndex(be, imp)
		case overload.IndexTry:
			return buildIndex(be, tmp)
		case overload.Typecast:
			return buildTypecast(be)
		}
	}
	return e
}

func buildIndex(e *extend.BinaryExtend, mp map[string]int32) extend.Extend {
	if lv, ok := e.Left.(*extend.Attribute); ok {
		if rv, ok := e.Right.(*value.String); ok {
			switch strings.ToLower(value.MustBeString(rv)) {
			case "int":
				mp[lv.Name] = types.T_int
				return lv
			case "null":
				mp[lv.Name] = types.T_null
				return lv
			case "time":
				mp[lv.Name] = types.T_time
				return lv
			case "bool":
				mp[lv.Name] = types.T_bool
				return lv
			case "float":
				mp[lv.Name] = types.T_float
				return lv
			case "string":
				mp[lv.Name] = types.T_string
				return lv
			}
		}
	}
	return e
}

func buildTypecast(e *extend.BinaryExtend) extend.Extend {
	if lv, ok := e.Left.(value.Value); ok {
		if rv, ok := e.Right.(*value.String); ok {
			switch typ := value.MustBeString(rv); strings.ToLower(typ) {
			case "int":
				if v, err := overload.BinaryEval(e.Op, lv, value.NewInt(0)); err == nil {
					return v
				}
			case "bool":
				if v, err := overload.BinaryEval(e.Op, lv, value.NewBool(true)); err == nil {
					return v
				}
			case "time":
				if v, err := overload.BinaryEval(e.Op, lv, value.NewTime(time.Now())); err == nil {
					return v
				}
			case "float":
				if v, err := overload.BinaryEval(e.Op, lv, value.NewFloat(0.0)); err == nil {
					return v
				}
			case "string":
				if v, err := overload.BinaryEval(e.Op, lv, value.NewString("")); err == nil {
					return v
				}
			}
		}
	}
	return e
}
