package build

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/group"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/nub"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/product"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/rename"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
)

func (b *build) buildSelect(n *tree.SelectClause) (op.OP, error) {
	b.ss = &summarizeOp{mp: make(map[string]struct{})}
	if n.From == nil {
		return nil, errors.New("need from clause")
	}
	if err := b.buildFrom(n.From); err != nil {
		return nil, err
	}
	e, err := b.buildWhere(n.Where)
	if err != nil {
		return nil, err
	}
	he, err := b.buildHaving(n.Having, n.GroupBy)
	if err != nil {
		return nil, err
	}
	ps, err := b.buildProjection(n.Sel)
	if err != nil {
		return nil, err
	}
	gs, err := b.buildGroup(n.GroupBy)
	if err != nil {
		return nil, err
	}
	return b.buildAlgebra(n.Distinct, e, he, ps, gs)
}

func (b *build) buildAliasedSelect(n *tree.AliasedSelect) (op.OP, error) {
	o, err := b.buildStatement(n.Sel)
	if err != nil {
		return nil, err
	}
	if n.As != nil {
		return rename.New(o, string(n.As.Alias), make(map[string]string), b.c), nil
	}
	return o, nil
}

func (b *build) buildAlgebra(distinct bool, e, he extend.Extend, ps []*projection.Extend, gs []string) (op.OP, error) {
	var o op.OP

	for i, t := range b.ts[0].ts {
		if i > 0 {
			o = product.New(b.buildTable(t), o, b.c)
		} else {
			o = b.buildTable(t)
		}
	}
	if e != nil {
		o = restrict.New(o, e, b.c)
	}
	if len(b.ss.es) > 0 {
		return b.buildAlgebraWithSummarize(distinct, he, o, ps, gs)
	}
	return b.buildAlgebraWithoutSummarize(distinct, o, ps, gs)
}

func (b *build) buildAlgebraWithoutSummarize(distinct bool, o op.OP, ps []*projection.Extend, gs []string) (op.OP, error) {
	if len(gs) > 0 {
		o = nub.New(o, gs, b.c)
		if len(ps) == 0 {
			return o, nil
		}
		return projection.New(o, ps, b.c), nil
	}
	if distinct {
		attrs, err := o.AttributeList()
		if err != nil {
			return nil, err
		}
		o = nub.New(o, attrs, b.c)
	}
	if len(ps) == 0 {
		return o, nil
	}
	return projection.New(o, ps, b.c), nil
}

func (b *build) buildAlgebraWithSummarize(distinct bool, e extend.Extend, o op.OP, ps []*projection.Extend, gs []string) (op.OP, error) {
	if len(gs) > 0 {
		o = group.New(o, e, gs, b.ss.es, b.c)
		if len(ps) == 0 {
			return o, nil
		}
		return projection.New(o, ps, b.c), nil
	}
	o = summarize.New(o, b.ss.es, b.c)
	if distinct {
		attrs, err := o.AttributeList()
		if err != nil {
			return nil, err
		}
		o = nub.New(o, attrs, b.c)
	}
	if len(ps) == 0 {
		return o, nil
	}
	return projection.New(o, ps, b.c), nil
}

func (b *build) buildTable(t *table) op.OP {
	switch {
	case t.o == nil && !t.isAlias:
		return t.r
	case t.o != nil && !t.isAlias:
		return t.o
	case t.o == nil && t.isAlias:
		return rename.New(t.r, t.name, make(map[string]string), b.c)
	default:
		return rename.New(t.o, t.name, make(map[string]string), b.c)
	}
}
