package build

import (
	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
)

func (b *build) buildProjection(ns tree.SelectExprs) ([]*projection.Extend, error) {
	var es []*projection.Extend

	for _, n := range ns {
		if e, err := b.buildSelExpr(n); err != nil {
			return nil, err
		} else {
			es = append(es, e)
		}
	}
	return es, nil
}

func (b *build) buildSelExpr(n *tree.SelectExpr) (*projection.Extend, error) {
	e, err := b.buildExpr(n.E, Projection)
	if err != nil {
		return nil, err
	}
	if len(n.As) > 0 {
		return &projection.Extend{E: e, Alias: string(n.As)}, nil
	}
	return &projection.Extend{E: e, Alias: e.String()}, nil
}
