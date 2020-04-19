package build

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/sql/parser"
	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/fetch"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/order"
)

func New(sql string, c context.Context) *build {
	return &build{
		c:   c,
		sql: sql,
		mp:  make(map[string]struct{}),
	}
}

func (b *build) Build() (op.OP, error) {
	n, err := parser.Parse(b.sql)
	if err != nil {
		return nil, err
	}
	return b.buildStatement(n)
}

func (b *build) buildStatement(n *tree.Select) (op.OP, error) {
	o, err := b.buildRelation(n.Relation)
	if err != nil {
		return nil, err
	}
	if len(n.OrderBy) > 0 {
		if o, err = b.buildOrderBy(o, n.OrderBy); err != nil {
			return nil, err
		}
	}
	if n.Limit != nil {
		if o, err = b.buildLimit(o, n.Limit); err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (b *build) buildLimit(o op.OP, lt *tree.Limit) (op.OP, error) {
	var off, cnt int

	if lt.Count != nil {
		count, err := b.buildExprIntConstant(lt.Count)
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, errors.New("the limit given must be >= 0")
		}
		cnt = int(count)
	}
	if lt.Offset != nil {
		offset, err := b.buildExprIntConstant(lt.Offset)
		if err != nil {
			return nil, err
		}
		if offset < 0 {
			return nil, errors.New("the offset given must be >= 0")
		}
		off = int(offset)
	}
	return fetch.New(o, cnt, off, b.c), nil
}

func (b *build) buildOrderBy(o op.OP, ords tree.OrderBy) (op.OP, error) {
	var descs []bool
	var attrs []string

	for _, ord := range ords {
		if ord.Type == tree.Descending {
			descs = append(descs, true)
		} else {
			descs = append(descs, false)
		}
		if e, ok := ord.E.(tree.ColunmNameList); ok {
			attr, err := b.buildExprColumn(e)
			if err != nil {
				return nil, err
			}
			attrs = append(attrs, attr)
		} else {
			return nil, fmt.Errorf("wrong 'ORDER BY %s' statement", ord.E)
		}
	}
	return order.New(o, descs, attrs, b.c), nil
}
