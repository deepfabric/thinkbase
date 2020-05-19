package restrict

import (
	"bytes"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(r relation.Relation, fl filter.Filter, c context.Context) *restrict {
	return &restrict{
		c:       c,
		r:       r,
		fl:      fl,
		isCheck: false,
		rows:    r.Rows(),
	}
}

func (n *restrict) Filter() filter.Filter {
	return n.fl
}

func (n *restrict) Size() float64 {
	return n.c.RestrictSizeWithIndex(n.r, n.fl)
}

func (n *restrict) Cost() float64 {
	return n.c.RestrictCostWithIndex(n.r, n.fl)
}

func (n *restrict) Dup() op.OP {
	return &restrict{
		c:       n.c,
		r:       n.r,
		fl:      n.fl,
		row:     n.row,
		rows:    n.rows,
		isCheck: n.isCheck,
	}
}

func (n *restrict) SetChild(o op.OP, _ int) { n.r = o.(relation.Relation) }
func (n *restrict) Operate() int            { return op.RestrictWithIndex }
func (n *restrict) Children() []op.OP       { return []op.OP{n.r} }
func (n *restrict) IsOrdered() bool         { return n.r.IsOrdered() }

func (n *restrict) String() string {
	var buf bytes.Buffer

	buf.WriteString("σ(index, ")
	buf.WriteString(n.fl.String())
	buf.WriteString(fmt.Sprintf(", %s)", n.r))
	return buf.String()
}

func (n *restrict) Name() (string, error) {
	return n.r.Name()
}

func (n *restrict) AttributeList() ([]string, error) {
	return n.r.AttributeList()
}

func (n *restrict) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	if n.row >= n.rows {
		return nil, nil
	}
	if len(n.is) > 0 {
		mp, err := n.r.GetAttributesByIndex(attrs, n.is, limit)
		if err != nil {
			return nil, err
		}
		n.is = n.is[len(mp[attrs[0]]):]
		return mp, nil
	}
	{
		mp, err := n.fl.Bitmap(n.r, n.row)
		if err != nil {
			return nil, err
		}
		n.is = mp.Slice()
		for i := range n.is {
			n.is[i] = n.is[i] + n.row
		}
	}
	n.row += storage.Segment
	mp, err := n.r.GetAttributesByIndex(attrs, n.is, limit)
	if err != nil {
		return nil, err
	}
	n.is = n.is[len(mp[attrs[0]]):]
	return mp, nil
}

func (n *restrict) check(attrs []string) error {
	as, err := n.r.AttributeList()
	if err != nil {
		return err
	}
	mp := make(map[string]struct{})
	for _, a := range as {
		mp[a] = struct{}{}
	}
	for _, attr := range attrs {
		if _, ok := mp[attr]; !ok {
			return fmt.Errorf("failed to find attribute '%s'", attr)
		}
	}
	return nil
}
