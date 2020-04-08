package a2t

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, c context.Context) *a2t {
	return &a2t{
		c:       c,
		prev:    prev,
		isCheck: false,
	}
}

func (n *a2t) Size() float64 {
	return n.prev.Size()
}

func (n *a2t) Cost() float64 {
	return n.prev.Cost()
}

func (n *a2t) Dup() op.OP {
	return &a2t{
		c:       n.c,
		prev:    n.prev,
		isCheck: n.isCheck,
	}
}

func (n *a2t) SetChild(o op.OP, _ int) { n.prev = o }
func (n *a2t) Operate() int            { return op.A2t }
func (n *a2t) Children() []op.OP       { return []op.OP{n.prev} }
func (n *a2t) IsOrdered() bool         { return n.prev.IsOrdered() }

func (n *a2t) String() string {
	return n.prev.String()
}

func (n *a2t) Name() (string, error) {
	return n.prev.Name()
}

func (n *a2t) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *a2t) GetTuples(limit int) (value.Array, error) {
	if n.isCheck {
		attrs, err := n.AttributeList()
		if err != nil {
			return nil, err
		}
		n.attrs = attrs
		n.isCheck = true
	}
	mp, err := n.GetAttributes(n.attrs, limit)
	if err != nil {
		return nil, err
	}
	if len(mp) == 0 || len(mp[n.attrs[0]]) == 0 {
		return nil, nil
	}
	var a value.Array
	for i, j := 0, len(mp[n.attrs[0]]); i < j; i++ {
		var t value.Array
		for _, attr := range n.attrs {
			t = append(t, mp[attr][i])
		}
		a = append(a, t)
	}
	return a, nil
}

func (n *a2t) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	return n.prev.GetAttributes(attrs, limit)
}
