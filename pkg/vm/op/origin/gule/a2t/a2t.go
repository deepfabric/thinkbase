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
