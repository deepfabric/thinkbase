package fetch

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, limit, offset int, c context.Context) *fetch {
	return &fetch{
		c:       c,
		prev:    prev,
		isCheck: false,
		limit:   limit,
		offset:  offset,
	}
}

func (n *fetch) Context() context.Context {
	return n.c
}

func (n *fetch) Size() float64 {
	return n.c.FetchSize(n.prev, n.limit, n.offset)
}

func (n *fetch) Cost() float64 {
	return n.c.FetchCost(n.prev, n.limit, n.offset)
}

func (n *fetch) Dup() op.OP {
	return &fetch{
		c:       n.c,
		prev:    n.prev,
		limit:   n.limit,
		offset:  n.offset,
		isCheck: n.isCheck,
	}
}

func (n *fetch) SetChild(o op.OP, _ int) { n.prev = o }
func (n *fetch) Operate() int            { return op.Fetch }
func (n *fetch) Children() []op.OP       { return []op.OP{n.prev} }
func (n *fetch) IsOrdered() bool         { return n.prev.IsOrdered() }

func (n *fetch) String() string {
	return fmt.Sprintf("fetch(%v, %v, %s)", n.limit, n.offset, n.prev)
}

func (n *fetch) Name() (string, error) {
	return n.prev.Name()
}

func (n *fetch) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *fetch) GetTuples(limit int) (value.Array, error) {
	if n.cnt >= n.limit {
		return nil, nil
	}
	ts, err := n.prev.GetTuples(limit)
	if err != nil {
		return nil, err
	}
	for len(ts) > 0 && n.off < n.offset {
		n.off++
		ts = ts[1:]
	}
	if len(ts) > n.limit-n.cnt {
		ts = ts[:n.limit-n.cnt]
	}
	n.cnt += len(ts)
	return ts, nil
}

func (n *fetch) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	if n.cnt >= n.limit {
		return nil, nil
	}
	mp, err := n.prev.GetAttributes(attrs, limit)
	if err != nil {
		return nil, err
	}
	if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
		return mp, nil
	}
	for len(mp[attrs[0]]) > 0 && n.off < n.offset {
		n.off++
		for _, attr := range attrs {
			mp[attr] = mp[attr][1:]
		}
	}
	size := len(mp[attrs[0]])
	if size > n.limit-n.cnt {
		size = n.limit - n.cnt
		for _, attr := range attrs {
			mp[attr] = mp[attr][:size]
		}
	}
	n.cnt += size
	return mp, nil
}

func (n *fetch) check(attrs []string) error {
	as, err := n.prev.AttributeList()
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
