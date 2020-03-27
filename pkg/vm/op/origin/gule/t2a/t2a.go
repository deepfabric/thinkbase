package t2a

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, c context.Context) *t2a {
	return &t2a{
		c:       c,
		prev:    prev,
		isCheck: false,
	}
}

func (n *t2a) Name() (string, error) {
	return n.prev.Name()
}

func (n *t2a) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *t2a) GetTuples(limit int) (value.Array, error) {
	return n.prev.GetTuples(limit)
}

func (n *t2a) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	is, err := n.indexs(attrs)
	if err != nil {
		return nil, err
	}
	ts, err := n.prev.GetTuples(limit)
	if err != nil {
		return nil, err
	}
	if len(ts) == 0 {
		return nil, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(ts); i < j; i++ {
		for idx, attr := range attrs {
			rq[attr] = append(rq[attr], ts[i].(value.Array)[is[idx]])
		}
	}
	return rq, nil
}

func (n *t2a) indexs(attrs []string) ([]int, error) {
	as, err := n.AttributeList()
	if err != nil {
		return nil, err
	}
	return util.Indexs(attrs, as), nil
}

func (n *t2a) check(attrs []string) error {
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
