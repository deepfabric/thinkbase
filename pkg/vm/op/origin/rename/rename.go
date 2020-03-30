package rename

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, name string, mp map[string]string, c context.Context) *rename {
	mq := make(map[string]string)
	for k, v := range mp {
		mq[v] = k
	}
	return &rename{
		c:    c,
		mq:   mq,
		mp:   mp,
		prev: prev,
		name: name,
	}
}

func (n *rename) Name() (string, error) {
	return n.name, nil
}

func (n *rename) AttributeList() ([]string, error) {
	attrs, err := n.prev.AttributeList()
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(attrs); i < j; i++ {
		if v, ok := n.mp[attrs[i]]; ok {
			attrs[i] = v
		}
	}
	return attrs, nil
}

func (n *rename) GetTuples(limit int) (value.Array, error) {
	return n.prev.GetTuples(limit)
}

func (n *rename) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	for i, j := 0, len(attrs); i < j; i++ {
		if v, ok := n.mq[attrs[i]]; ok {
			attrs[i] = v
		}
	}
	mp, err := n.prev.GetAttributes(attrs, limit)
	if err != nil {
		return nil, err
	}
	for k, v := range n.mp {
		mp[v] = mp[k]
		delete(mp, k)
	}
	return mp, nil
}

func (n *rename) check(attrs []string) error {
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
