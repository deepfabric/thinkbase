package nub

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(mp *sync.Map, c context.Context, r relation.Relation) *nub {
	return &nub{mp, c, r}
}

func (n *nub) Nub() (relation.Relation, error) {
	ts, err := util.GetTuples(n.r)
	if err != nil {
		return nil, err
	}
	mp := make(map[string]struct{})
	for i, j := 0, len(ts); i < j; i++ {
		k := ts[i].String()
		if _, ok := n.mp.LoadOrStore(k, struct{}{}); ok {
			ts = append(ts[:i], ts[i+1:]...)
			j = len(ts)
		} else {
			mp[k] = struct{}{}
		}
	}
	r := mem.New(n.r.Name(), n.r.Metadata(), n.c)
	r.AddTuples(ts)
	return r, nil
}
