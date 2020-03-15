package product

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/union"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func New(attrs []string, us []unit.Unit, c context.Context) *product {
	return &product{attrs, us, c}
}

// A ⨯ B = (A ⨯ B1) ∪  (A ⨯ B2) ...
func (e *product) Join() (relation.Relation, error) {
	var err error
	var wg sync.WaitGroup

	rs := make([]relation.Relation, len(e.us))
	for i, j := 0, len(e.us); i < j; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, privErr := e.us[idx].Result()
			if privErr != nil {
				err = privErr
			}
			rs[idx] = r
		}(i)
	}
	wg.Wait()
	if err != nil {
		return nil, err
	}
	var r relation.Relation
	{
		ts, err := util.GetTuples(rs[0])
		if err != nil {
			return nil, err
		}
		r = mem.New("", e.attrs, e.c)
		r.AddTuples(ts)
	}
	for i, j := 1, len(rs); i < j; i++ {
		r, err = union.New(e.c, r, rs[i]).Union()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
