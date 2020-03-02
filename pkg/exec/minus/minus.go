package minus

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/intersect"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

func New(us []unit.Unit) *minus {
	return &minus{us}
}

// A - B = A - (B1 V B2 ...) = (A - B1) ^ (A - B2) ...
func (e *minus) Minus() (relation.Relation, error) {
	var err error
	var wg sync.WaitGroup

	rs := make([]relation.Relation, len(e.us))
	for i, j := 0, len(e.us); i < j; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, privErr := e.us[idx].Result()
			if err != nil {
				err = privErr
			}
			rs[idx] = r
		}(i)
	}
	wg.Wait()
	if err != nil {
		return nil, err
	}
	r := rs[0]
	for i, j := 1, len(rs); i < j; i++ {
		r, err = intersect.New(r, rs[i]).Intersect()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
