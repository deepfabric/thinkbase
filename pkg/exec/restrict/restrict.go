package restrict

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/union"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func New(us []unit.Unit) *restrict {
	return &restrict{us}
}

// σ(A) = σ(A1) ∪  σ(A2) ...
func (e *restrict) Restrict() (relation.Relation, error) {
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
		r, err = union.New(false, r, rs[i]).Union()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
