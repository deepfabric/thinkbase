package minus

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/union"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func New(us []unit.Unit, c context.Context) *minus {
	return &minus{us, c}
}

/*
// A - B = A - (B1 ∪  B2 ...) = (A - B1) ∩  (A - B2) ...
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
		r, err = intersect.New(e.c, r, rs[i]).Intersect()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
*/

func (e *minus) Minus() (relation.Relation, error) {
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
	r := rs[0]
	for i, j := 1, len(rs); i < j; i++ {
		r, err = union.New(e.c, r, rs[i]).Union()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
