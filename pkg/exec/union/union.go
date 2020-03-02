package union

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	aunion "github.com/deepfabric/thinkbase/pkg/sql/algebra/union"
)

func New(isNub bool, us []unit.Unit) *union {
	return &union{isNub, us}
}

// A U B = (A1 V A2 ...) V (B1 V B2 ...)
func (e *union) Union() (relation.Relation, error) {
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
		r, err = aunion.New(e.isNub, r, rs[i]).Union()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
