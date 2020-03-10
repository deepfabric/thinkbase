package order

import (
	"sort"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, r relation.Relation, lt func(value.Tuple, value.Tuple) bool) *order {
	return &order{c, r, lt}
}

func (o *order) Order() (relation.Relation, error) {
	ts, err := util.GetTuples(o.r)
	if err != nil {
		return nil, err
	}
	sort.Sort(&tuples{ts, o.lt})
	r := mem.New(o.r.Name(), o.r.Metadata(), o.c)
	r.AddTuples(ts)
	return r, nil
}

func NewLT(descs []bool, attrs []string, md []string) func(value.Tuple, value.Tuple) bool {
	var is []int

	for _, attr := range attrs {
		is = append(is, getAttributeIndex(attr, md))
	}
	return func(x, y value.Tuple) bool {
		for idx, i := range is {
			if i >= 0 {
				if r := int(x[i].ResolvedType().Oid - y[i].ResolvedType().Oid); r != 0 {
					return less(descs[idx], r)
				}
				if r := x[i].Compare(y[i]); r != 0 {
					return less(descs[idx], r)
				}
			}
		}
		return false
	}
}

func getAttributeIndex(attr string, md []string) int {
	for i, a := range md {
		if a == attr {
			return i
		}
	}
	return -1
}

func less(desc bool, r int) bool {
	if desc {
		return r > 0
	}
	return r < 0
}
