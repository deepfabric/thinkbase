package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func newUnion(n int, c context.Context, a, b relation.Relation) ([]unit.Unit, error) {
	if len(a.Metadata()) != len(b.Metadata()) {
		return nil, errors.New("size is different")
	}
	if n < 4 {
		us := []unit.Unit{}
		us = append(us, &unionUnit{c, a})
		us = append(us, &unionUnit{c, b})
		return us, nil
	}
	as, err := a.Split(n / 2)
	if err != nil {
		return nil, err
	}
	bs, err := b.Split(n / 2)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	for i, j := 0, len(as); i < j; i++ {
		us = append(us, &unionUnit{c, as[i]})
	}
	for i, j := 0, len(bs); i < j; i++ {
		us = append(us, &unionUnit{c, bs[i]})
	}
	return us, nil
}

func (u *unionUnit) Result() (relation.Relation, error) {
	return u.a, nil
}
