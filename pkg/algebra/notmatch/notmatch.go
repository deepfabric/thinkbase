package notmatch

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/join/match"
	"github.com/deepfabric/thinkbase/pkg/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, a, b relation.Relation) *notmatch {
	return &notmatch{c, a, b}
}

func (m *notmatch) Minus() (relation.Relation, error) {
	c, err := match.New(m.c, m.a, m.b).Join()
	if err != nil {
		return nil, err
	}
	r, err := minus.New(m.c, m.a, c).Minus()
	if err != nil {
		return nil, err
	}
	return r, nil
}
