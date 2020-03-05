package notmatch

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/join/match"
	"github.com/deepfabric/thinkbase/pkg/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

func New(a, b relation.Relation) *notmatch {
	return &notmatch{a, b}
}

func (m *notmatch) Minus() (relation.Relation, error) {
	c, err := match.New(m.a, m.b).Join()
	if err != nil {
		return nil, err
	}
	r, err := minus.New(m.a, c).Minus()
	if err != nil {
		return nil, err
	}
	return r, nil
}
