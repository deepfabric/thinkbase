package notmatch

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/join/match"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

func New(isNub bool, a, b relation.Relation) *notmatch {
	return &notmatch{isNub, a, b}
}

func (m *notmatch) Minus() (relation.Relation, error) {
	c, err := match.New(m.a, m.b).Join()
	if err != nil {
		return nil, err
	}
	r, err := minus.New(m.isNub, m.a, c).Minus()
	if err != nil {
		return nil, err
	}
	if m.isNub {
		if err := r.Nub(); err != nil {
			return nil, err
		}
	}
	return r, nil
}
