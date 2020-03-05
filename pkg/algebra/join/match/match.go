package match

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/join/natural"
	"github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

func New(a, b relation.Relation) *match {
	return &match{a, b}
}

func (j *match) Join() (relation.Relation, error) {
	r, err := natural.New(j.a, j.b).Join()
	if err != nil {
		return nil, err
	}
	if err := r.Nub(); err != nil {
		return nil, err
	}
	attrs := []*projection.Attribute{}
	{
		as := j.a.Metadata()
		for _, a := range as {
			e, err := extend.NewAttribute(a, r)
			if err != nil {
				return nil, err
			}
			attrs = append(attrs, &projection.Attribute{E: e})
		}
	}
	return projection.New(r, attrs).Projection()
}
