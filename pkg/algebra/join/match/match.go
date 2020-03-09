package match

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/join/natural"
	"github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, a, b relation.Relation) *match {
	return &match{c, a, b}
}

func (j *match) Join() (relation.Relation, error) {
	r, err := natural.New(j.c, j.a, j.b).Join()
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
			attrs = append(attrs, &projection.Attribute{E: &extend.Attribute{r.Placeholder(), a}})
		}
	}
	return projection.New(r, j.c, attrs).Projection()
}
