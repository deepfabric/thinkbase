package projection

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func New(r relation.Relation, as []*Attribute) *projection {
	return &projection{r: r, as: as}
}

func (p *projection) Projection() (relation.Relation, error) {
	var attrs []string

	for _, a := range p.as {
		name, err := getAttributeName(a)
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, name)
	}
	ts, err := util.GetTuples(p.r)
	if err != nil {
		return nil, err
	}
	r := mem.New("", attrs)
	for _, t := range ts {
		var rt value.Tuple
		for _, a := range p.as {
			if v, err := a.E.Eval([]value.Tuple{t, t}); err != nil {
				return nil, err
			} else {
				rt = append(rt, v)
			}
		}
		r.AddTuple(rt)
	}
	return r, nil
}

func getAttributeName(a *Attribute) (string, error) {
	if len(a.Alias) > 0 {
		return a.Alias, nil
	}
	switch t := a.E.(type) {
	case *extend.Attribute:
		return t.Name, nil
	default:
		if len(a.Alias) == 0 {
			return "", errors.New("need alias")
		}
	}
	return a.Alias, nil
}
