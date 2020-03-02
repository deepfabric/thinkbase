package projection

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func New(r relation.Relation, as []*Attribute) *projection {
	return &projection{r: r, as: as}
}

func (p *projection) Projection() (relation.Relation, error) {
	var as []*relation.AttributeMetadata

	for _, a := range p.as {
		name, err := getAttributeName(a)
		if err != nil {
			return nil, err
		}
		as = append(as, &relation.AttributeMetadata{
			Name:  name,
			Types: make(map[int32]int),
		})
	}
	r := relation.New("", nil, as)
	ts, err := util.GetTuples(p.r)
	if err != nil {
		return nil, err
	}
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
