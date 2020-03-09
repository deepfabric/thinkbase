package projection

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(r relation.Relation, c context.Context, as []*Attribute) *projection {
	return &projection{r: r, c: c, as: as}
}

func (p *projection) Projection() (relation.Relation, error) {
	cnt, err := p.r.GetTupleCount()
	if err != nil {
		return nil, err
	}
	mp, as, err := util.Getattribute(p.r.Placeholder(), getattributes(p.r.Placeholder(), p.as), p.c)
	if err != nil {
		return nil, err
	}
	var r relation.Relation
	{
		var attrs []string
		for _, a := range p.as {
			name, err := getAttributeName(a)
			if err != nil {
				return nil, err
			}
			attrs = append(attrs, name)
		}
		r = mem.New("", attrs, p.c)
	}
	for i := 0; i < cnt; i++ {
		var t, et value.Tuple
		for _, attrs := range as {
			et = append(et, attrs[i])
		}
		for _, a := range p.as {
			v, err := a.E.Eval([]value.Tuple{et, et}, mp)
			if err != nil {
				return nil, err
			}
			t = append(t, v)
		}
		r.AddTuple(t)
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

func getattributes(plh int, as []*Attribute) map[int][]string {
	mp := make(map[int][]string)
	for i, a := range as {
		if i == 0 {
			mp[plh] = a.E.Attributes()[plh]
		} else {
			mp[plh] = append(mp[plh], a.E.Attributes()[plh]...)
		}
	}
	return mp
}
