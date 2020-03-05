package summarize

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
)

type Attribute struct {
	Name  string
	Alias string
}

type Summarize interface {
	Summarize() (relation.Relation, error)
}

type summarize struct {
	is   []int // array of group by attribute's index
	gs   []string
	as   []*Attribute // array of aggregation function attribute
	r    relation.Relation
	aggs []overload.Aggregation
}
