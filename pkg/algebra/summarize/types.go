package summarize

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Attribute struct {
	Name  string
	Alias string
}

type Summarize interface {
	Summarize() (relation.Relation, error)
}

type summarize struct {
	gs   []string
	as   []*Attribute // array of aggregation function attribute
	c    context.Context
	r    relation.Relation
	aggs []overload.Aggregation
}
