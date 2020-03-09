package summarize

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	asummarize "github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Summarize interface {
	Summarize() (relation.Relation, error)
}

type summarize struct {
	ops   []int
	gs    []string
	avgs  []string
	us    []unit.Unit
	c     context.Context
	r     relation.Relation
	attrs []*asummarize.Attribute
}
