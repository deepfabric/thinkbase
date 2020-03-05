package union

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Union interface {
	Union() (relation.Relation, error)
}

type union struct {
	isNub bool
	us    []unit.Unit
}
