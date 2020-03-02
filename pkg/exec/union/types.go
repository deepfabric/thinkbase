package union

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type Union interface {
	Union() (relation.Relation, error)
}

type union struct {
	isNub bool
	us    []unit.Unit
}
