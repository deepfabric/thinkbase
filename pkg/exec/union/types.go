package union

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Union interface {
	Union() (relation.Relation, error)
}

type union struct {
	us []unit.Unit
	c  context.Context
}
