package nub

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Nub interface {
	Nub() (relation.Relation, error)
}

type nub struct {
	us []unit.Unit
	c  context.Context
}
