package product

import (
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type product struct {
	attrs []string
	us    []unit.Unit
	c     context.Context
}
