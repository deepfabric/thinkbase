package semi

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/join/natural"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
)

func New(left, right op.OP, c context.Context) op.OP {
	attrs, err := left.AttributeList()
	if err != nil {
		return nil
	}
	var es []*projection.Extend
	for _, attr := range attrs {
		es = append(es, &projection.Extend{E: &extend.Attribute{attr}})
	}
	return projection.New(natural.New(left, right, c), es, c)
}
