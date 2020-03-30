package inner

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/product"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
)

func New(left, right op.OP, e extend.Extend, c context.Context) op.OP {
	return restrict.New(product.New(left, right, c), e, c)
}
