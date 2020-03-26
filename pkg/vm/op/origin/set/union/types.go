package union

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type union struct {
	isCheck     bool
	isLeftEmpty bool
	left        op.OP
	right       op.OP
	c           context.Context
	dict        dictionary.Dictionary
}
