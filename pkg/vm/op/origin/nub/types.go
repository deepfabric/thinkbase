package nub

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type nub struct {
	isCheck bool
	prev    op.OP
	attrs   []string
	c       context.Context
	dict    dictionary.Dictionary
}
