package opt

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule"
)

type optimizer struct {
	o  op.OP
	rp map[int][]rule.Rule
}
