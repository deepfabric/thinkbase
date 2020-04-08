package rule

import "github.com/deepfabric/thinkbase/pkg/vm/op"

type Rule interface {
	Match(op.OP) bool
	Rewrite(op.OP, map[string]op.OP) (op.OP, bool)
}
