package rule

import "github.com/deepfabric/thinkbase/pkg/vm/op"

type Rule interface {
	Match(op.OP, map[string]op.OP) bool
	Rewrite(op.OP, map[string]op.OP, map[string]int32, map[string]int32) (op.OP, bool)
}
