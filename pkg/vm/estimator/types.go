package estimator

import "github.com/deepfabric/thinkbase/pkg/vm/op"

type Estimator interface {
	Less(op.OP, op.OP) bool

	Min(op.OP, op.OP) op.OP
}
