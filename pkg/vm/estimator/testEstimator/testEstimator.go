package testEstimator

import "github.com/deepfabric/thinkbase/pkg/vm/op"

func New() *testEstimator {
	return &testEstimator{}
}

func (e *testEstimator) Min(left, right op.OP) op.OP {
	return left
}

func (e *testEstimator) Less(left, right op.OP) bool {
	return true
}
