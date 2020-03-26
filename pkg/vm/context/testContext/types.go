package testContext

import "github.com/deepfabric/thinkbase/pkg/vm/estimator"

type testContext struct {
	mcpu     int
	rcpu     int
	memSize  int
	diskSize int
	est      estimator.Estimator
}
