package testContext

import (
	"github.com/deepfabric/thinkbase/pkg/vm/estimator"
	"github.com/deepfabric/thinkbase/pkg/vm/workspace"
)

type testContext struct {
	mcpu     int
	rcpu     int
	memSize  int
	diskSize int
	est      estimator.Estimator
	wsp      workspace.Workspace
}
