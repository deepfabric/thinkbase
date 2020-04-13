package workspaces

import "github.com/deepfabric/thinkbase/pkg/vm/container/workspace"

type Workspaces interface {
	Destroy() error

	Workspace(string) (workspace.Workspace, error)
}
