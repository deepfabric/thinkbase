package union

import "github.com/deepfabric/thinkbase/pkg/algebra/relation"

type Union interface {
	relation.Relation
	Union(int) relation.Relation
}

type union struct {
	IsNub bool
	A, B  relation.Relation
}
