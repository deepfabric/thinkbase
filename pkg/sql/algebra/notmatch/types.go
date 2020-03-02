package notmatch

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

type notmatch struct {
	isNub bool
	a, b  relation.Relation
}
