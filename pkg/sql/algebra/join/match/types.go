package match

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

// semi join
type match struct {
	a, b relation.Relation
}
