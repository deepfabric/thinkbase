package order

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

type Order interface {
	Order() (relation.Relation, error)
}

type order struct {
	isNub bool
	descs []bool
	attrs []string
	r     relation.Relation
}
