package testunit

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

type minusUnit struct {
	a, b relation.Relation
}

type unionUnit struct {
	a relation.Relation
}

type intersectUnit struct {
	a, b relation.Relation
}
