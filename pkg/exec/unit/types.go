package unit

import "github.com/deepfabric/thinkbase/pkg/algebra/relation"

const (
	Intersect = iota
	FullJoin
	InnerJoin
	LeftJoin
	Match
	NaturalJoin
	Product
	RightJoin
	Minus
	Notmatch
	Order
	Restrict
	Summarize
	Union
	Xunion
)

type Unit interface {
	Result() (relation.Relation, error)
}
