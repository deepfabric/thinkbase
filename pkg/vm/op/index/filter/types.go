package filter

import (
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

const (
	EQ = iota
	NE
	LT
	LE
	GT
	GE
)

type Condition struct {
	IsOr bool
	Op   int // eq, ne, lt, le, gt, ge
	Name string
	Val  value.Value
}

type Filter interface {
	String() string
	Bitmap(relation.Relation, uint64) (*roaring.Bitmap, error)
}

type filter struct {
	cs []*Condition
}
