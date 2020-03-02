package rename

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

type Rename interface {
	Rename() error
}

type rename struct {
	a string // alias
	r relation.Relation
}
