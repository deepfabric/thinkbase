package rename

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

func New(r relation.Relation, a string) *rename {
	return &rename{a, r}
}

func (r *rename) Rename() error {
	return r.r.Rename(r.a)
}
