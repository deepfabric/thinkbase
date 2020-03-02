package join

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

type Join interface {
	Join() (relation.Relation, error)
}
