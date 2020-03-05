package join

import "github.com/deepfabric/thinkbase/pkg/algebra/relation"

type Join interface {
	Join() (relation.Relation, error)
}
