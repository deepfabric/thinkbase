package relation

import vrelation "github.com/deepfabric/thinkbase/pkg/vm/container/relation"

type Cache interface {
	Set(string, vrelation.Relation)
	Get(string) (vrelation.Relation, bool)
}
