package database

import "github.com/deepfabric/thinkbase/pkg/vm/container/relation"

type Database interface {
	Destroy() error

	Relation(string) (relation.Relation, error)
}
