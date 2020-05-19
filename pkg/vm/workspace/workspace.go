package workspace

import (
	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
)

func New(id string, db string, stg storage.Storage) *workspace {
	return &workspace{
		id:  id,
		db:  db,
		stg: stg,
	}
}

func (w *workspace) Id() string {
	return w.id
}

func (w *workspace) Database() string {
	return w.db
}

func (w *workspace) Relation(name string) (relation.Relation, error) {
	return w.stg.Relation(w.id + "." + name)
}
