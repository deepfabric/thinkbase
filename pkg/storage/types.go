package storage

import (
	"errors"
	"sync"

	"github.com/deepfabric/thinkbase/pkg/storage/cache/bitmap"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/rangebitmap"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/relation"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/srangebitmap"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging"
	vrelation "github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkkv/pkg/engine"
)

var (
	NotExist = errors.New("Not Exist")
)

const (
	Segment   = 1 << 20
	RowsLimit = 1 << 52
	Scale     = ranging.Scale
)

type Storage interface {
	Close() error
	Relation(string) (vrelation.Relation, error)
}

type storage struct {
	db   engine.DB
	bc   bitmap.Cache       // bitmap cache
	rc   relation.Cache     // relation cache
	rbc  rangebitmap.Cache  // range bitmap cache
	srbc srangebitmap.Cache // string range bitmap cache
}

// id.C -> rows
// id.S -> size
// id.A -> attribute list
// id.C.attr's name.row number -> value
// id.I.attr's name.BN.block_number        -> bitmap -- null bitmap
// id.I.attr's name.BB.block_number.value  -> bitmap -- bool bitmap
// id.I.attr's name.BS.block_number.value  -> bitmap -- string bitmap
// id.I.attr's name.RBI.block_number -> bitmap -- int range bitmap
// id.I.attr's name.RBF.block_number -> bitmap -- float range bitmap
// id.I.attr's name.RBT.block_number -> bitmap -- time range bitmap
type table struct {
	sync.RWMutex
	pos   uint64
	id    string // uid.database_name.table_name
	name  string // table name
	rows  uint64
	size  uint64
	s     *storage
	attrs []string
	db    engine.DB
	mp    map[string]struct{}
}
