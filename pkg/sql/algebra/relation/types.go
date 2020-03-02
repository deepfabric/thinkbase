package relation

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/sql/types"
)

type Relation interface {
	Name() string
	Reference() interface{}
	Metadata() []*AttributeMetadata

	Nub() error

	Rename(string) error
	RenameAttribute(string, string) error

	AddAttribute(*AttributeMetadata) error

	AddTuple(value.Tuple) error
	AddTuples([]value.Tuple) error

	GetTupleCount() (int, error)
	GetTuple(int) (value.Tuple, error)
	GetTuples(int, int) ([]value.Tuple, error)

	GetAttributeIndex(string) (int, error)
	GetAttribute(string) (value.Attribute, error)
	GetAttributeByLimit(string, int, int) (value.Attribute, error)

	Sort([]string, []bool) error
}

type AttributeMetadata struct {
	Name  string        // attribute's name
	Types map[int32]int // type's oid -> count
}

type metadata struct {
	attrs []*AttributeMetadata
}

type relation struct {
	metadata
	name  string
	tuple []value.Tuple
	ref   interface{}
}

type tuples struct {
	descs []bool
	attrs []string
	r     *relation
	tuple []value.Tuple
}

func (t tuples) Len() int { return len(t.tuple) }

func (t tuples) Swap(i, j int) {
	t.tuple[i], t.tuple[j] = t.tuple[j], t.tuple[i]
}

func (t tuples) Less(i, j int) bool {
	return t.r.less(t.tuple[i], t.tuple[j], t.attrs, t.descs)
}

func (r *relation) String() string {
	s := r.name + "\n"
	for i, as := range r.attrs {
		if i > 0 {
			s += ", "
		}
		s += as.Name + "("
		cnt := 0
		for k, v := range as.Types {
			if cnt > 0 {
				s += ", "
			}
			s += fmt.Sprintf("%s.%v", &types.T{Oid: k}, v)
			cnt++
		}
		s += ")"
	}
	s += "\n"
	for _, t := range r.tuple {
		for i, v := range t {
			if i > 0 {
				s += ", "
			}
			s += fmt.Sprintf("%s", v)
		}
		s += "\n"
	}
	return s
}
