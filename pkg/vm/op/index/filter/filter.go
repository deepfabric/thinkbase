package filter

import (
	"bytes"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
)

func New(cs []*Condition) *filter {
	return &filter{cs}
}

func (f *filter) String() string {
	var buf bytes.Buffer

	for i, c := range f.cs {
		if i > 0 {
			if c.IsOr {
				buf.WriteString(" OR ")
			} else {
				buf.WriteString(" AND ")
			}
		}
		switch c.Op {
		case EQ:
			buf.WriteString(fmt.Sprintf("%s = %s", c.Name, c.Val))
		case NE:
			buf.WriteString(fmt.Sprintf("%s <> %s", c.Name, c.Val))
		case LT:
			buf.WriteString(fmt.Sprintf("%s < %s", c.Name, c.Val))
		case LE:
			buf.WriteString(fmt.Sprintf("%s <= %s", c.Name, c.Val))
		case GT:
			buf.WriteString(fmt.Sprintf("%s > %s", c.Name, c.Val))
		case GE:
			buf.WriteString(fmt.Sprintf("%s >= %s", c.Name, c.Val))
		}
	}
	return buf.String()
}

func (f *filter) Bitmap(r relation.Relation, row uint64) (*roaring.Bitmap, error) {
	var ms []*roaring.Bitmap

	for _, c := range f.cs {
		switch c.Op {
		case EQ:
			mp, err := r.Eq(c.Name, c.Val, row)
			if err != nil {
				return nil, err
			}
			ms = append(ms, mp)
		case NE:
			mp, err := r.Ne(c.Name, c.Val, row)
			if err != nil {
				return nil, err
			}
			ms = append(ms, mp)
		case LT:
			mp, err := r.Lt(c.Name, c.Val, row)
			if err != nil {
				return nil, err
			}
			ms = append(ms, mp)
		case LE:
			mp, err := r.Le(c.Name, c.Val, row)
			if err != nil {
				return nil, err
			}
			ms = append(ms, mp)
		case GT:
			mp, err := r.Gt(c.Name, c.Val, row)
			if err != nil {
				return nil, err
			}
			ms = append(ms, mp)
		case GE:
			mp, err := r.Ge(c.Name, c.Val, row)
			if err != nil {
				return nil, err
			}
			ms = append(ms, mp)
		}
	}
	mp := ms[0]
	for i, j := 1, len(ms); i < j; i++ {
		if f.cs[i].IsOr {
			mp = mp.Union(ms[i])
		} else {
			mp = mp.Intersect(ms[i])
		}
	}
	return mp, nil
}
