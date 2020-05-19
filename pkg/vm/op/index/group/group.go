package group

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(r relation.Relation, fl filter.Filter, ts []int, gs []string, es []*Extend, c context.Context) *group {
	return &group{
		c:       c,
		r:       r,
		es:      es,
		fl:      fl,
		ts:      ts,
		gs:      gs,
		isUsed:  false,
		isCheck: false,
		rows:    r.Rows(),
	}
}

func (n *group) Size() float64 {
	var ops []int

	for _, e := range n.es {
		ops = append(ops, e.Op)
	}
	return n.c.GroupSize(n.r, n.gs, ops)
}

func (n *group) Cost() float64 {
	var ops []int

	for _, e := range n.es {
		ops = append(ops, e.Op)
	}
	return n.c.GroupCost(n.r, n.gs, ops)
}

func (n *group) Dup() op.OP {
	return &group{
		c:       n.c,
		r:       n.r,
		es:      n.es,
		gs:      n.gs,
		fl:      n.fl,
		ts:      n.ts,
		rows:    n.rows,
		isUsed:  n.isUsed,
		isCheck: n.isCheck,
	}
}

func (n *group) SetChild(o op.OP, _ int) { n.r = o.(relation.Relation) }
func (n *group) Operate() int            { return op.GroupWithIndex }
func (n *group) Children() []op.OP       { return []op.OP{n.r} }
func (n *group) IsOrdered() bool         { return n.r.IsOrdered() }

func (n *group) String() string {
	r := fmt.Sprintf("γ(index, [")
	for i, g := range n.gs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", g)
		default:
			r += fmt.Sprintf(", %s", g)
		}
	}
	r += "], ["
	for i, e := range n.es {
		switch i {
		case 0:
			r += fmt.Sprintf("%s(%s) -> %s", overload.AggName[e.Op], e.Name, e.Alias)
		default:
			r += fmt.Sprintf(", %s(%s) -> %s", overload.AggName[e.Op], e.Name, e.Alias)
		}
	}
	if n.fl == nil {
		r += fmt.Sprintf("], %s)", n.r)
	} else {
		r += fmt.Sprintf("], σ(%s, %s)", n.fl, n.r)
	}
	return r
}

func (n *group) Name() (string, error) {
	return n.r.Name()
}

func (n *group) AttributeList() ([]string, error) {
	return aliasList(n.es, n.gs), nil
}

// 根据代价选择，采用该实现的前提是组的数目比较少
func (n *group) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	if n.isUsed {
		return nil, nil
	}
	defer func() { n.isUsed = true }()
	es := subExtend(n.es, attrs)
	as = append(as, attributeList(es, n.gs))
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		if err := util.Contain(attrs, aliasList(es, n.gs)); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	mv, err := n.c.NewMvector()
	if err != nil {
		return nil, err
	}
	rq := make(map[string]value.Array)
	if n.fl != nil {
		for row := uint64(0); row < n.rows; row += storage.Segment {
			mp, err := n.fl.Bitmap(n.r, row)
			if err != nil {
				mv.Destroy()
				return nil, err
			}
			mv.Append([]*roaring.Bitmap{mp})
		}
	}
	var ms []mdictionary.Mdictionary
	for row := uint64(0); row < n.rows; row += storage.Segment {
		m, err := n.getBitmap(row)
		if err != nil {
			for _, m := range ms {
				m.Destroy()
			}
			mv.Destroy()
			return nil, err
		}
		ms = append(ms, m)
	}
	if len(ms) == 0 {
		return rq, nil
	}
	var ks []string
	for i, e := range es {
		switch e.Op {
		case overload.Avg:
			var err error
			var fp *roaring.Bitmap
			var mp *ranging.Ranging

			smp, cmp := make(map[string]int64), make(map[string]uint64)
			for row := uint64(0); row < n.rows; row += storage.Segment {
				switch e.Typ {
				case types.T_int:
					mp, err = n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				case types.T_time:
					mp, err = n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				case types.T_float:
					mp, err = n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				if n.fl != nil {
					fp, err = mv.Get(int(row / storage.Segment))
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				m := ms[row/storage.Segment]
				m.Range(func(k string, v *roaring.Bitmap) {
					if v == nil || v.Count() == 0 {
						return
					}
					v = v.Intersect(fp)
					if v == nil || v.Count() == 0 {
						return
					}
					n, cnt, privErr := mp.Sum(v)
					if privErr != nil {
						err = privErr
						return
					}
					smp[k] += n
					cmp[k] += cnt
				})
				if err != nil {
					for _, m := range ms {
						m.Destroy()
					}
					mv.Destroy()
					return nil, err
				}
			}
			switch {
			case i == 0:
				for k, v := range smp {
					a, err := decodeKey(k)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					for i, g := range n.gs {
						rq[g] = append(rq[g], a[i])
					}
					ks = append(ks, k)
					sum := v
					count := cmp[k]
					switch e.Typ {
					case types.T_int:
						if count > 0 {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(sum)/float64(count)))
						} else {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(0.0))
						}
					case types.T_time:
						if count > 0 {
							rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(sum/int64(count), 0)))
						} else {
							rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(0, 0)))
						}
					case types.T_float:
						if count > 0 {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(sum)/float64(count*storage.Scale)))
						} else {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(0.0))
						}
					}
				}
			default:
				for _, k := range ks {
					sum := smp[k]
					count := cmp[k]
					switch e.Typ {
					case types.T_int:
						if count > 0 {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(sum)/float64(count)))
						} else {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(0.0))
						}
					case types.T_time:
						if count > 0 {
							rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(sum/int64(count), 0)))
						} else {
							rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(0, 0)))
						}
					case types.T_float:
						if count > 0 {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(sum)/float64(count*storage.Scale)))
						} else {
							rq[e.Alias] = append(rq[e.Alias], value.NewFloat(0.0))
						}
					}
				}
			}
		case overload.Max:
			switch e.Typ {
			case types.T_null:
				smp := make(map[string]struct{})
				for row := uint64(0); row < n.rows; row += storage.Segment {
					var err error
					var fp *roaring.Bitmap

					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						smp[k] = struct{}{}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, _ := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.ConstNull)
					}
				default:
					for _ = range ks {
						rq[e.Alias] = append(rq[e.Alias], value.ConstNull)
					}
				}
			case types.T_bool:
				smp := make(map[string]struct{})
				for row := uint64(0); row < n.rows; row += storage.Segment {
					var err error
					var fp *roaring.Bitmap

					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						smp[k] = struct{}{}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, _ := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewBool(true))
					}
				default:
					for _ = range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewBool(true))
					}
				}
			case types.T_int:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]int64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						n, _, privErr := mp.Max(v)
						if privErr != nil {
							err = privErr
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = n
						} else if n > smp[k] {
							smp[k] = n
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(v))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(smp[k]))
					}
				}
			case types.T_time:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]int64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						n, _, privErr := mp.Max(v)
						if privErr != nil {
							err = privErr
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = n
						} else if n > smp[k] {
							smp[k] = n
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(v, 0)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(smp[k], 0)))
					}
				}
			case types.T_float:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]int64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						n, _, privErr := mp.Max(v)
						if privErr != nil {
							err = privErr
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = n
						} else if n > smp[k] {
							smp[k] = n
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(v)/float64(storage.Scale)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(smp[k])/float64(storage.Scale)))
					}
				}
			case types.T_string:
				var err error
				var fp *roaring.Bitmap

				smp := make(map[string]string)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						s, privErr := n.r.StrMax(e.Name, row, v)
						if privErr != nil && privErr != storage.NotExist {
							err = privErr
							return
						}
						if privErr == storage.NotExist {
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = s
						} else if strings.Compare(s, smp[k]) > 0 {
							smp[k] = s
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewString(v))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewString(smp[k]))
					}
				}
			}
		case overload.Min:
			switch e.Typ {
			case types.T_null:
				smp := make(map[string]struct{})
				for row := uint64(0); row < n.rows; row += storage.Segment {
					var err error
					var fp *roaring.Bitmap

					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						smp[k] = struct{}{}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, _ := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.ConstNull)
					}
				default:
					for _ = range ks {
						rq[e.Alias] = append(rq[e.Alias], value.ConstNull)
					}
				}
			case types.T_bool:
				smp := make(map[string]struct{})
				for row := uint64(0); row < n.rows; row += storage.Segment {
					var err error
					var fp *roaring.Bitmap

					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						smp[k] = struct{}{}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, _ := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewBool(false))
					}
				default:
					for _ = range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewBool(false))
					}
				}
			case types.T_int:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]int64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						n, _, privErr := mp.Min(v)
						if privErr != nil {
							err = privErr
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = n
						} else if n < smp[k] {
							smp[k] = n
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(v))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(smp[k]))
					}
				}
			case types.T_time:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]int64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						n, _, privErr := mp.Min(v)
						if privErr != nil {
							err = privErr
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = n
						} else if n < smp[k] {
							smp[k] = n
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(v, 0)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(smp[k], 0)))
					}
				}
			case types.T_float:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]int64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						n, _, privErr := mp.Min(v)
						if privErr != nil {
							err = privErr
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = n
						} else if n < smp[k] {
							smp[k] = n
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(v)/float64(storage.Scale)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(smp[k])/float64(storage.Scale)))
					}
				}
			case types.T_string:
				var err error
				var fp *roaring.Bitmap

				smp := make(map[string]string)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						s, privErr := n.r.StrMin(e.Name, row, v)
						if privErr != nil && privErr != storage.NotExist {
							err = privErr
							return
						}
						if privErr == storage.NotExist {
							return
						}
						if _, ok := smp[k]; !ok {
							smp[k] = s
						} else if strings.Compare(s, smp[k]) < 0 {
							smp[k] = s
						}
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewString(v))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewString(smp[k]))
					}
				}
			}
		case overload.Sum:
			var err error
			var fp *roaring.Bitmap
			var mp *ranging.Ranging

			smp := make(map[string]int64)
			for row := uint64(0); row < n.rows; row += storage.Segment {
				switch e.Typ {
				case types.T_int:
					mp, err = n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				case types.T_time:
					mp, err = n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				case types.T_float:
					mp, err = n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				if n.fl != nil {
					fp, err = mv.Get(int(row / storage.Segment))
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				m := ms[row/storage.Segment]
				m.Range(func(k string, v *roaring.Bitmap) {
					if v == nil || v.Count() == 0 {
						return
					}
					v = v.Intersect(fp)
					if v == nil || v.Count() == 0 {
						return
					}
					n, _, privErr := mp.Sum(v)
					if privErr != nil {
						err = privErr
						return
					}
					smp[k] += n
				})
				if err != nil {
					for _, m := range ms {
						m.Destroy()
					}
					mv.Destroy()
					return nil, err
				}
			}
			switch {
			case i == 0:
				for k, v := range smp {
					a, err := decodeKey(k)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					for i, g := range n.gs {
						rq[g] = append(rq[g], a[i])
					}
					sum := v
					ks = append(ks, k)
					switch e.Typ {
					case types.T_int:
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(sum))
					case types.T_time:
						rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(sum, 0)))
					case types.T_float:
						rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(sum)/float64(storage.Scale)))
					}
				}
			default:
				for _, k := range ks {
					sum := smp[k]
					switch e.Typ {
					case types.T_int:
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(sum))
					case types.T_time:
						rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(sum, 0)))
					case types.T_float:
						rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(sum)/float64(storage.Scale)))
					}
				}
			}
		case overload.Count:
			switch e.Typ {
			case types.T_null:
				smp := make(map[string]uint64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					var err error
					var fp *roaring.Bitmap

					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						cnt, privErr := n.r.NullCount(e.Name, row, v)
						if privErr != nil {
							err = privErr
							return
						}
						smp[k] += cnt
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(v)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(smp[k])))
					}
				}
			case types.T_bool:
				smp := make(map[string]uint64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					var err error
					var fp *roaring.Bitmap

					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						cnt, privErr := n.r.BoolCount(e.Name, row, v)
						if privErr != nil {
							err = privErr
							return
						}
						smp[k] += cnt
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(v)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(smp[k])))
					}
				}
			case types.T_int:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]uint64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						smp[k] += mp.Count(v)
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(v)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(smp[k])))
					}
				}
			case types.T_time:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]uint64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						smp[k] += mp.Count(v)
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(v)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(smp[k])))
					}
				}
			case types.T_float:
				var err error
				var fp *roaring.Bitmap
				var mp *ranging.Ranging

				smp := make(map[string]uint64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err = n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						smp[k] += mp.Count(v)
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(v)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(smp[k])))
					}
				}
			case types.T_string:
				var err error
				var fp *roaring.Bitmap

				smp := make(map[string]uint64)
				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
					}
					m := ms[row/storage.Segment]
					m.Range(func(k string, v *roaring.Bitmap) {
						if v == nil || v.Count() == 0 {
							return
						}
						v = v.Intersect(fp)
						if v == nil || v.Count() == 0 {
							return
						}
						cnt, privErr := n.r.StrCount(e.Name, row, v)
						if privErr != nil {
							err = privErr
							return
						}
						smp[k] += cnt
					})
					if err != nil {
						for _, m := range ms {
							m.Destroy()
						}
						mv.Destroy()
						return nil, err
					}
				}
				switch {
				case i == 0:
					for k, v := range smp {
						a, err := decodeKey(k)
						if err != nil {
							for _, m := range ms {
								m.Destroy()
							}
							mv.Destroy()
							return nil, err
						}
						for i, g := range n.gs {
							rq[g] = append(rq[g], a[i])
						}
						ks = append(ks, k)
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(v)))
					}
				default:
					for _, k := range ks {
						rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(smp[k])))
					}
				}
			}
		}
	}
	return rq, nil
}

func (n *group) groupBy(m mdictionary.Mdictionary, ms []mdictionary.Mdictionary) (mdictionary.Mdictionary, error) {
	switch len(ms) {
	case 0:
		return m, nil
	case 1:
		rq, err := n.c.NewMdictionary()
		if err != nil {
			return nil, err
		}
		m.Range(func(k0 string, v0 *roaring.Bitmap) {
			ms[0].Range(func(k1 string, v1 *roaring.Bitmap) {
				if mq := v0.Intersect(v1); mq.Count() > 0 {
					k, _ := joinKey(k0, k1)
					rq.Set(k, mq)
				}
			})
		})
		return rq, nil
	default:
		rq, err := n.c.NewMdictionary()
		if err != nil {
			return nil, err
		}
		m.Range(func(k0 string, v0 *roaring.Bitmap) {
			ms[0].Range(func(k1 string, v1 *roaring.Bitmap) {
				if mq := v0.Intersect(v1); mq.Count() > 0 {
					k, _ := joinKey(k0, k1)
					rq.Set(k, mq)
				}
			})
		})
		return n.groupBy(rq, ms[1:])
	}
}

func (n *group) getBitmap(row uint64) (mdictionary.Mdictionary, error) {
	var ms []mdictionary.Mdictionary

	defer func() {
		for _, m := range ms {
			m.Destroy()
		}
	}()
	for i, g := range n.gs {
		switch n.ts[i] {
		case types.T_int:
			m, err := n.c.NewMdictionary()
			if err != nil {
				return nil, err
			}
			if err := n.r.IntBitmapFold(g, row, m); err != nil {
				m.Destroy()
				return nil, err
			}
			ms = append(ms, m)
		case types.T_null:
			m, err := n.c.NewMdictionary()
			if err != nil {
				return nil, err
			}
			if err := n.r.NullBitmapFold(g, row, m); err != nil {
				m.Destroy()
				return nil, err
			}
			ms = append(ms, m)
		case types.T_bool:
			m, err := n.c.NewMdictionary()
			if err != nil {
				return nil, err
			}
			if err := n.r.BoolBitmapFold(g, row, m); err != nil {
				m.Destroy()
				return nil, err
			}
			ms = append(ms, m)
		case types.T_time:
			m, err := n.c.NewMdictionary()
			if err != nil {
				return nil, err
			}
			if err := n.r.TimeBitmapFold(g, row, m); err != nil {
				m.Destroy()
				return nil, err
			}
			ms = append(ms, m)
		case types.T_float:
			m, err := n.c.NewMdictionary()
			if err != nil {
				return nil, err
			}
			if err := n.r.FloatBitmapFold(g, row, m); err != nil {
				m.Destroy()
				return nil, err
			}
			ms = append(ms, m)
		case types.T_string:
			m, err := n.c.NewMdictionary()
			if err != nil {
				return nil, err
			}
			if err := n.r.StringBitmapFold(g, row, m); err != nil {
				m.Destroy()
				return nil, err
			}
			ms = append(ms, m)
		}
	}
	switch len(ms) {
	case 0:
		return nil, nil
	case 1:
		rq, err := n.c.NewMdictionary()
		if err != nil {
			return nil, err
		}
		ms[0].Range(func(k string, v *roaring.Bitmap) {
			val, _, _ := encoding.DecodeValue([]byte(k))
			key, _ := encoding.EncodeValue(value.Array{val.(value.Value)})
			rq.Set(string(key), v)
		})
		return rq, nil
	default:
		return n.groupBy(ms[0], ms[1:])
	}
}

func (n *group) check(attrs []string) error {
	{
		for i, j := 0, len(n.es); i < j; i++ {
			if len(n.es[i].Name) == 0 {
				return errors.New("need attribute")
			}
			switch n.es[i].Op {
			case overload.Avg:
			case overload.Max:
			case overload.Min:
			case overload.Sum:
			case overload.Count:
			default:
				return fmt.Errorf("unsupport aggreation operator '%v'", n.es[i].Op)
			}
		}
	}
	as, err := n.r.AttributeList()
	if err != nil {
		return err
	}
	mp := make(map[string]struct{})
	for _, a := range as {
		mp[a] = struct{}{}
	}
	for _, attr := range attrs {
		if _, ok := mp[attr]; !ok {
			return fmt.Errorf("failed to find attribute '%s'", attr)
		}
	}
	return nil
}

func aliasList(es []*Extend, attrs []string) []string {
	var rs []string

	for _, e := range es {
		rs = append(rs, e.Alias)
	}
	return util.MergeAttributes(attrs, rs)
}

func attributeList(es []*Extend, attrs []string) []string {
	var rs []string

	for _, e := range es {
		rs = append(rs, e.Name)
	}
	return util.MergeAttributes(attrs, rs)
}

func subExtend(es []*Extend, attrs []string) []*Extend {
	var rs []*Extend

	mp := make(map[string]struct{})
	for i, j := 0, len(attrs); i < j; i++ {
		mp[attrs[i]] = struct{}{}
	}
	for i, j := 0, len(es); i < j; i++ {
		if _, ok := mp[es[i].Alias]; ok {
			rs = append(rs, es[i])
		}
	}
	return rs
}

func decodeKey(k string) (value.Array, error) {
	v, _, err := encoding.DecodeValue([]byte(k))
	if err != nil {
		return nil, err
	}
	return v.(value.Array), nil
}

func joinKey(k0, k1 string) (string, error) {
	var a value.Array

	v0, _, err := encoding.DecodeValue([]byte(k0))
	if err != nil {
		return "", err
	}
	v1, _, err := encoding.DecodeValue([]byte(k1))
	if err != nil {
		return "", err
	}
	switch t := v0.(type) {
	case value.Array:
		a = append(a, t...)
	default:
		a = append(a, v0.(value.Value))
	}
	a = append(a, v1.(value.Value))
	k, err := encoding.EncodeValue(a)
	if err != nil {
		return "", err
	}
	return string(k), nil
}
