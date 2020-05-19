package summarize

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging"
	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(r relation.Relation, fl filter.Filter, es []*Extend, c context.Context) *summarize {
	return &summarize{
		c:       c,
		r:       r,
		es:      es,
		fl:      fl,
		isUsed:  false,
		isCheck: false,
		rows:    r.Rows(),
	}
}

func (n *summarize) Size() float64 {
	var ops []int

	for _, e := range n.es {
		ops = append(ops, e.Op)
	}
	return n.c.SummarizeSize(n.r, ops)
}

func (n *summarize) Cost() float64 {
	var ops []int

	for _, e := range n.es {
		ops = append(ops, e.Op)
	}
	return n.c.SummarizeCost(n.r, ops)
}

func (n *summarize) Dup() op.OP {
	return &summarize{
		c:       n.c,
		r:       n.r,
		es:      n.es,
		fl:      n.fl,
		rows:    n.rows,
		isUsed:  n.isUsed,
		isCheck: n.isCheck,
	}
}

func (n *summarize) SetChild(o op.OP, _ int) { n.r = o.(relation.Relation) }
func (n *summarize) Operate() int            { return op.SummarizeWithIndex }
func (n *summarize) Children() []op.OP       { return []op.OP{n.r} }
func (n *summarize) IsOrdered() bool         { return n.r.IsOrdered() }

func (n *summarize) String() string {
	r := fmt.Sprintf("γ(index, [")
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

func (n *summarize) Name() (string, error) {
	return n.r.Name()
}

func (n *summarize) AttributeList() ([]string, error) {
	return aliasList(n.es), nil
}

func (n *summarize) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	if n.isUsed {
		return nil, nil
	}
	defer func() { n.isUsed = true }()
	es := subExtend(n.es, attrs)
	as = append(as, attributeList(es))
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		if err := util.Contain(attrs, aliasList(es)); err != nil {
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
	for _, e := range es {
		switch e.Op {
		case overload.Avg:
			var err error
			var sum int64
			var count uint64
			var mp *ranging.Ranging

			for row := uint64(0); row < n.rows; row += storage.Segment {
				switch e.Typ {
				case types.T_int:
					mp, err = n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
				case types.T_time:
					mp, err = n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
				case types.T_float:
					mp, err = n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
				}
				if n.fl != nil {
					fp, err := mv.Get(int(row / storage.Segment))
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					n, cnt, err := mp.Sum(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					sum += n
					count += cnt
				} else {
					n, cnt, err := mp.Sum(nil)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					sum += n
					count += cnt
				}
			}
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
		case overload.Max:
			switch e.Typ {
			case types.T_null:
				rq[e.Alias] = append(rq[e.Alias], value.ConstNull)
			case types.T_bool:
				rq[e.Alias] = append(rq[e.Alias], value.NewBool(true))
			case types.T_int:
				var max int64
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					n, cnt, err := mp.Max(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if count == 0 || n > max {
						max = n
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(max))
			case types.T_time:
				var max int64
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					n, cnt, err := mp.Max(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if count == 0 || n > max {
						max = n
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(max, 0)))
			case types.T_float:
				var max int64
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					n, cnt, err := mp.Max(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if count == 0 || n > max {
						max = n
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(max)/float64(storage.Scale)))
			case types.T_string:
				var s string
				var fp *roaring.Bitmap

				flg := true
				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					t, err := n.r.StrMax(e.Name, row, fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if err == storage.NotExist {
						continue
					}
					if flg || strings.Compare(t, s) > 0 {
						s = t
						flg = false
					}
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewString(s))
			}
		case overload.Min:
			switch e.Typ {
			case types.T_null:
				rq[e.Alias] = append(rq[e.Alias], value.ConstNull)
			case types.T_bool:
				rq[e.Alias] = append(rq[e.Alias], value.NewBool(false))
			case types.T_int:
				var min int64
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					n, cnt, err := mp.Min(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if count == 0 || n < min {
						min = n
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(min))
			case types.T_time:
				var min int64
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					n, cnt, err := mp.Min(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if count == 0 || n < min {
						min = n
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(min, 0)))
			case types.T_float:
				var min int64
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					n, cnt, err := mp.Min(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if count == 0 || n < min {
						min = n
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(min)/float64(storage.Scale)))
			case types.T_string:
				var s string
				var fp *roaring.Bitmap

				flg := true
				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					t, err := n.r.StrMin(e.Name, row, fp)
					if err != nil && err != storage.NotExist {
						mv.Destroy()
						return nil, err
					}
					if err == storage.NotExist {
						continue
					}
					if flg || strings.Compare(t, s) < 0 {
						s = t
						flg = false
					}
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewString(s))
			}
		case overload.Sum:
			var err error
			var sum int64
			var mp *ranging.Ranging

			for row := uint64(0); row < n.rows; row += storage.Segment {
				switch e.Typ {
				case types.T_int:
					mp, err = n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
				case types.T_time:
					mp, err = n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
				case types.T_float:
					mp, err = n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
				}
				if n.fl != nil {
					fp, err := mv.Get(int(row / storage.Segment))
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					n, _, err := mp.Sum(fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					sum += n
				} else {
					n, _, err := mp.Sum(nil)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					sum += n
				}
			}
			switch e.Typ {
			case types.T_int:
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(sum))
			case types.T_time:
				rq[e.Alias] = append(rq[e.Alias], value.NewTime(time.Unix(sum, 0)))
			case types.T_float:
				rq[e.Alias] = append(rq[e.Alias], value.NewFloat(float64(sum)/float64(storage.Scale)))
			}
		case overload.Count:
			switch e.Typ {
			case types.T_null:
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					cnt, err := n.r.NullCount(e.Name, row, fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(count)))
			case types.T_bool:
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					cnt, err := n.r.BoolCount(e.Name, row, fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(count)))
			case types.T_int:
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.IntRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					count += mp.Count(fp)
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(count)))
			case types.T_time:
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.TimeRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					count += mp.Count(fp)
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(count)))
			case types.T_float:
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					mp, err := n.r.FloatRangeBitmap(e.Name, row)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					count += mp.Count(fp)
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(count)))
			case types.T_string:
				var count uint64
				var fp *roaring.Bitmap

				for row := uint64(0); row < n.rows; row += storage.Segment {
					if n.fl != nil {
						var err error
						fp, err = mv.Get(int(row / storage.Segment))
						if err != nil {
							mv.Destroy()
							return nil, err
						}
					}
					cnt, err := n.r.StrCount(e.Name, row, fp)
					if err != nil {
						mv.Destroy()
						return nil, err
					}
					count += cnt
				}
				rq[e.Alias] = append(rq[e.Alias], value.NewInt(int64(count)))
			}
		}
	}
	mv.Destroy()
	return rq, nil
}

func (n *summarize) check(attrs []string) error {
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

func aliasList(es []*Extend) []string {
	var rs []string

	for _, e := range es {
		rs = append(rs, e.Alias)
	}
	return rs
}

func attributeList(es []*Extend) []string {
	var rs []string

	for _, e := range es {
		rs = append(rs, e.Name)
	}
	return rs
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
