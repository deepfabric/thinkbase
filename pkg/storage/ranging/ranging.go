package ranging

import (
	"bytes"
	"errors"
	"time"

	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *Ranging {
	return &Ranging{
		mp: roaring.NewBitmap(),
	}
}

func (r *Ranging) Map() *roaring.Bitmap {
	return r.subMap(bsiExistsBit)
}

func (r *Ranging) Fold(typ int, dict mdictionary.Mdictionary) error {
	var err error

	mp := r.subMap(bsiExistsBit)
	fp := make(map[int64]struct{})
	mp.ForEach(func(i uint64) {
		v, ok := r.value(i)
		if ok {
			if _, ok := fp[v]; !ok {
				fp[v] = struct{}{}
				mq, privErr := r.Eq(v)
				if privErr != nil {
					err = privErr
				}
				switch typ {
				case Int:
					k, privErr := encoding.EncodeValue(value.NewInt(v))
					if privErr != nil {
						err = privErr
					}
					dict.Set(string(k), mq)
				case Time:
					k, privErr := encoding.EncodeValue(value.NewTime(time.Unix(v, 0)))
					if privErr != nil {
						err = privErr
					}
					dict.Set(string(k), mq)
				case Float:
					k, privErr := encoding.EncodeValue(value.NewFloat(float64(v) / Scale))
					if privErr != nil {
						err = privErr
					}
					dict.Set(string(k), mq)
				}
			}
		}
	})
	return err
}

func (r *Ranging) Clone() *Ranging {
	return &Ranging{
		mp: r.mp.Clone(),
	}
}

func (r *Ranging) Show() ([]byte, error) {
	var buf bytes.Buffer

	if _, err := r.mp.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *Ranging) Read(data []byte) error {
	return r.mp.UnmarshalBinary(data)
}

func (r *Ranging) Get(k uint64) (int64, bool) {
	var v int64

	if r.bit(uint64(bsiExistsBit), k) {
		return -1, false
	}
	for i := uint(0); i < 64; i++ {
		if r.bit(uint64(bsiOffsetBit+i), k) {
			v |= (1 << i)
		}
	}
	if r.bit(uint64(bsiSignBit), k) {
		v = -v
	}
	return v, true
}

func (r *Ranging) Set(k uint64, v int64) error {
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
	}
	for i := uint(0); i < 64; i++ {
		if uv&(1<<i) != 0 {
			if err := r.setBit(uint64(bsiOffsetBit+i), k); err != nil {
				return err
			}
		} else {
			if err := r.clearBit(uint64(bsiOffsetBit+i), k); err != nil {
				return err
			}
		}
	}
	if err := r.setBit(uint64(bsiExistsBit), k); err != nil {
		return err
	}
	if v < 0 {
		if err := r.setBit(uint64(bsiSignBit), k); err != nil {
			return err
		}
	} else {
		if err := r.clearBit(uint64(bsiSignBit), k); err != nil {
			return err
		}
	}
	return nil
}

func (r *Ranging) Del(k uint64, v int64) error {
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
	}
	for i := uint(0); i < 64; i++ {
		if uv&(1<<i) != 0 {
			if err := r.setBit(uint64(bsiOffsetBit+i), k); err != nil {
				return err
			}
		} else {
			if err := r.clearBit(uint64(bsiOffsetBit+i), k); err != nil {
				return err
			}
		}
	}
	if err := r.clearBit(uint64(bsiExistsBit), k); err != nil {
		return err
	}
	if err := r.setBit(uint64(bsiSignBit), k); err != nil {
		return err
	}
	return nil
}

func (r *Ranging) Sum(filter *roaring.Bitmap) (int64, uint64, error) {
	var sum int64

	mp := r.subMap(bsiExistsBit)
	if filter != nil {
		mp = mp.Intersect(filter)
	}
	count := mp.Count()
	nmp := r.subMap(bsiSignBit)
	pmp := mp.Difference(nmp)
	for i := uint(0); i < 64; i++ {
		mq := r.subMap(uint64(bsiOffsetBit + i))
		n := int64((1 << i) * mq.IntersectionCount(pmp))
		m := int64((1 << i) * mq.IntersectionCount(nmp))
		sum += n - m
	}
	return sum, count, nil
}

func (r *Ranging) Count(filter *roaring.Bitmap) uint64 {
	mp := r.subMap(bsiExistsBit)
	if filter != nil {
		mp = mp.Intersect(filter)
	}
	return mp.Count()
}

func (r *Ranging) Min(filter *roaring.Bitmap) (int64, uint64, error) {
	mp := r.subMap(bsiExistsBit)
	if filter != nil {
		mp = mp.Intersect(filter)
	}
	if mp.Count() == 0 {
		return 0, 0, nil
	}
	if mq := r.subMap(bsiSignBit).Intersect(mp); mq.Any() {
		min, count := r.maxUnsigned(mq)
		return -min, count, nil
	}
	min, count := r.minUnsigned(mp)
	return min, count, nil
}

func (r *Ranging) Max(filter *roaring.Bitmap) (int64, uint64, error) {
	mp := r.subMap(bsiExistsBit)
	if filter != nil {
		mp = mp.Intersect(filter)
	}
	if !mp.Any() {
		return 0, 0, nil
	}
	mq := mp.Difference(r.subMap(bsiSignBit))
	if !mq.Any() {
		max, count := r.minUnsigned(mp)
		return -max, count, nil
	}
	max, count := r.maxUnsigned(mq)
	return max, count, nil
}

func (r *Ranging) Eq(v int64) (*roaring.Bitmap, error) {
	mp := r.subMap(bsiExistsBit)
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
		mp = mp.Intersect(r.subMap(bsiSignBit))
	} else {
		mp = mp.Difference(r.subMap(bsiSignBit))
	}
	for i := 63; i >= 0; i-- {
		if (uv>>uint(i))&1 == 1 {
			mp = mp.Intersect(r.subMap(uint64(bsiOffsetBit + i)))
		} else {
			mp = mp.Difference(r.subMap(uint64(bsiOffsetBit + i)))
		}
	}
	return mp, nil
}

func (r *Ranging) Ne(v int64) (*roaring.Bitmap, error) {
	mp := r.subMap(bsiExistsBit)
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
		mp = mp.Intersect(r.subMap(bsiSignBit))
	} else {
		mp = mp.Difference(r.subMap(bsiSignBit))
	}
	for i := 63; i >= 0; i-- {
		if (uv>>uint(i))&1 == 1 {
			mp = mp.Intersect(r.subMap(uint64(bsiOffsetBit + i)))
		} else {
			mp = mp.Difference(r.subMap(uint64(bsiOffsetBit + i)))
		}
	}
	return r.subMap(bsiExistsBit).Difference(mp), nil
}

func (r *Ranging) Lt(v int64) (*roaring.Bitmap, error) {
	mp := r.subMap(bsiExistsBit)
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
	}
	if v >= -1 {
		mp = r.lt(uv, mp.Difference(r.subMap(bsiSignBit)), false)
		return r.subMap(bsiSignBit).Union(mp), nil
	}
	return r.gt(uv, mp.Intersect(r.subMap(bsiSignBit)), false), nil
}

func (r *Ranging) Le(v int64) (*roaring.Bitmap, error) {
	mp := r.subMap(bsiExistsBit)
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
	}
	if v >= 0 {
		mp = r.lt(uv, mp.Difference(r.subMap(bsiSignBit)), true)
		return r.subMap(bsiSignBit).Union(mp), nil
	}
	return r.gt(uv, mp.Intersect(r.subMap(bsiSignBit)), true), nil
}

func (r *Ranging) Gt(v int64) (*roaring.Bitmap, error) {
	mp := r.subMap(bsiExistsBit)
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
	}
	if v >= -1 {
		return r.gt(uv, mp.Difference(r.subMap(uint64(bsiSignBit))), false), nil
	}
	mq := r.lt(uv, mp.Intersect(r.subMap(uint64(bsiSignBit))), false)
	return mp.Difference(r.subMap(uint64(bsiSignBit))).Union(mq), nil
}

func (r *Ranging) Ge(v int64) (*roaring.Bitmap, error) {
	mp := r.subMap(bsiExistsBit)
	uv := uint64(v)
	if v < 0 {
		uv = uint64(-v)
	}
	if v >= -1 {
		return r.gt(uv, mp.Difference(r.subMap(uint64(bsiSignBit))), true), nil
	}
	mq := r.lt(uv, mp.Intersect(r.subMap(uint64(bsiSignBit))), true)
	return mp.Difference(r.subMap(uint64(bsiSignBit))).Union(mq), nil
}

func (r *Ranging) Between(x, y int64) (*roaring.Bitmap, error) {
	mp := r.subMap(bsiExistsBit)
	ux, uy := uint64(x), uint64(y)
	if x < 0 {
		ux = uint64(-x)
	}
	if y < 0 {
		uy = uint64(-y)
	}
	if x >= 0 {
		return r.between(ux, uy, mp.Difference(r.subMap(bsiSignBit))), nil
	}
	if y < 0 {
		return r.between(uy, ux, mp.Intersect(r.subMap(bsiSignBit))), nil
	}
	return r.lt(uy, mp.Difference(r.subMap(bsiSignBit)), true).Union(
		r.lt(ux, mp.Intersect(r.subMap(bsiSignBit)), true)), nil
}

func (r *Ranging) lt(v uint64, mp *roaring.Bitmap, eq bool) *roaring.Bitmap {
	zflg := true // leading zero flag
	mq := roaring.NewBitmap()
	for i := 63; i >= 0; i-- {
		bit := (v >> uint(i)) & 1
		if zflg {
			if bit == 0 {
				mp = mp.Difference(r.subMap(uint64(bsiOffsetBit + i)))
				continue
			} else {
				zflg = false
			}
		}
		if i == 0 && !eq {
			if bit == 0 {
				return mq
			}
			return mp.Difference(r.subMap(uint64(bsiOffsetBit + i)).Difference(mq))
		}
		if bit == 0 {
			mp = mp.Difference(r.subMap(uint64(bsiOffsetBit + i)).Difference(mq))
			continue
		}
		if i > 0 {
			mq = mq.Union(mp.Difference(r.subMap(uint64(bsiOffsetBit + i))))
		}
	}
	return mp
}

func (r *Ranging) gt(v uint64, mp *roaring.Bitmap, eq bool) *roaring.Bitmap {
	mq := roaring.NewBitmap()
	for i := 63; i >= 0; i-- {
		bit := (v >> uint(i)) & 1
		if i == 0 && !eq {
			if bit == 1 {
				return mq
			}
			return mp.Difference(mp.Difference(r.subMap(uint64(bsiOffsetBit + i))).Difference(mq))
		}
		if bit == 1 {
			mp = mp.Difference(mp.Difference(r.subMap(uint64(bsiOffsetBit + i))).Difference(mq))
			continue
		}
		if i > 0 {
			mq = mq.Union(mp.Intersect(r.subMap(uint64(bsiOffsetBit + i))))
		}
	}
	return mp
}

func (r *Ranging) between(x, y uint64, mp *roaring.Bitmap) *roaring.Bitmap {
	mx, my := roaring.NewBitmap(), roaring.NewBitmap()
	for i := 63; i >= 0; i-- {
		bitx := (x >> uint(i)) & 1
		bity := (y >> uint(i)) & 1
		if bitx == 1 {
			mp = mp.Difference(mp.Difference(r.subMap(uint64(bsiOffsetBit + i))).Difference(mx))
		} else {
			if i > 0 {
				mx = mx.Union(mp.Intersect(r.subMap(uint64(bsiOffsetBit + i))))
			}
		}
		if bity == 0 {
			mp = mp.Difference(r.subMap(uint64(bsiOffsetBit + i)).Difference(my))
		} else {
			if i > 0 {
				my = my.Union(mp.Difference(r.subMap(uint64(bsiOffsetBit + i))))
			}
		}
	}
	return mp
}

func (r *Ranging) minUnsigned(filter *roaring.Bitmap) (int64, uint64) {
	var min int64
	var count uint64

	for i := 63; i >= 0; i-- {
		mp := filter.Difference(r.subMap(uint64(bsiOffsetBit + i)))
		count = mp.Count()
		if count > 0 {
			filter = mp
		} else {
			min += (1 << uint(i))
			if i == 0 {
				count = filter.Count()
			}
		}
	}
	return min, count
}

func (r *Ranging) maxUnsigned(filter *roaring.Bitmap) (int64, uint64) {
	var max int64
	var count uint64

	for i := 63; i >= 0; i-- {
		mp := r.subMap(uint64(bsiOffsetBit + i)).Intersect(filter)
		count = mp.Count()
		if count > 0 {
			max += (1 << uint(i))
			filter = mp
		} else if i == 0 {
			count = filter.Count()
		}
	}
	return max, count
}

func (r *Ranging) subMap(v uint64) *roaring.Bitmap {
	return r.mp.OffsetRange(0, v*ShardWidth, (v+1)*ShardWidth)
}

func (r *Ranging) value(k uint64) (int64, bool) {
	var v int64

	if ok := r.bit(bsiExistsBit, k); !ok {
		return 0, false
	}
	for i := uint(0); i < 64; i++ {
		if ok := r.bit(uint64(bsiOffsetBit+i), k); ok {
			v |= (1 << i)
		}
	}
	if ok := r.bit(bsiSignBit, k); ok {
		v = -v
	}
	return v, true
}

func (r *Ranging) bit(x, y uint64) bool {
	return r.mp.Contains(pos(x, y))
}

func (r *Ranging) setBit(x, y uint64) error {
	if _, err := r.mp.Add(pos(x, y)); err != nil {
		return err
	}
	return nil
}

func (r *Ranging) clearBit(x, y uint64) error {
	if _, err := r.mp.Remove(pos(x, y)); err != nil {
		return err
	}
	return nil
}

func pos(x, y uint64) uint64 {
	if y >= ShardWidth {
		panic(errors.New("x"))
	}
	return (x * ShardWidth) + (y % ShardWidth)
}
