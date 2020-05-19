package textranging

import (
	"bytes"
	"errors"

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
	return r.subMap(64)
}

func (r *Ranging) Fold(dict mdictionary.Mdictionary) error {
	var err error

	mp := r.subMap(64)
	fp := make(map[string]struct{})
	mp.ForEach(func(i uint64) {
		v, ok := r.value(i)
		if ok {
			if _, ok := fp[v]; !ok {
				fp[v] = struct{}{}
				mq, privErr := r.Eq(v)
				if privErr != nil {
					err = privErr
				}
				k, privErr := encoding.EncodeValue(value.NewString(v))
				if privErr != nil {
					err = privErr
				}
				dict.Set(string(k), mq)
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

func (r *Ranging) Set(k uint64, s string) error {
	v := StringToInt(s)
	for i := uint(0); i < 64; i++ {
		if v&(1<<i) != 0 {
			if err := r.setBit(uint64(i), k); err != nil {
				return err
			}
		} else {
			if err := r.clearBit(uint64(i), k); err != nil {
				return err
			}
		}
	}
	if err := r.setBit(uint64(64), k); err != nil {
		return err
	}
	return nil
}

func (r *Ranging) Del(k uint64, s string) error {
	v := StringToInt(s)
	for i := uint(0); i < 64; i++ {
		if v&(1<<i) != 0 {
			if err := r.setBit(uint64(i), k); err != nil {
				return err
			}
		} else {
			if err := r.clearBit(uint64(i), k); err != nil {
				return err
			}
		}
	}
	if err := r.clearBit(uint64(64), k); err != nil {
		return err
	}
	return nil
}

func (r *Ranging) Count(filter *roaring.Bitmap) uint64 {
	mp := r.subMap(64)
	if filter != nil {
		mp = mp.Intersect(filter)
	}
	return mp.Count()
}

func (r *Ranging) Min(filter *roaring.Bitmap) (string, uint64, error) {
	mp := r.subMap(64)
	if filter != nil {
		mp = mp.Intersect(filter)
	}
	if mp.Count() == 0 {
		return "", 0, nil
	}
	min, count := r.minUnsigned(mp)
	return IntToString(min), count, nil
}

func (r *Ranging) Max(filter *roaring.Bitmap) (string, uint64, error) {
	mp := r.subMap(64)
	if filter != nil {
		mp = mp.Intersect(filter)
	}
	if mp.Count() == 0 {
		return "", 0, nil
	}
	max, count := r.maxUnsigned(mp)
	return IntToString(max), count, nil
}

func (r *Ranging) Eq(s string) (*roaring.Bitmap, error) {
	v := StringToInt(s)
	mp := r.subMap(64)
	for i := 63; i >= 0; i-- {
		if (v>>uint(i))&1 == 1 {
			mp = mp.Intersect(r.subMap(uint64(i)))
		} else {
			mp = mp.Difference(r.subMap(uint64(i)))
		}
	}
	return mp, nil
}

func (r *Ranging) Ne(s string) (*roaring.Bitmap, error) {
	v := StringToInt(s)
	mp := r.subMap(64)
	for i := 63; i >= 0; i-- {
		if (v>>uint(i))&1 == 1 {
			mp = mp.Intersect(r.subMap(uint64(i)))
		} else {
			mp = mp.Difference(r.subMap(uint64(i)))
		}
	}
	return r.subMap(64).Difference(mp), nil
}

func (r *Ranging) Lt(s string) (*roaring.Bitmap, error) {
	v := StringToInt(s)
	return r.lt(v, r.subMap(64), false), nil
}

func (r *Ranging) Le(s string) (*roaring.Bitmap, error) {
	v := StringToInt(s)
	return r.lt(v, r.subMap(64), true), nil
}

func (r *Ranging) Gt(s string) (*roaring.Bitmap, error) {
	v := StringToInt(s)
	return r.gt(v, r.subMap(64), false), nil
}

func (r *Ranging) Ge(s string) (*roaring.Bitmap, error) {
	v := StringToInt(s)
	return r.gt(v, r.subMap(64), true), nil
}

func (r *Ranging) Between(s, t string) (*roaring.Bitmap, error) {
	x := StringToInt(s)
	y := StringToInt(t)
	return r.between(x, y, r.subMap(64)), nil
}

func (r *Ranging) lt(v uint64, mp *roaring.Bitmap, eq bool) *roaring.Bitmap {
	zflg := true // leading zero flag
	mq := roaring.NewBitmap()
	for i := 63; i >= 0; i-- {
		bit := (v >> uint(i)) & 1
		if zflg {
			if bit == 0 {
				mp = mp.Difference(r.subMap(uint64(i)))
				continue
			} else {
				zflg = false
			}
		}
		if i == 0 && !eq {
			if bit == 0 {
				return mq
			}
			return mp.Difference(r.subMap(uint64(i)).Difference(mq))
		}
		if bit == 0 {
			mp = mp.Difference(r.subMap(uint64(i)).Difference(mq))
			continue
		}
		if i > 0 {
			mq = mq.Union(mp.Difference(r.subMap(uint64(i))))
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
			return mp.Difference(mp.Difference(r.subMap(uint64(i))).Difference(mq))
		}
		if bit == 1 {
			mp = mp.Difference(mp.Difference(r.subMap(uint64(i))).Difference(mq))
			continue
		}
		if i > 0 {
			mq = mq.Union(mp.Intersect(r.subMap(uint64(i))))
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
			mp = mp.Difference(mp.Difference(r.subMap(uint64(i))).Difference(mx))
		} else {
			if i > 0 {
				mx = mx.Union(mp.Intersect(r.subMap(uint64(i))))
			}
		}
		if bity == 0 {
			mp = mp.Difference(r.subMap(uint64(i)).Difference(my))
		} else {
			if i > 0 {
				my = my.Union(mp.Difference(r.subMap(uint64(i))))
			}
		}
	}
	return mp
}

func (r *Ranging) minUnsigned(filter *roaring.Bitmap) (uint64, uint64) {
	var min, count uint64

	for i := 63; i >= 0; i-- {
		mp := filter.Difference(r.subMap(uint64(i)))
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

func (r *Ranging) maxUnsigned(filter *roaring.Bitmap) (uint64, uint64) {
	var max, count uint64

	for i := 63; i >= 0; i-- {
		mp := r.subMap(uint64(i)).Intersect(filter)
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

func (r *Ranging) value(k uint64) (string, bool) {
	var v uint64

	if ok := r.bit(64, k); !ok {
		return "", false
	}
	for i := uint(0); i < 64; i++ {
		if ok := r.bit(uint64(i), k); ok {
			v |= (1 << i)
		}
	}
	return IntToString(v), true
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

func IntToString(v uint64) string {
	return string(encoding.EncodeUint64(v))
}

func StringToInt(s string) uint64 {
	v := []byte(s)
	if n := len(v); n < 8 {
		v = append(v, zeroByteSlice()[:8-n]...)
	}
	return encoding.DecodeUint64(v)
}

func zeroByteSlice() []byte {
	return []byte{
		0, 0, 0, 0,
		0, 0, 0, 0,
	}
}
