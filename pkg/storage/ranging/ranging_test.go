package ranging

import (
	"encoding/binary"
	"fmt"
	"log"
	"testing"
)

func ZeroByteSlice() []byte {
	return []byte{
		0, 0, 0, 0,
		0, 0, 0, 0,
	}
}

func DecodeUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func TestRanging(t *testing.T) {
	mp := New()
	{
		v := []byte("shanghai")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		{
			fmt.Printf("shanghai: %v\n", iv)
		}
		if err := mp.Set(0, iv); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := []byte("shuzhou")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		{
			fmt.Printf("shuzhou: %v\n", iv)
		}
		if err := mp.Set(3, iv); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := []byte("chendu")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		{
			fmt.Printf("chendu: %v\n", iv)
		}
		if err := mp.Set(8, iv); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := []byte("beijing")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		{
			fmt.Printf("beijing: %v\n", iv)
		}
		if err := mp.Set(12, iv); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := []byte{127, 0, 1}
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		{
			fmt.Printf("?: %v\n", iv)
		}
		if err := mp.Set(14, iv); err != nil {
			log.Fatal(err)
		}
	}

	{
		v := []byte("shuzhou")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		mq, err := mp.Eq(iv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("= shuzhou: %v\n", mq.Slice())
	}
	{
		v := []byte("shuzhou")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		mq, err := mp.Ne(iv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("<> shuzhou: %v\n", mq.Slice())
	}
	{
		v := []byte("shuzhou")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		mq, err := mp.Lt(iv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("< shuzhou: %v\n", mq.Slice())
	}
	{
		v := []byte("shuzhou")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		mq, err := mp.Le(iv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("<= shuzhou: %v\n", mq.Slice())
	}
	{
		v := []byte("shuzhou")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		mq, err := mp.Gt(iv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("> shuzhou: %v\n", mq.Slice())
	}
	{
		v := []byte("shuzhou")
		if n := len(v); n < 8 {
			v = append(v, ZeroByteSlice()[:8-n]...)
		}
		iv := int64(DecodeUint64(v))
		mq, err := mp.Ge(iv)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(">= shuzhou: %v\n", mq.Slice())
		fmt.Printf(">= shuzhou: %v\n", mq.Intersect(nil).Slice())
	}
}
