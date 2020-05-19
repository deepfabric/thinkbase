package textranging

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
)

func TestRanging(t *testing.T) {
	mp := New()
	{
		v := "shanghai"
		{
			fmt.Printf("0: shanghai\n")
		}
		if err := mp.Set(0, v); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := ""
		{
			fmt.Printf("2: \n")
		}
		if err := mp.Set(2, v); err != nil {
			log.Fatal(err)
		}
	}

	{
		v := "shuzhou"
		{
			fmt.Printf("3: shuzhou\n")
		}
		if err := mp.Set(3, v); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := "chendu"
		{
			fmt.Printf("8: chendu\n")
		}
		if err := mp.Set(8, v); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := "beijing"
		{
			fmt.Printf("12: beijing\n")
		}
		if err := mp.Set(12, v); err != nil {
			log.Fatal(err)
		}
	}
	{
		v := []byte{245, 0, 1}
		{
			fmt.Printf("14: %v\n", v)
		}
		if err := mp.Set(14, string(v)); err != nil {
			log.Fatal(err)
		}
	}

	{
		mq, err := mp.Eq("")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("= empty: %v\n", mq.Slice())
	}

	{
		mq, err := mp.Eq("shuzhou")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("= shuzhou: %v\n", mq.Slice())
	}
	{
		mq, err := mp.Ne("shuzhou")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("<> shuzhou: %v\n", mq.Slice())
	}
	{
		mq, err := mp.Lt("shuzhou")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("< shuzhou: %v\n", mq.Slice())
	}
	{
		mq, err := mp.Le("shuzhou")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("<= shuzhou: %v\n", mq.Slice())
	}
	{
		mq, err := mp.Gt("shuzhou")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("> shuzhou: %v\n", mq.Slice())
	}
	{
		mq, err := mp.Ge("shuzhou")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(">= shuzhou: %v\n", mq.Slice())
	}
	{
		dict := mem.New()
		if err := mp.Fold(dict); err != nil {
			log.Fatal(err)
		}
		dict.Range(func(k string, mp *roaring.Bitmap) {
			v, _, _ := encoding.DecodeValue([]byte(k))
			fmt.Printf("%v: %v\n", v, mp.Slice())
		})
	}
}
