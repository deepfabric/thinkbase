package restrict

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/bitmap/mem"
	rbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/rangebitmap/mem"
	rmem "github.com/deepfabric/thinkbase/pkg/storage/cache/relation/mem"
	srbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/srangebitmap/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
	"github.com/deepfabric/thinkkv/pkg/engine/pb"
)

func TestRestrict(t *testing.T) {
	var cs []*filter.Condition

	r := newRelation()
	cs = append(cs, &filter.Condition{
		IsOr: true,
		Name: "a",
		Op:   filter.GT,
		Val:  value.NewInt(1),
	})
	cs = append(cs, &filter.Condition{
		IsOr: false,
		Name: "a",
		Op:   filter.LT,
		Val:  value.NewInt(10),
	})
	c := context.New(context.NewConfig("tom"), nil, nil)
	n := New(r, filter.New(cs), c)
	{
		fmt.Printf("%s\n", n)
	}
	{
		attrs, err := n.AttributeList()
		fmt.Printf("%v, %v\n", attrs, err)
	}
	bs := c.BlockSize()
	for {
		mp, err := n.GetAttributes([]string{"a", "b"}, bs)
		if err != nil {
			log.Fatal(err)
		}
		if len(mp["a"]) == 0 {
			break
		}
		fmt.Printf("a = %v\n", mp["a"])
		fmt.Printf("b = %v\n", mp["b"])
	}
	os.RemoveAll("test.db")
}

func newRelation() relation.Relation {
	db := pb.New("test.db", nil, false, false)
	stg := storage.New(db, mem.New(), rmem.New(), rbmem.New(), srbmem.New())
	r, err := stg.Relation("test.A")
	if err != nil {
		log.Fatal(err)
	}
	var ts []map[string]interface{}
	{
		mp := make(map[string]interface{})
		mp["a"] = int64(1)
		mp["b"] = "x"
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = int64(3)
		mp["b"] = "y"
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = int64(2)
		mp["b"] = "m"
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = float64(3.1)
		mp["b"] = int64(3)
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = float64(3.1)
		mp["b"] = int64(3)
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = int64(1)
		mp["b"] = "x"
		ts = append(ts, mp)
	}
	if err := r.AddTuplesByJson(ts); err != nil {
		log.Fatal(err)
	}
	return r
}
