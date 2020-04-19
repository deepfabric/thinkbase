package testWorkspace

import (
	"fmt"
	"log"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New() *testWorkspace {
	mp := make(map[string]relation.Relation)
	mp["test.A"] = newRelation0()
	mp["test.A.c"] = newRelation1()
	mp["test.B"] = newRelation2()
	mp["test.C"] = newRelation3()
	mp["test.c"] = newRelation4()
	mp["test.A.a._0"] = newRelation5()
	mp["test.user"] = newRelation6()
	mp["test.A.a._1"] = newRelation7()
	return &testWorkspace{
		mp: mp,
		id: "tom",
		db: "test",
	}
}

func (w *testWorkspace) Id() string {
	return w.id
}

func (w *testWorkspace) Database() string {
	return w.db
}

func (w *testWorkspace) Relation(name string) (relation.Relation, error) {
	if r, ok := w.mp[name]; ok {
		return r, nil
	}
	return nil, fmt.Errorf("cannot find relation '%s'", name)
}

func newRelation0() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	attrs = append(attrs, "c")
	r := mem.New("A", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("x"))
		t = append(t, value.ConstNull)
		t = append(t, &value.ConstTrue)
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("a"))
		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("y"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("b"))
		t = append(t, value.ConstNull)
		t = append(t, value.NewString("m"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewTable("c"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array
		t = append(t, value.Array{value.NewTable("_0"), value.NewTable("_1")})
		t = append(t, value.NewString("hello world"))
		t = append(t, value.ConstNull)
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array
		t = append(t, value.Array{value.NewTable("_0")})
		t = append(t, value.ConstNull)
		t = append(t, value.ConstNull)
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation1() relation.Relation {
	var attrs []string

	attrs = append(attrs, "d")
	attrs = append(attrs, "f")
	r := mem.New("c", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("hello"))
		t = append(t, value.NewFloat(11.11))
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation2() relation.Relation {
	var attrs []string

	attrs = append(attrs, "b")
	attrs = append(attrs, "d")
	r := mem.New("B", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewFloat(3.0))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("xx"))
		tm, err := value.ParseTime("2020-04-18 12:35:43")
		if err != nil {
			log.Fatal(err)
		}
		t = append(t, tm)
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(2))
		t = append(t, value.NewString("m"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation3() relation.Relation {
	var attrs []string

	attrs = append(attrs, "d")
	attrs = append(attrs, "g")
	r := mem.New("C", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("0"))
		t = append(t, value.NewFloat(0.0))
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation4() relation.Relation {
	var attrs []string

	attrs = append(attrs, "d")
	attrs = append(attrs, "f")
	r := mem.New("c", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("world"))
		t = append(t, value.NewFloat(12.2))
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation5() relation.Relation {
	var attrs []string

	attrs = append(attrs, "_")
	attrs = append(attrs, "d")
	attrs = append(attrs, "f")
	r := mem.New("_0", attrs)
	{
		var t value.Array

		t = append(t, value.NewInt(3))
		t = append(t, value.ConstNull)
		t = append(t, value.ConstNull)
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.ConstNull)
		t = append(t, value.NewInt(13))
		tm, err := value.ParseTime("2020-04-18 12:35:40")
		if err != nil {
			log.Fatal(err)
		}
		t = append(t, tm)
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation6() relation.Relation {
	var attrs []string

	attrs = append(attrs, "id")
	attrs = append(attrs, "outlay")
	attrs = append(attrs, "income")
	attrs = append(attrs, "point")
	attrs = append(attrs, "level")
	r := mem.New("user", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("bob"))
		t = append(t, value.NewInt(13))
		t = append(t, value.NewInt(13))
		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("vip"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("tom"))
		t = append(t, value.NewInt(15))
		t = append(t, value.NewInt(3))
		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("vvip"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("robin"))
		t = append(t, value.NewInt(150))
		t = append(t, value.NewInt(40))
		t = append(t, value.NewInt(0))
		t = append(t, value.NewString("vvip"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("tom"))
		t = append(t, value.NewInt(130))
		t = append(t, value.NewInt(300))
		t = append(t, value.NewInt(-4))
		t = append(t, value.NewString("vvip"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("bob"))
		t = append(t, value.NewInt(345))
		t = append(t, value.NewInt(5435))
		t = append(t, value.NewInt(6))
		t = append(t, value.NewString("vip"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("tom"))
		t = append(t, value.NewInt(150))
		t = append(t, value.NewInt(300))
		t = append(t, value.NewInt(8))
		t = append(t, value.NewString("vvip"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("tom"))
		t = append(t, value.NewInt(77))
		t = append(t, value.NewInt(88))
		t = append(t, value.NewInt(9))
		t = append(t, value.NewString("vvip"))
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation7() relation.Relation {
	var attrs []string

	attrs = append(attrs, "_")
	r := mem.New("_1", attrs)
	{
		var t value.Array
		t = append(t, value.NewInt(2))
		r.AddTuples([]value.Array{t})
	}
	return r
}
