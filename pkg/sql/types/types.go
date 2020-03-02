package types

import (
	"fmt"
)

const (
	T_null = iota
	T_any
	T_int
	T_bool
	T_time
	T_float
	T_tuple
	T_array
	T_table
	T_string
)

type T struct {
	// Oid returns the type's Object ID
	Oid int32
	// Width is the size or scale of the type, such as number of characters
	Width         int32
	TupleContents []T
	TupleLabels   []string
}

var (
	Any = &T{Oid: T_any}

	Null = &T{Oid: T_null}

	Bool = &T{Oid: T_bool}

	Array = &T{Oid: T_array}

	Table = &T{Oid: T_table}

	Tuple = &T{Oid: T_tuple}

	String = &T{Oid: T_string}

	Int = &T{Oid: T_int, Width: 64}

	Time = &T{Oid: T_time, Width: 64}

	Float = &T{Oid: T_float, Width: 64}

	Scalar = []*T{Bool, String, Int, Time, Float}
)

func MakeScalar(oid, width int32) *T {
	return &T{
		Oid:   oid,
		Width: width,
	}
}

func MakeAny() *T {
	return &T{
		Oid: T_any,
	}
}

func MakeInt() *T {
	return &T{
		Width: 64,
		Oid:   T_int,
	}
}

func MakeNull() *T {
	return &T{
		Oid: T_null,
	}
}

func MakeBool() *T {
	return &T{
		Oid: T_bool,
	}
}

func MakeTime() *T {
	return &T{
		Width: 64,
		Oid:   T_time,
	}
}

func MakeFloat() *T {
	return &T{
		Width: 64,
		Oid:   T_float,
	}
}

func MakeArray() *T {
	return &T{
		Oid: T_array,
	}
}

func MakeTable() *T {
	return &T{
		Oid: T_table,
	}
}

// MakeTuple constructs a new instance of a TupleFamily type with the given
// field types (some/all of which may be other TupleFamily types).
//
// Warning: the contents slice is used directly; the caller should not modify it
// after calling this function.
func MakeTuple(contents []T) *T {
	return &T{
		Oid:           T_tuple,
		TupleContents: contents,
	}
}

// MakeString constructs a new instance of the STRING type (oid = T_string) having
// the given max # characters (0 = unspecified number).
func MakeString(width int32) *T {
	if width == 0 {
		return String
	}
	if width < 0 {
		panic(fmt.Errorf("width %d cannot be negative", width))
	}
	return &T{
		Width: width,
		Oid:   T_string,
	}
}

// Equivalent types are compatible with one another: they can be compared,
// assigned, and unioned. Equivalent types must always have the same type family
// for the root type and any descendant types (i.e. in case of array or tuple
// types). Types in the CollatedStringFamily must have the same locale. But
// other attributes of equivalent types, such as width, precision, and oid, can
// be different.
//
// Wildcard types (e.g. Any, AnyArray, AnyTuple, etc) have special equivalence
// behavior. AnyFamily types match any other type, including other AnyFamily
// types. And a wildcard collation (empty string) matches any other collation.
func (t *T) Equivalent(other *T) bool {
	if t.Oid != other.Oid {
		return false
	}
	return true
}

// Identical returns true if every field in this ColumnType is exactly the same
// as every corresponding field in the given ColumnType. Identical performs a
// deep comparison, traversing any Tuple or Array contents.
//
// NOTE: Consider whether the desired semantics really require identical types,
// or if Equivalent is the right method to call instead.
func (t *T) Identical(other *T) bool {
	if t.Width != other.Width {
		return false
	}
	return t.Oid == other.Oid
}

func (t *T) String() string { return t.SQLString() }

func (t *T) SQLString() string {
	switch t.Oid {
	case T_any:
		return "ANY"
	case T_int:
		return "INT"
	case T_null:
		return "NULL"
	case T_bool:
		return "BOOL"
	case T_time:
		return "TIME"
	case T_float:
		return "FLOAT"
	case T_tuple:
		return "TUPLE"
	case T_table:
		return "TABLE"
	case T_array:
		return "ARRAY"
	case T_string:
		return "STRING"
	}
	panic(fmt.Errorf("unexpected oid: %v", t.Oid))
}
