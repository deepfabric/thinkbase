package types

import "fmt"

const (
	T_any = iota
	T_int
	T_null
	T_bool
	T_time
	T_float
	T_array
	T_table
	T_string
)

type T struct {
	Oid int32
}

var (
	Any = &T{T_any}

	Null = &T{T_null}

	Bool = &T{T_bool}

	Array = &T{T_array}

	Table = &T{T_table}

	String = &T{T_string}

	Int = &T{T_int}

	Time = &T{T_time}

	Float = &T{T_float}
)

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
	case T_table:
		return "TABLE"
	case T_array:
		return "ARRAY"
	case T_string:
		return "STRING"
	}
	panic(fmt.Errorf("unexpected oid: %v", t.Oid))
}
