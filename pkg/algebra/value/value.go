package value

import (
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/deepfabric/thinkbase/pkg/algebra/types"
)

func Compare(a, b Value) int {
	if ta, tb := a.ResolvedType().Oid, b.ResolvedType().Oid; (ta == types.T_int || ta == types.T_float) &&
		(tb == types.T_int || tb == types.T_float) {
		return a.Compare(b)
	}
	if r := int(a.ResolvedType().Oid - b.ResolvedType().Oid); r != 0 {
		return r
	}
	return a.Compare(b)
}

// makeParseError returns a parse error using the provided string and type. An
// optional error can be provided, which will be appended to the end of the
// error string.
func makeParseError(s string, typ *types.T, err error) error {
	if err != nil {
		return fmt.Errorf("could not parse %q as type %s: %v", s, typ, err)
	}
	return fmt.Errorf("could not parse %q as type %s", s, typ)
}

func makeUnsupportedComparisonMessage(d1, d2 Value) error {
	return fmt.Errorf("unsupported comparison: %s to %s", d1.ResolvedType(), d2.ResolvedType())
}

func isCaseInsensitivePrefix(prefix, s string) bool {
	if len(prefix) > len(s) {
		return false
	}
	return strings.EqualFold(prefix, s[:len(prefix)])
}

func NewBool(v bool) *Bool {
	if v {
		return &ConstTrue
	}
	return &ConstFalse
}

func AsBool(v interface{}) (Bool, bool) {
	switch t := v.(type) {
	case *Bool:
		return *t, true
	default:
		return false, false
	}
}

// MustBeBool attempts to retrieve a Bool from a value, panicking if the
// assertion fails.
func MustBeBool(v interface{}) bool {
	b, ok := AsBool(v)
	if !ok {
		panic(fmt.Errorf("expected *Bool, found %T", v))
	}
	return bool(b)
}

// GetBool get Bool or an error.
func GetBool(v Value) (Bool, error) {
	if b, ok := v.(*Bool); ok {
		return *b, nil
	}
	return false, fmt.Errorf("cannot convert %s to type %s", v.ResolvedType(), types.Bool)
}

func (a *Bool) String() string {
	return strconv.FormatBool(bool(*a))
}

func (_ *Bool) ResolvedType() *types.T {
	return types.Bool
}

func ParseBool(s string) (*Bool, error) {
	s = strings.TrimSpace(s)
	if len(s) >= 1 {
		switch s[0] {
		case 't', 'T':
			if isCaseInsensitivePrefix(s, "true") {
				return &ConstTrue, nil
			}
		case 'f', 'F':
			if isCaseInsensitivePrefix(s, "false") {
				return &ConstFalse, nil
			}
		}
	}
	return nil, makeParseError(s, types.Bool, errors.New("invalid bool value"))
}

func (a *Bool) Compare(v Value) int {
	if v == ConstNull {
		return 1 // NULL is less than any non-NULL value
	}
	b, ok := v.(*Bool)
	if !ok {
		panic(makeUnsupportedComparisonMessage(a, v))
	}
	return CompareBool(bool(*a), bool(*b))
}

// CompareBool compare the input bools according to the SQL comparison rules.
func CompareBool(d, v bool) int {
	if !d && v {
		return -1
	}
	if d && !v {
		return 1
	}
	return 0
}

func (_ *Bool) IsLogical() bool              { return true }
func (_ *Bool) Attributes() map[int][]string { return make(map[int][]string) }

func NewInt(v int64) *Int {
	r := Int(v)
	return &r
}

func AsInt(v interface{}) (Int, bool) {
	switch t := v.(type) {
	case *Int:
		return *t, true
	default:
		return 0, false
	}
}

// MustBeInt attempts to retrieve a Int from a value, panicking if the
// assertion fails.
func MustBeInt(v interface{}) int64 {
	i, ok := AsInt(v)
	if !ok {
		panic(fmt.Errorf("expected *Int, found %T", v))
	}
	return int64(i)
}

func GetInt(v Value) (Int, error) {
	if i, ok := v.(*Int); ok {
		return *i, nil
	}
	return 0, fmt.Errorf("cannot convert %s to type %s", v.ResolvedType(), types.Int)
}

func (a *Int) String() string {
	return strconv.FormatInt(int64(*a), 10)
}

func (_ *Int) ResolvedType() *types.T {
	return types.Int
}

// ParseInt parses and returns the *Int value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseInt(s string) (*Int, error) {
	i, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return nil, makeParseError(s, types.Int, err)
	}
	return NewInt(i), nil
}

func (a *Int) Compare(v Value) int {
	var x, y int64

	if v == ConstNull {
		return 1 // NULL is less than any non-NULL value
	}
	x = int64(*a)
	if b, ok := v.(*Int); !ok {
		if b, ok := v.(*Float); !ok {
			panic(makeUnsupportedComparisonMessage(a, v))
		} else {
			y = int64(*b)
		}
	} else {
		y = int64(*b)
	}
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func (_ *Int) IsLogical() bool              { return false }
func (_ *Int) Attributes() map[int][]string { return make(map[int][]string) }

func NewFloat(v float64) *Float {
	r := Float(v)
	return &r
}

func AsFloat(v interface{}) (Float, bool) {
	switch t := v.(type) {
	case *Float:
		return *t, true
	default:
		return 0.0, false
	}
}

func MustBeFloat(v interface{}) float64 {
	f, ok := AsFloat(v)
	if !ok {
		panic(fmt.Errorf("expected *Float, found %T", v))
	}
	return float64(f)
}

func GetFloat(v Value) (Float, error) {
	if f, ok := v.(*Float); ok {
		return *f, nil
	}
	return 0, fmt.Errorf("cannot convert %s to type %s", v.ResolvedType(), types.Float)
}

func (a *Float) String() string {
	f := float64(*a)
	if _, frac := math.Modf(f); frac == 0 && -1000000 < *a && *a < 1000000 {
		return fmt.Sprintf("%.1f", f)
	} else {
		return fmt.Sprintf("%g", f)
	}
}

func (_ *Float) ResolvedType() *types.T {
	return types.Float
}

// ParseFloat parses and returns the *Float value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseFloat(s string) (*Float, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, makeParseError(s, types.Float, err)
	}
	return NewFloat(f), nil
}

func (a *Float) Compare(v Value) int {
	var x, y float64

	if v == ConstNull {
		return 1 // NULL is less than any non-NULL value
	}
	x = float64(*a)
	if b, ok := v.(*Float); !ok {
		if b, ok := v.(*Int); !ok {
			panic(makeUnsupportedComparisonMessage(a, v))
		} else {
			y = float64(*b)
		}
	} else {
		y = float64(*b)
	}
	// NaN sorts before non-NaN (#10109).
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	case x == y:
		return 0
	}
	if math.IsNaN(x) {
		if math.IsNaN(y) {
			return 0
		}
		return -1
	}
	return 1
}

func (_ *Float) IsLogical() bool              { return false }
func (_ *Float) Attributes() map[int][]string { return make(map[int][]string) }

func NewString(v string) *String {
	r := String(v)
	return &r
}

func AsString(v interface{}) (String, bool) {
	switch t := v.(type) {
	case *String:
		return *t, true
	default:
		return "", false
	}
}

func MustBeString(v interface{}) string {
	s, ok := AsString(v)
	if !ok {
		panic(fmt.Errorf("expected *String, found %T", v))
	}
	return string(s)
}

func GetString(v Value) (String, error) {
	if s, ok := v.(*String); ok {
		return *s, nil
	}
	return "", fmt.Errorf("cannot convert %s to type %s", v.ResolvedType(), types.String)
}

func (a *String) String() string {
	return string(*a)
}

func (_ *String) ResolvedType() *types.T {
	return types.String
}

func (a *String) Compare(v Value) int {
	if v == ConstNull {
		return 1 // NULL is less than any non-NULL value
	}
	b, ok := v.(*String)
	if !ok {
		panic(makeUnsupportedComparisonMessage(a, v))
	}
	if *a < *b {
		return -1
	}
	if *a > *b {
		return 1
	}
	return 0
}

func (_ *String) IsLogical() bool              { return false }
func (_ *String) Attributes() map[int][]string { return make(map[int][]string) }

func NewTime(t time.Time) *Time {
	return &Time{Time: t.Round(time.Second)}
}

func AsTime(v interface{}) (Time, bool) {
	switch t := v.(type) {
	case *Time:
		return *t, true
	default:
		return Time{}, false
	}
}

func MustBeTime(v interface{}) time.Time {
	t, ok := AsTime(v)
	if !ok {
		panic(fmt.Errorf("expected *Time, found %T", v))
	}
	return t.Time
}

func GetTime(v Value) (Time, error) {
	if t, ok := v.(*Time); ok {
		return *t, nil
	}
	return Time{}, fmt.Errorf("cannot convert %s to type %s", v.ResolvedType(), types.Time)
}

func (a *Time) String() string {
	return a.UTC().Format(TimeOutputFormat)
}

func (_ *Time) ResolvedType() *types.T {
	return types.Time
}

// ParseTime parses and returns the *Time value represented by
// the provided string in UTC, or an error if parsing is unsuccessful.
func ParseTime(s string) (*Time, error) {
	t, err := time.Parse(TimeOutputFormat, s)
	if err != nil {
		return nil, err
	}
	return NewTime(t), nil
}

func (a *Time) Compare(v Value) int {
	if v == ConstNull {
		return 1 // NULL is less than any non-NULL value
	}
	return compareTime(a, v)
}

func compareTime(a, b Value) int {
	aTime, aErr := GetTime(a)
	bTime, bErr := GetTime(b)
	if aErr != nil || bErr != nil {
		panic(makeUnsupportedComparisonMessage(a, b))
	}
	if aTime.Before(bTime.Time) {
		return -1
	}
	if bTime.Before(aTime.Time) {
		return 1
	}
	return 0
}

func (_ *Time) IsLogical() bool              { return false }
func (_ *Time) Attributes() map[int][]string { return make(map[int][]string) }

func NewTable(id string) *Table {
	return &Table{id}
}

func AsTable(v interface{}) (Table, bool) {
	switch t := v.(type) {
	case *Table:
		return *t, true
	default:
		return Table{}, false
	}
}

func MustBeTable(v interface{}) string {
	t, ok := AsTable(v)
	if !ok {
		panic(fmt.Errorf("expected *Table, found %T", v))
	}
	return t.Id
}

func GetTable(v Value) (Table, error) {
	if t, ok := v.(*Table); ok {
		return *t, nil
	}
	return Table{}, fmt.Errorf("cannot convert %s to type %s", v.ResolvedType(), types.Table)
}

func (a *Table) String() string {
	return "<" + a.Id + ">"
}

func (_ *Table) ResolvedType() *types.T {
	return types.Table
}

func (a *Table) Compare(v Value) int {
	if v == ConstNull {
		return 1 // NULL is less than any non-NULL value
	}
	return compareTable(a, v)
}

func compareTable(a, b Value) int {
	aTable, aErr := GetTable(a)
	bTable, bErr := GetTable(b)
	if aErr != nil || bErr != nil {
		panic(makeUnsupportedComparisonMessage(a, b))
	}
	return strings.Compare(aTable.Id, bTable.Id)
}

func (_ *Table) IsLogical() bool              { return false }
func (_ *Table) Attributes() map[int][]string { return make(map[int][]string) }

func (_ Null) String() string               { return "null" }
func (_ Null) ResolvedType() *types.T       { return types.Null }
func (_ Null) IsLogical() bool              { return false }
func (_ Null) Attributes() map[int][]string { return make(map[int][]string) }

func (a Null) Compare(v Value) int {
	if v == ConstNull {
		return 0
	}
	return 1 // NULL is less than any non-NULL value
}

func (_ Array) ResolvedType() *types.T       { return types.Array }
func (_ Array) IsLogical() bool              { return false }
func (_ Array) Attributes() map[int][]string { return make(map[int][]string) }

func (a Array) String() string {
	s := "["
	for i, v := range a {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s", v)
	}
	s += "]"
	return s
}

func (a Array) Compare(v Value) int {
	if v == ConstNull {
		return 1
	}
	b, ok := v.(Array)
	if !ok {
		panic(makeUnsupportedComparisonMessage(a, v))
	}
	if r := len(a) - len(b); r != 0 {
		if r < 0 {
			return -1
		}
		if r > 0 {
			return 1
		}

	}
	for i := range a {
		if r := int(a[i].ResolvedType().Oid - b[i].ResolvedType().Oid); r != 0 {
			return r
		}
		if r := a[i].Compare(b[i]); r != 0 {
			return r
		}
	}
	return 0
}

func (a *Int) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error)    { return a, nil }
func (a *Bool) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error)   { return a, nil }
func (a *Time) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error)   { return a, nil }
func (a *Float) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error)  { return a, nil }
func (a Null) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error)    { return a, nil }
func (a *Table) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error)  { return a, nil }
func (a Array) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error)   { return a, nil }
func (a *String) Eval(_ []Tuple, _ map[int]map[string]int) (Value, error) { return a, nil }

func (a Tuple) Len() int { return len(a) }
func (a Tuple) Compare(b Tuple) int {
	if r := len(a) - len(b); r != 0 {
		if r < 0 {
			return -1
		}
		if r > 0 {
			return 1
		}

	}
	for i := range a {
		if r := int(a[i].ResolvedType().Oid - b[i].ResolvedType().Oid); r != 0 {
			return r
		}
		if r := a[i].Compare(b[i]); r != 0 {
			return r
		}
	}
	return 0
}

func (a Attribute) Len() int { return len(a) }
func (a Attribute) Compare(b Attribute) int {
	if r := len(a) - len(b); r != 0 {
		if r < 0 {
			return -1
		}
		if r > 0 {
			return 1
		}

	}
	for i := range a {
		if r := int(a[i].ResolvedType().Oid - b[i].ResolvedType().Oid); r != 0 {
			return r
		}
		if r := a[i].Compare(b[i]); r != 0 {
			return r
		}
	}
	return 0

}

func (a Tuple) String() string {
	s := "("
	for i, v := range a {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s", v)
	}
	s += ")"
	return s
}

func (a Attribute) String() string {
	s := "("
	for i, v := range a {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s", v)
	}
	s += ")"
	return s
}

func init() {
	gob.Register(Null{})
	gob.Register(Time{})
	gob.Register(Array{})
	gob.Register(Tuple{})
	gob.Register(Table{})
	gob.Register(Attribute{})

	gob.Register(Int(0))
	gob.Register(Float(0))
	gob.Register(Bool(true))
	gob.Register(String(""))

	gob.Register(time.Time{})
	gob.Register([]interface{}{})
}
