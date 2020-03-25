package encoding

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"math"
	"time"

	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

func EncodeValue(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	switch t := v.(type) {
	case *value.Int:
		buf.Write([]byte{byte(types.T_int & 0xFF)})
		buf.Write(EncodeUint64(uint64(value.MustBeInt(t))))
		return buf.Bytes(), nil
	case value.Null:
		buf.Write([]byte{byte(types.T_null & 0xFF)})
		return buf.Bytes(), nil
	case *value.Bool:
		buf.Write([]byte{byte(types.T_bool & 0xFF)})
		if value.MustBeBool(t) {
			buf.Write([]byte{1})
		} else {
			buf.Write([]byte{0})
		}
		return buf.Bytes(), nil
	case *value.Time:
		buf.Write([]byte{byte(types.T_time & 0xFF)})
		buf.Write(EncodeUint64(uint64(value.MustBeTime(t).Unix())))
		return buf.Bytes(), nil
	case *value.Float:
		buf.Write([]byte{byte(types.T_float & 0xFF)})
		buf.Write(EncodeUint64(math.Float64bits(value.MustBeFloat(t))))
		return buf.Bytes(), nil
	case *value.Table:
		s := value.MustBeTable(t)
		buf.Write([]byte{byte(types.T_table & 0xFF)})
		buf.Write(EncodeUint32(uint32(len(s))))
		buf.Write([]byte(s))
		return buf.Bytes(), nil
	case *value.String:
		s := value.MustBeString(t)
		buf.Write([]byte{byte(types.T_string & 0xFF)})
		buf.Write(EncodeUint32(uint32(len(s))))
		buf.Write([]byte(s))
		return buf.Bytes(), nil
	case value.Array:
		buf.Write([]byte{byte(types.T_array & 0xFF)})
		buf.Write(EncodeUint32(uint32(len(t))))
		for _, e := range t {
			data, err := EncodeValue(e)
			if err != nil {
				return nil, err
			}
			buf.Write(data)
		}
		return buf.Bytes(), nil
	default:
		return nil, errors.New("unsupport type")
	}
}

func DecodeValue(data []byte) (interface{}, []byte, error) {
	switch data[0] {
	case types.T_int:
		return value.NewInt(int64(DecodeUint64(data[1:]))), data[9:], nil
	case types.T_null:
		return value.ConstNull, data[1:], nil
	case types.T_bool:
		if data[1] == 1 {
			return value.NewBool(true), data[2:], nil
		}
		return value.NewBool(false), data[2:], nil
	case types.T_time:
		return value.NewTime(time.Unix(int64(DecodeUint64(data[1:])), 0)), data[9:], nil
	case types.T_float:
		return value.NewFloat(math.Float64frombits(DecodeUint64(data[1:]))), data[9:], nil
	case types.T_table:
		n := DecodeUint32(data[1:])
		return value.NewTable(string(data[5 : 5+n])), data[5+n:], nil
	case types.T_string:
		n := DecodeUint32(data[1:])
		return value.NewString(string(data[5 : 5+n])), data[5+n:], nil
	case types.T_array:
		var err error
		var xs value.Array
		var x interface{}
		cnt := int(DecodeUint32(data[1:]))
		data = data[5:]
		for i := 0; i < cnt; i++ {
			x, data, err = DecodeValue(data)
			if err != nil {
				return nil, nil, err
			}
			if v, ok := x.(value.Value); ok {
				xs = append(xs, v)
			} else {
				return nil, nil, errors.New("unsupport type")
			}
		}
		return xs, data, nil
	default:
		return nil, nil, errors.New("unsupport type")
	}
}

func EncodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

func DecodeUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func EncodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func DecodeUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
