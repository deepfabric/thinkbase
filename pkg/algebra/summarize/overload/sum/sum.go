package sum

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/types"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/util/arith"
)

func New() *sum {
	return &sum{}
}

func (s *sum) Reset() {
	s.iv, s.fv, s.isFloat = 0, 0, false
}

func (s *sum) Fill(a value.Attribute) error {
	if len(a) == 0 {
		return nil
	}
	for _, v := range a {
		switch v.ResolvedType().Oid {
		case types.T_int:
			if s.isFloat {
				s.fv += float64(value.MustBeInt(v))
			} else {
				r, ok := arith.AddWithOverflow(s.iv, value.MustBeInt(v))
				if !ok {
					return errors.New("integer out of range")
				}
				s.iv = r
			}
		case types.T_float:
			if !s.isFloat {
				s.isFloat = true
				s.fv = float64(s.iv)
			}
			s.fv += value.MustBeFloat(v)
		default: // skip
		}
	}
	return nil
}

func (s *sum) Eval() (value.Value, error) {
	if s.isFloat {
		return value.NewFloat(s.fv), nil
	}
	return value.NewInt(s.iv), nil
}
