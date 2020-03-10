package mem

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type relation struct {
	plh   int // placeholder
	name  string
	attrs []string
	tuple []value.Tuple
	mp    map[string]int
	ct    context.Context
}

func (r *relation) String() string {
	s := r.name + "\n"
	for i, as := range r.attrs {
		if i > 0 {
			s += "\t"
		}
		s += as
	}
	s += "\n"
	for _, t := range r.tuple {
		for i, v := range t {
			if i > 0 {
				s += "\t"
			}
			s += fmt.Sprintf("%s", v)
		}
		s += "\n"
	}
	return s
}
