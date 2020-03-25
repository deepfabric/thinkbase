package mem

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type mem struct {
	start int
	name  string
	attrs []string
	ts    []value.Array
	mp    map[string]int
}

func (r *mem) String() string {
	s := r.name + "\n"
	for i, as := range r.attrs {
		if i > 0 {
			s += "\t"
		}
		s += as
	}
	s += "\n"
	for _, t := range r.ts {
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
