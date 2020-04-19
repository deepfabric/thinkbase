package build

import (
	"fmt"
	"strings"

	"github.com/deepfabric/thinkbase/pkg/vm/util"
)

func (b *build) pruneColumnName(name string, typ int) (string, error) {
	var ts []*table

	if typ == On {
		return b.pruneColumnNameWithJoin(name)
	}
	names := strings.Split(name, ".")
	attr := names[len(names)-1]
	for _, t := range b.ts[0].ts {
		switch {
		case len(names) == 1 && t.existColunm(attr):
			ts = append(ts, t)
		case t.o != nil && t.existColunm(name):
			ts = append(ts, t)
		case t.o == nil && len(names) > 1 && t.name == names[len(names)-2] && t.existColunm(attr):
			ts = append(ts, t)
		}
	}
	switch {
	case len(ts) == 0:
		return "", fmt.Errorf("no such column '%s'", name)
	case len(ts) > 1:
		return "", fmt.Errorf("column '%s' is ambiguous", name)
	}
	if len(names) == 1 {
		return name, nil
	}
	if len(b.ts) == 1 && ts[0].o == nil {
		return attr, nil
	}
	return names[len(names)-2] + "." + attr, nil
}

func (b *build) pruneColumnNameWithJoin(name string) (string, error) {
	var cnt int

	for _, t := range b.ts[0].ts {
		if t.existColunm(name) {
			cnt++
		}
	}
	switch {
	case cnt == 0:
		return "", fmt.Errorf("no such column '%s'", name)
	case cnt > 1:
		return "", fmt.Errorf("column '%s' is ambiguous", name)
	}
	return name, nil
}

func tableName(names []string) string {
	var name string

	names = names[:len(names)-1]
	for i := range names {
		if i > 0 {
			name += "."
		}
		name += names[i]
	}
	return name
}

func (t *table) tableName() string {
	return strings.Split(t.name, ".")[1]
}

func (t *table) databaseName() string {
	return strings.Split(t.name, ".")[0]
}

func (t *table) existColunm(attr string) bool {
	return util.Contain([]string{attr}, t.attrs) == nil
}
