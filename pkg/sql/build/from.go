package build

import (
	"fmt"
	"strings"

	"github.com/deepfabric/thinkbase/pkg/sql/tree"
)

func (b *build) buildFrom(n *tree.From) error {
	for i := range n.Tables {
		switch t := n.Tables[i].(type) {
		case *tree.AliasedTable:
			if err := b.buildAliasedTable(t); err != nil {
				return err
			}
		default:
			return fmt.Errorf("illegal table '%s'", n.Tables[i])
		}
	}
	return nil
}

func (b *build) buildAliasedTable(n *tree.AliasedTable) error {
	switch t := n.Tbl.(type) {
	case *tree.Subquery:
		o, err := b.buildStatement(t.Select)
		if err != nil {
			return err
		}
		attrs, err := o.AttributeList()
		if err != nil {
			return err
		}
		if n.As == nil {
			b.ts[0].ts = append(b.ts[0].ts, &table{isAlias: false, o: o, name: t.String(), attrs: attrs})
		} else {
			alias := string(n.As.Alias)
			b.ts[0].ts = append(b.ts[0].ts, &table{isAlias: true, o: o, name: alias, attrs: attrs})
		}
		return nil
	case *tree.TableName:
		tbl, err := b.buildTableName(t)
		if err != nil {
			return err
		}
		if n.As != nil {
			tbl.isAlias = true
			tbl.name = string(n.As.Alias)
		}
		if _, ok := b.mp[tbl.name]; ok {
			return fmt.Errorf("table '%s' is ambiguous", tbl.name)
		} else {
			b.mp[tbl.name] = struct{}{}
		}
		b.ts[0].ts = append(b.ts[0].ts, tbl)
		return nil
	default:
		return fmt.Errorf("illegal aliased table '%s'", n)
	}
}

func (b *build) buildTableName(n *tree.TableName) (*table, error) {
	name, err := b.buildExprColumn(n.N)
	if err != nil {
		return nil, err
	}
	names := strings.Split(name, ".")
	if len(names) == 1 {
		name = b.c.Database() + "." + name
	}
	r, err := b.c.Relation(name)
	if err != nil {
		return nil, err
	}
	attrs, err := r.AttributeList()
	if err != nil {
		return nil, err
	}
	return &table{false, nil, names[len(names)-1], attrs, r}, nil
}
