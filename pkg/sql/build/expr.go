package build

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func (b *build) buildConditionWithoutSubquery(n tree.ExprStatement) (extend.Extend, error) {
	return nil, nil
}

func (b *build) buildExprColumn(n tree.ExprStatement) (string, error) {
	ns, ok := n.(tree.ColunmNameList)
	if !ok {
		return "", fmt.Errorf("'%s' is not colunm name", n)
	}
	var name string
	for i := range ns {
		if i > 0 {
			name += "."
		}
		name += string(ns[i].Path)
		if ns[i].Index != nil {
			if idx, err := b.buildExprIntConstant(ns[i].Index); err != nil {
				return "", err
			} else {
				name += fmt.Sprintf("._%v", idx)
			}
		}
	}
	return name, nil
}

func (b *build) buildExprIntConstant(n tree.ExprStatement) (int64, error) {
	switch e := n.(type) {
	case *tree.Value:
		if i, err := value.GetInt(e.E); err != nil {
			return 0, err
		} else {
			return int64(i), nil
		}
	case *tree.ModExpr:
		x, err := b.buildExprIntConstant(e.Left)
		if err != nil {
			return 0, err
		}
		y, err := b.buildExprIntConstant(e.Right)
		if err != nil {
			return 0, err
		}
		return x % y, nil
	case *tree.MultExpr:
		x, err := b.buildExprIntConstant(e.Left)
		if err != nil {
			return 0, err
		}
		y, err := b.buildExprIntConstant(e.Right)
		if err != nil {
			return 0, err
		}
		return x * y, nil
	case *tree.PlusExpr:
		x, err := b.buildExprIntConstant(e.Left)
		if err != nil {
			return 0, err
		}
		y, err := b.buildExprIntConstant(e.Right)
		if err != nil {
			return 0, err
		}
		return x + y, nil
	case *tree.MinusExpr:
		x, err := b.buildExprIntConstant(e.Left)
		if err != nil {
			return 0, err
		}
		y, err := b.buildExprIntConstant(e.Right)
		if err != nil {
			return 0, err
		}
		return x - y, nil
	case *tree.UnaryMinusExpr:
		x, err := b.buildExprIntConstant(e.E)
		if err != nil {
			return 0, err
		}
		return x * -1, nil
	default:
		return 0, fmt.Errorf("'%s' is not integer", n)
	}
}
