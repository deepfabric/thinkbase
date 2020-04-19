package build

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
)

func (b *build) buildGroup(n *tree.GroupBy) ([]string, error) {
	var attrs []string

	if n == nil {
		return nil, nil
	}
	for i := range n.Es {
		if e, ok := n.Es[i].(tree.ColunmNameList); ok {
			attr, err := b.buildExprColumn(e)
			if err != nil {
				return nil, err
			}
			attrs = append(attrs, attr)
		} else {
			return nil, fmt.Errorf("wrong 'GROUP BY %s' statement", n.Es[i])
		}
	}
	return util.MergeAttributes(attrs, []string{}), nil
}
