package tree

import (
	"fmt"
)

type Select struct {
	Limit    *Limit
	OrderBy  OrderBy
	Relation RelationStatement
}

func (n *Select) String() string {
	var s string

	s += n.Relation.String()
	if len(n.OrderBy) > 0 {
		s += " " + n.OrderBy.String()
	}
	if n.Limit != nil {
		s += " " + n.Limit.String()
	}
	return s
}

type Limit struct {
	Offset, Count ExprStatement
}

func (n *Limit) String() string {
	var s string

	if n.Count != nil {
		s += "LIMIT " + n.Count.String()
	}
	if n.Offset != nil {
		s += "OFFSET " + n.Offset.String()
	}
	return s
}

type OrderBy []*Order

type Order struct {
	Type Direction
	E    ExprStatement
}

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}

func (n *Order) String() string {
	var s string

	s += n.E.String()
	if n.Type != DefaultDirection {
		s = " " + n.Type.String()
	}
	return s
}

func (n OrderBy) String() string {
	var s string

	s += "ORDER BY "
	for i := range n {
		if i > 0 {
			s += ", "
		}
		s += n[i].String()
	}
	return s
}

type SelectClause struct {
	Distinct bool
	From     *From
	Where    *Where
	Having   *Where
	GroupBy  *GroupBy
	Sel      SelectExprs
}

func (n *SelectClause) String() string {
	var s string

	s += "SELECT "
	if n.Distinct {
		s += "DISTINCT "
	}
	if len(n.Sel) > 0 {
		s += n.Sel.String()
	} else {
		s += "*"
	}
	if n.From != nil {
		s += " " + n.From.String()
	}
	if n.Where != nil {
		s += " " + n.Where.String()
	}
	if n.GroupBy != nil {
		s += " " + n.GroupBy.String()
	}
	if n.Having != nil {
		s += " " + n.Having.String()
	}
	return s
}

type From struct {
	Tables TableStatements
}

type TableStatements []TableStatement

func (n TableStatements) String() string {
	var s string

	for i := range n {
		if i > 0 {
			s += ", "
		}
		s += n[i].String()
	}
	return s
}

func (n *From) String() string {
	return "FROM " + n.Tables.String()
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	E    ExprStatement
}

// Where.Type
const (
	AstWhere  = "WHERE"
	AstHaving = "HAVING"
)

func (n *Where) String() string {
	return n.Type + " " + n.E.String()
}

type GroupBy struct {
	Es ExprStatements
}

func (n *GroupBy) String() string {
	var s string

	s += "GROUP BY "
	for i := range n.Es {
		if i > 0 {
			s += ", "
		}
		s += n.Es[i].String()
	}
	return s
}

type SelectExprs []*SelectExpr

func (n SelectExprs) String() string {
	var s string

	for i := range n {
		if i > 0 {
			s += ", "
		}
		s += n[i].String()
	}
	return s
}

type SelectExpr struct {
	As Name
	E  ExprStatement
}

func (n *SelectExpr) String() string {
	var s string

	s += n.E.String()
	if len(n.As) > 0 {
		s += " As " + n.As.String()
	}
	return s
}
