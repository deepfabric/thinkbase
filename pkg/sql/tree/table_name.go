package tree

type TableName struct {
	NumParts int
	Parts    [2]Name
}

func (n *TableName) String() string {
	var s string

	for i := 0; i < n.NumParts; i++ {
		if i > 0 {
			s += "."
		}
		s += n.Parts[i].String()
	}
	return s
}
