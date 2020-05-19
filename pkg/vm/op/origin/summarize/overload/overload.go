package overload

func Convert(op int) int {
	switch op {
	case AvgI, AvgIt:
		return Avg
	case MaxI, MaxIt:
		return Max
	case MinI, MinIt:
		return Min
	case SumI, SumIt:
		return Sum
	case CountI, CountIt:
		return Count
	}
	return op
}

func IsMax(op int) bool {
	switch op {
	case Max:
		return true
	case MaxI:
		return true
	case MaxIt:
		return true
	}
	return false
}

func IsIndexAggFunc(op int) bool {
	switch op {
	case AvgI:
		return true
	case MaxI:
		return true
	case MinI:
		return true
	case SumI:
		return true
	case CountI:
		return true
	}
	return false
}

func IsIndexTryAggFunc(op int) bool {
	switch op {
	case AvgIt:
		return true
	case MaxIt:
		return true
	case MinIt:
		return true
	case SumIt:
		return true
	case CountIt:
		return true
	}
	return false
}
