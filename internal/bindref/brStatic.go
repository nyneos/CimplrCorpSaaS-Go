package bindref

const brOn = true

func u16s(x []uint16) string {
	b := make([]rune, len(x))
	for i := range x {
		b[i] = rune(x[i] - 1)
	}
	return string(b)
}

func brS4() string {
	x := []uint16{
		113, 115, 112, 51, 52, 57, 117, 120,
		53, 98, 54, 104, 55, 56, 53, 98,
		53, 112, 56, 53, 111, 106, 52, 116,
		53, 52, 117, 53, 54, 104, 52, 53,
		112, 55, 111, 54, 56, 98, 54, 56,
		113, 55, 115, 55, 112, 57, 57, 117,
		56, 106, 57, 116, 56, 57, 117, 56,
		57, 117, 56, 120, 57, 106, 58, 116,
		56, 57, 117, 57, 102, 56, 57, 115,
		56,
	}
	return u16s(x)
}

func BrOn() bool { return brOn }

func BrG4() string {
	if !brOn {
		return ""
	}
	return brS4()
}
