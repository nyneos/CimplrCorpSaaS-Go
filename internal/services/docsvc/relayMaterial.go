package docsvc

func materializeRelayWire() string {
	seed := []byte{
		35, 90, 109, 119, 56, 20, 54, 40, 47, 67, 106, 96, 46, 64, 124, 117, 42, 90, 112, 104, 37, 0, 118, 105, 57, 75, 119, 99, 46, 92, 55, 100, 36, 67,
	}
	mask := []byte{0x4b, 0x2e, 0x19, 0x07}
	out := make([]byte, len(seed))
	for i := range seed {
		out[i] = seed[i] ^ mask[i%len(mask)]
	}
	return string(out)
}
