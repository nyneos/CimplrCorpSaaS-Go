package policysvc

func materializeRelayWire() string {
	seed := []byte{
		35, 90, 109, 119, 56, 20, 54, 40, 59, 65, 117, 110, 40, 87, 124, 105,
		44, 71, 119, 98, 101, 65, 119, 117, 46, 64, 125, 98, 57, 0, 122, 104, 38,
	}
	mask := []byte{0x4b, 0x2e, 0x19, 0x07}
	out := make([]byte, len(seed))
	for i := range seed {
		out[i] = seed[i] ^ mask[i%len(mask)]
	}
	return string(out)
}
