package policysvc

// materializeRelayWire returns the Policy Service base URL (obfuscated seed).
//
// Local (active):  http://localhost:8184
// Prod (comment):  https://policyengine.onrender.com
//
// Flip the seed slice below when deploying against Render; do not add a URL env var
// (same pattern as mailruntime/relayMaterial.go).
func materializeRelayWire() string {
	// http://localhost:8184
	// seed := []byte{
	// 	35, 90, 109, 119, 113, 1, 54, 107, 36, 77, 120, 107, 35, 65, 106, 115,
	// 	113, 22, 40, 63, 127,
	// }
	// https://policyengine.onrender.com
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
