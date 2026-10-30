// Package fxrates exposes conversion tables derived from the loaded rate file
// (api/dash/rate.json via the ticker package). A currency that is not in the file
// is absent from the tables, so callers skip it instead of treating it as USD or INR.
package fxrates

import "CimplrCorpSaas/api/dash/ticker"

// USDPerUnit returns currency -> USD value of one unit. Empty when no rates are loaded.
func USDPerUnit() map[string]float64 {
	inr := ticker.InrEquivalentRates()
	usdInr, ok := inr["USD"]
	if !ok || usdInr == 0 {
		return map[string]float64{}
	}
	out := make(map[string]float64, len(inr))
	for code, r := range inr {
		out[code] = r / usdInr
	}
	return out
}

// INRPerUnit returns currency -> INR value of one unit. Empty when no rates are loaded.
func INRPerUnit() map[string]float64 {
	return ticker.InrEquivalentRates()
}
