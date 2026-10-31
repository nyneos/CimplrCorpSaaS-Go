package fxrates

import "CimplrCorpSaas/api/dash/ticker"

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

func INRPerUnit() map[string]float64 {
	return ticker.InrEquivalentRates()
}
