package fxrates

import (
	"strings"

	"CimplrCorpSaas/api/dash/ticker"
	"CimplrCorpSaas/internal/logger"
)

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

func Rate(rates map[string]float64, currency string) float64 {
	cur := strings.ToUpper(strings.TrimSpace(currency))
	if r, ok := rates[cur]; ok && r > 0 {
		return r
	}
	logger.LogError("no FX rate for currency %q; amount excluded", cur)
	return 0
}
