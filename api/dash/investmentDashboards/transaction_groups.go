package investmentdashboards

import (
	"CimplrCorpSaas/api/constants"
	"fmt"
	"sort"
	"time"
)

// TransactionDetailGroup is a labelled block of transactions (YTD vs prior FY).
type TransactionDetailGroup struct {
	Heading string              `json:"heading"`
	Period  string              `json:"period,omitempty"`
	Rows    []TransactionDetail `json:"rows"`
}

// buildTransactionDetailGroups splits all portfolio transactions into YTD (current FY) and prior FY sections.
func buildTransactionDetailGroups(all []TransactionDetail, currentFYStart time.Time) []TransactionDetailGroup {
	if len(all) == 0 {
		return []TransactionDetailGroup{}
	}

	fyStartStr := currentFYStart.Format(constants.DateFormat)
	ytd := make([]TransactionDetail, 0, len(all))
	priorByFY := make(map[string][]TransactionDetail)
	priorOrder := make([]string, 0)

	for _, tx := range all {
		if tx.TransactionDate >= fyStartStr {
			ytd = append(ytd, tx)
			continue
		}
		label := indianFYLabelForDate(tx.TransactionDate)
		if _, ok := priorByFY[label]; !ok {
			priorOrder = append(priorOrder, label)
		}
		priorByFY[label] = append(priorByFY[label], tx)
	}

	sort.Slice(priorOrder, func(i, j int) bool { return priorOrder[i] > priorOrder[j] })

	groups := make([]TransactionDetailGroup, 0, 1+len(priorOrder))
	groups = append(groups, TransactionDetailGroup{
		Heading: fmt.Sprintf("FY %d-%d (YTD)", currentFYStart.Year(), currentFYStart.Year()+1),
		Period:  fmt.Sprintf("%s → today", fyStartStr),
		Rows:    ytd,
	})
	for _, label := range priorOrder {
		rows := priorByFY[label]
		if len(rows) == 0 {
			continue
		}
		groups = append(groups, TransactionDetailGroup{
			Heading: label,
			Period:  "Before current financial year",
			Rows:    rows,
		})
	}
	return groups
}

func indianFYLabelForDate(dateStr string) string {
	t, err := time.Parse(constants.DateFormat, dateStr)
	if err != nil {
		return "Prior years"
	}
	y := t.Year()
	if t.Month() >= time.April {
		return fmt.Sprintf("FY %d-%d", y, y+1)
	}
	return fmt.Sprintf("FY %d-%d", y-1, y)
}

// transactionDetailPayload builds flat YTD list + grouped sections for dashboard APIs.
func transactionDetailPayload(all []TransactionDetail, fyStart time.Time) (ytd []TransactionDetail, groups []TransactionDetailGroup) {
	groups = buildTransactionDetailGroups(all, fyStart)
	if len(groups) > 0 {
		ytd = groups[0].Rows
	}
	return ytd, groups
}
