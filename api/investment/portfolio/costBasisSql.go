package portfolio

// Shared cost-basis expressions for portfolio holdings and snapshot rebuilds.
// Uses weighted-average purchase NAV; remaining cost = remaining_units * wavg purchase NAV.
// Realized P&L = redemption proceeds - (redeemed_units * wavg purchase NAV).

const txIsPurchase = `LOWER(TRIM(transaction_type)) IN ('purchase','buy','subscription','bonus')`
const txIsSell = `LOWER(TRIM(transaction_type)) IN ('sell','redemption')`

const txPurchaseUnits = `SUM(CASE WHEN ` + txIsPurchase + ` THEN units ELSE 0 END)`
const txPurchaseNavAmount = `SUM(CASE WHEN ` + txIsPurchase + ` THEN nav * units ELSE 0 END)`
const txRedeemUnits = `SUM(CASE WHEN ` + txIsSell + ` THEN units ELSE 0 END)`
const txRedeemProceeds = `SUM(CASE WHEN ` + txIsSell + ` THEN amount ELSE 0 END)`
const txNetUnits = `SUM(CASE WHEN ` + txIsPurchase + ` THEN units WHEN ` + txIsSell + ` THEN -units ELSE units END)`

const txWavgPurchaseNav = `CASE WHEN ` + txPurchaseUnits + ` = 0 THEN 0 ELSE ` + txPurchaseNavAmount + ` / NULLIF(` + txPurchaseUnits + `, 0) END`

// TransactionSummaryMetrics are the aggregated cost / P&L columns inside transaction_summary.
const TransactionSummaryMetrics = `
        ` + txNetUnits + ` AS total_units,
        ` + txWavgPurchaseNav + ` AS avg_nav,
        CASE WHEN ` + txNetUnits + ` > 0
             THEN ` + txNetUnits + ` * (` + txWavgPurchaseNav + `)
             ELSE 0
        END AS total_invested_amount,
        CASE WHEN ` + txRedeemUnits + ` > 0
             THEN ` + txRedeemProceeds + ` - (` + txRedeemUnits + ` * (` + txWavgPurchaseNav + `))
             ELSE 0
        END AS realized_gain_loss`
