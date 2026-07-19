package investmentdashboards

import (
	benchmarks "CimplrCorpSaas/api/dash/benchmarks"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/portfolio"
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type portfolioVsBenchmarkInput struct {
	EntityFilter    string
	AllowedEntities []string
	Year            int
	Benchmark       string
	Flag            string // NSE/BSE chart window: 1M, 3M, 6M, 1Y / 12M
	Now             time.Time
}

type portfolioVsBenchmarkResult struct {
	Series        []BenchmarkPoint
	BenchmarkName string
	Provider      string
	DataSource    string
	IndexType     string // PRI = price return index from NSE/BSE chart API
}

func buildFinancialYearMonthEnds(year int, now time.Time) (monthNames []string, monthDates []string, monthEnds []time.Time) {
	monthNames = []string{"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}
	for i := 0; i < 12; i++ {
		var monthStart time.Time
		if i < 9 {
			monthStart = time.Date(year, time.Month(4+i), 1, 0, 0, 0, 0, time.UTC)
		} else {
			monthStart = time.Date(year+1, time.Month(i-8), 1, 0, 0, 0, 0, time.UTC)
		}
		monthEnd := monthStart.AddDate(0, 1, -1)
		if monthStart.After(now) {
			break
		}
		if monthEnd.After(now) {
			monthEnd = now
		}
		monthDates = append(monthDates, monthEnd.Format(constants.DateFormat))
		monthEnds = append(monthEnds, monthEnd.UTC())
	}
	return monthNames, monthDates, monthEnds
}

// trimMonthsByPeriodFlag keeps the last N month-end buckets for 1M/3M/6M/1Y.
// Portfolio and benchmark are re-indexed to 100 at the first month in the window.
func trimMonthsByPeriodFlag(flag string, monthNames []string, monthDates []string, monthEnds []time.Time) ([]string, []string, []time.Time) {
	n := len(monthDates)
	if n == 0 {
		return monthNames, monthDates, monthEnds
	}

	keep := n
	switch strings.ToUpper(strings.TrimSpace(flag)) {
	case "1M":
		keep = 1
		if n >= 2 {
			// One month is always indexed to 100 with no line; show prior month for context.
			keep = 2
		}
	case "3M":
		keep = 3
	case "6M":
		keep = 6
	case "1Y", "12M":
		keep = n
	default:
		keep = n
	}
	if keep > n {
		keep = n
	}
	if keep < 1 {
		keep = 1
	}
	if keep >= n {
		return monthNames, monthDates, monthEnds
	}
	start := n - keep
	return monthNames[start:], monthDates[start:], monthEnds[start:]
}

func computePortfolioVsBenchmarkSeries(ctx context.Context, pgxPool *pgxpool.Pool, in portfolioVsBenchmarkInput) (portfolioVsBenchmarkResult, error) {
	if in.Now.IsZero() {
		in.Now = time.Now()
	}
	if in.Benchmark == "" {
		in.Benchmark = constants.Nifty50
	}
	if in.Year == 0 {
		if in.Now.Month() >= time.April {
			in.Year = in.Now.Year()
		} else {
			in.Year = in.Now.Year() - 1
		}
	}

	monthNames, monthDates, monthEnds := buildFinancialYearMonthEnds(in.Year, in.Now)
	flag := in.Flag
	if flag == "" {
		flag = benchmarks.FYChartFlag(len(monthDates))
	}
	monthNames, monthDates, monthEnds = trimMonthsByPeriodFlag(flag, monthNames, monthDates, monthEnds)
	if len(monthDates) == 0 {
		return portfolioVsBenchmarkResult{
			Series:        []BenchmarkPoint{{Month: "Apr", Portfolio: 100, Benchmark: 100}},
			BenchmarkName: in.Benchmark,
		}, nil
	}

	monthlyAUM, monthlyFlows, err := portfolio.ComputeMonthlyPortfolioAUMForChart(ctx, pgxPool, in.EntityFilter, in.AllowedEntities, monthDates)
	if err != nil {
		return portfolioVsBenchmarkResult{}, err
	}

	def, _ := benchmarks.ResolveBenchmark(in.Benchmark)
	indexPoints, fetchErr := benchmarks.FetchIndexSeries(ctx, def, flag)
	dataSource := def.Provider
	if fetchErr != nil {
		dataSource = "fallback"
	}

	portfolioIndexed := benchmarks.BuildPortfolioIndexedSeriesTWR(monthlyAUM, monthlyFlows, len(monthDates))
	benchmarkIndexed := benchmarks.BuildIndexedMonthSeries(indexPoints, monthEnds)
	if len(benchmarkIndexed) == 0 && fetchErr == nil {
		benchmarkIndexed = make([]float64, len(monthDates))
		for i := range benchmarkIndexed {
			if i == 0 {
				benchmarkIndexed[i] = 100
			} else {
				benchmarkIndexed[i] = benchmarkIndexed[i-1]
			}
		}
	}
	if len(benchmarkIndexed) == 0 {
		benchmarkIndexed = fallbackBenchmarkIndexed(len(monthDates))
	}

	points := make([]BenchmarkPoint, 0, len(monthDates))
	for i := 0; i < len(monthDates) && i < len(monthNames); i++ {
		p := 100.0
		b := 100.0
		if i < len(portfolioIndexed) {
			p = portfolioIndexed[i]
		}
		if i < len(benchmarkIndexed) {
			b = benchmarkIndexed[i]
		}
		points = append(points, BenchmarkPoint{
			Month:     monthNames[i],
			Portfolio: benchmarks.RoundIndexed(p),
			Benchmark: benchmarks.RoundIndexed(b),
		})
	}

	return portfolioVsBenchmarkResult{
		Series:        points,
		BenchmarkName: def.Name,
		Provider:      def.Provider,
		DataSource:    dataSource,
		IndexType:     "PRI",
	}, nil
}

// fetchBenchmarkFYReturn returns indexed benchmark % change since FY start (e.g. 5.2 means +5.2%).
func fetchBenchmarkFYReturn(ctx context.Context, benchmark string, year int, now time.Time) float64 {
	_, _, monthEnds := buildFinancialYearMonthEnds(year, now)
	if len(monthEnds) == 0 {
		return 0
	}
	def, _ := benchmarks.ResolveBenchmark(benchmark)
	points, err := benchmarks.FetchIndexSeries(ctx, def, benchmarks.FYChartFlag(len(monthEnds)))
	if err != nil || len(points) == 0 {
		return 0
	}
	indexed := benchmarks.BuildIndexedMonthSeries(points, monthEnds)
	if len(indexed) == 0 {
		return 0
	}
	return benchmarks.RoundIndexed(indexed[len(indexed)-1] - 100)
}

func fallbackBenchmarkIndexed(monthCount int) []float64 {
	// Legacy placeholder monthly returns if live NSE/BSE fetch fails.
	monthlyReturns := []float64{0.8, 1.0, 0.6, 0.9, 1.1, 0.7, 0.5, 0.8, 1.2, 0.9, 0.6, 0.8}
	out := make([]float64, monthCount)
	idx := 100.0
	for i := 0; i < monthCount; i++ {
		if i == 0 {
			out[i] = 100
			continue
		}
		idx = idx * (1 + monthlyReturns[(i-1)%12]/100)
		out[i] = benchmarks.RoundIndexed(idx)
	}
	return out
}
