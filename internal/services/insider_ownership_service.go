package services

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/dnhan1707/trader/internal/massive"
)

type InsiderOwnershipService struct {
	db      *sql.DB
	massive *massive.Client
}

type TopInsider struct {
	RptOwnerCIK             string    `json:"rpt_owner_cik"`
	RptOwnerName            string    `json:"rpt_owner_name"`
	TotalShares             float64   `json:"total_shares"`
	LastFilingDate          time.Time `json:"last_filing_date"`
	DirectIndirectOwnership string    `json:"direct_indirect_ownership"`
	LastTransCode           string    `json:"last_trans_code"`
	ImpliedPrice            float64   `json:"implied_price"`
	TotalValueUSD           float64   `json:"total_value_usd"`
}

type TopInsidersResponse struct {
	IssuerCIK   string       `json:"issuer_cik"`
	CompanyName string       `json:"company_name"`
	Ticker      string       `json:"ticker"`
	StartYear   int          `json:"start_year"`
	TopInsiders []TopInsider `json:"top_insiders"`
}

type CompanyDetailWithCIK struct {
	Name              string  `json:"company_name"`
	PricePerShare     float64 `json:"price_per_share"`
	SharesOutstanding int64   `json:"shares_outstanding"`
	CIK               string  `json:"cik"`
}

func NewInsiderOwnershipService(db *sql.DB, massive *massive.Client) *InsiderOwnershipService {
	return &InsiderOwnershipService{
		db:      db,
		massive: massive,
	}
}

// GetTopInsidersFiltered gets top N insider owners who have filed a report on or after 'start_year'
// This filters out deceased or long-gone executives by only including recent activity
func (s *InsiderOwnershipService) GetTopInsidersFiltered(ticker string, startYear int, limit int) (*TopInsidersResponse, error) {
	// 1. Validate inputs
	if ticker == "" {
		return nil, fmt.Errorf("ticker is required")
	}
	if startYear <= 1990 || startYear > 2030 {
		return nil, fmt.Errorf("start year must be between 1990 and 2030")
	}
	if limit <= 0 {
		limit = 10 // default
	}

	// 2. Get company details including CIK from ticker
	companyDetail, err := s.getCompanyByTickerWithCIK(ticker)
	if err != nil {
		return nil, fmt.Errorf("failed to get company details for ticker %s: %w", ticker, err)
	}

	// 3. Query database with aggregation using the CIK
	query := `
		SELECT 
			rpt_owner_cik,
			rpt_owner_name,
			SUM(shares) as total_shares,
			MAX(filing_date) as last_filing_date,
			STRING_AGG(DISTINCT direct_indirect_ownership, ', ' ORDER BY direct_indirect_ownership) as ownership_types,
			(ARRAY_AGG(trans_code ORDER BY filing_date DESC))[1] as last_trans_code,
			(ARRAY_AGG(issuer_name ORDER BY filing_date DESC))[1] as company_name
		FROM insider_ownership 
		WHERE issuer_cik = $1 
			AND EXTRACT(YEAR FROM filing_date) >= $2
			AND shares > 0
		GROUP BY rpt_owner_cik, rpt_owner_name
		ORDER BY total_shares DESC 
		LIMIT $3
	`

	rows, err := s.db.Query(query, companyDetail.CIK, startYear, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query insider ownership: %w", err)
	}
	defer rows.Close()

	var insiders []TopInsider
	var companyName string

	for rows.Next() {
		var insider TopInsider
		err := rows.Scan(
			&insider.RptOwnerCIK,
			&insider.RptOwnerName,
			&insider.TotalShares,
			&insider.LastFilingDate,
			&insider.DirectIndirectOwnership,
			&insider.LastTransCode,
			&companyName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan insider row: %w", err)
		}

		insiders = append(insiders, insider)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	if len(insiders) == 0 {
		return nil, fmt.Errorf("no active insider filings found for ticker %s (CIK: %s) since %d", ticker, companyDetail.CIK, startYear)
	}

	// 4. Calculate implied price and total value for each insider
	for i := range insiders {
		if companyDetail.PricePerShare > 0 {
			insiders[i].ImpliedPrice = companyDetail.PricePerShare
			insiders[i].TotalValueUSD = companyDetail.PricePerShare * insiders[i].TotalShares
		}
	}

	return &TopInsidersResponse{
		IssuerCIK:   companyDetail.CIK,
		CompanyName: companyName,
		Ticker:      ticker,
		StartYear:   startYear,
		TopInsiders: insiders,
	}, nil
}

// Helper function to get company details by ticker (reused from institutional service pattern)
func (s *InsiderOwnershipService) getCompanyByTicker(ticker string) (*CompanyDetail, error) {
	tickerDetail, err := s.massive.GetTickerDetails(ticker)
	if err != nil {
		return nil, fmt.Errorf("failed to get ticker details for %s: %w", ticker, err)
	}

	// Access the results from the map
	results, ok := tickerDetail["results"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format for ticker %s", ticker)
	}

	// Extract name
	name, ok := results["name"].(string)
	if !ok || name == "" {
		return nil, fmt.Errorf("company name not found for ticker %s", ticker)
	}

	// Handle different possible types for weighted_shares_outstanding
	var weightedSharesOutstanding int64
	switch v := results["weighted_shares_outstanding"].(type) {
	case int64:
		weightedSharesOutstanding = v
	case float64:
		weightedSharesOutstanding = int64(v)
	case int:
		weightedSharesOutstanding = int64(v)
	default:
		return nil, fmt.Errorf("invalid weighted_shares_outstanding type for ticker %s", ticker)
	}

	// Handle different possible types for market_cap
	var marketCap float64
	switch v := results["market_cap"].(type) {
	case float64:
		marketCap = v
	case int64:
		marketCap = float64(v)
	case int:
		marketCap = float64(v)
	default:
		return nil, fmt.Errorf("invalid market_cap type for ticker %s", ticker)
	}

	var pricePerShare float64
	if weightedSharesOutstanding > 0 {
		pricePerShare = marketCap / float64(weightedSharesOutstanding)
	}

	return &CompanyDetail{
		Name:              name,
		SharesOutstanding: weightedSharesOutstanding,
		PricePerShare:     pricePerShare,
	}, nil
}

// Helper function to get company details with CIK by ticker
func (s *InsiderOwnershipService) getCompanyByTickerWithCIK(ticker string) (*CompanyDetailWithCIK, error) {
	tickerDetail, err := s.massive.GetTickerDetails(ticker)
	if err != nil {
		return nil, fmt.Errorf("failed to get ticker details for %s: %w", ticker, err)
	}

	// Access the results from the map
	results, ok := tickerDetail["results"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format for ticker %s", ticker)
	}

	// Extract name
	name, ok := results["name"].(string)
	if !ok || name == "" {
		return nil, fmt.Errorf("company name not found for ticker %s", ticker)
	}

	// Extract CIK
	cik, ok := results["cik"].(string)
	if !ok || cik == "" {
		return nil, fmt.Errorf("CIK not found for ticker %s", ticker)
	}

	// Strip leading zeros from CIK to match database format
	// CIK from API might be "0001018724" but database stores "1018724"
	normalizedCIK := stripLeadingZeros(cik)

	// Handle different possible types for weighted_shares_outstanding
	var weightedSharesOutstanding int64
	switch v := results["weighted_shares_outstanding"].(type) {
	case int64:
		weightedSharesOutstanding = v
	case float64:
		weightedSharesOutstanding = int64(v)
	case int:
		weightedSharesOutstanding = int64(v)
	default:
		return nil, fmt.Errorf("invalid weighted_shares_outstanding type for ticker %s", ticker)
	}

	// Handle different possible types for market_cap
	var marketCap float64
	switch v := results["market_cap"].(type) {
	case float64:
		marketCap = v
	case int64:
		marketCap = float64(v)
	case int:
		marketCap = float64(v)
	default:
		return nil, fmt.Errorf("invalid market_cap type for ticker %s", ticker)
	}

	var pricePerShare float64
	if weightedSharesOutstanding > 0 {
		pricePerShare = marketCap / float64(weightedSharesOutstanding)
	}

	return &CompanyDetailWithCIK{
		Name:              name,
		SharesOutstanding: weightedSharesOutstanding,
		PricePerShare:     pricePerShare,
		CIK:               normalizedCIK,
	}, nil
}

// GetTopInsidersByCIK is a simpler version that gets top insiders by ticker without date filtering
func (s *InsiderOwnershipService) GetTopInsidersByTicker(ticker string, limit int) (*TopInsidersResponse, error) {
	// Use current year - 5 as default start year to get recent activity
	currentYear := time.Now().Year()
	startYear := currentYear - 5

	return s.GetTopInsidersFiltered(ticker, startYear, limit)
}

// stripLeadingZeros removes leading zeros from CIK to match database format
// Example: "0001018724" -> "1018724"
func stripLeadingZeros(cik string) string {
	// Convert to int and back to string to remove leading zeros
	if num, err := strconv.Atoi(cik); err == nil {
		return strconv.Itoa(num)
	}
	// If conversion fails, return original (shouldn't happen for valid CIKs)
	return cik
}
