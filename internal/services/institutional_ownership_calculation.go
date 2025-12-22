package services

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/dnhan1707/trader/internal/massive"
)

type InstitutionalOwnershipService struct {
	DB      *sql.DB
	Massive *massive.Client
}

// NewInstitutionalOwnershipService wires the DB into the service.
func NewInstitutionalOwnershipService(db *sql.DB, m *massive.Client) *InstitutionalOwnershipService {
	return &InstitutionalOwnershipService{DB: db, Massive: m}
}

type CompanyDetail struct {
	Name string `json:"name"`
	CIK  string `json:"cik"`
}

type TopOwner struct {
	Investor            string  `json:"investor"`
	Year                int     `json:"year"`
	Quarter             int     `json:"quarter"`
	NumberOfShares      float64 `json:"number_of_shares"`
	TotalValue          float64 `json:"total_value"`
	PortfolioPercentage float64 `json:"portfolio_percentage"`
	OwnershipPercentage float64 `json:"ownership_percentage"`
}

type TopOwnersResponse struct {
	CompanyName string     `json:"company_name"`
	Results     []TopOwner `json:"results"`
}

type TopHolder struct {
	NameOfIssuer        string  `json:"name_of_issuer"`
	TotalValue          float64 `json:"total_value"`
	TotalShares         float64 `json:"total_shares"`
	PortfolioPercentage float64 `json:"portfolio_percentage"`
}

type TopHoldersResponse struct {
	CIK     string      `json:"cik"`
	Results []TopHolder `json:"results"`
}

func (s *InstitutionalOwnershipService) resolveCUSIP(companyName string) (string, error) {
	// Step 1: Try exact match first
	query := `
        SELECT cusip, COUNT(*) as usage_count
        FROM holdings
        WHERE UPPER(name_of_issuer) LIKE UPPER($1)
        GROUP BY cusip
        ORDER BY COUNT(*) DESC
        LIMIT 1
    `

	var cusip string
	var count int
	err := s.DB.QueryRow(query, "%"+companyName+"%").Scan(&cusip, &count)
	if err == nil && cusip != "" {
		return cusip, nil
	}

	// Step 2: If no exact match, try core name extraction
	coreName := extractCoreName(companyName)
	err = s.DB.QueryRow(query, "%"+coreName+"%").Scan(&cusip, &count)
	if err == nil && cusip != "" {
		return cusip, nil
	}

	return "", fmt.Errorf("could not resolve CUSIP for company: %s", companyName)
}

// Helper function to extract core company name
func extractCoreName(companyName string) string {
	name := companyName

	// Remove common corporate suffixes
	suffixes := []string{" Inc.", " Inc", " Corporation", " Corp", " Corp.", " Com", " Ltd", " LLC", " LP"}
	for _, suffix := range suffixes {
		name = strings.Replace(name, suffix, "", -1)
	}

	// Trim spaces
	name = strings.TrimSpace(name)

	return name
}

func createReportPeriod(quarter, year string) (string, error) {
	// 13F "Report Period" is always the last day of the quarter.
	var reportPeriod string
	switch quarter {
	case "1":
		reportPeriod = year + "-03-31"
	case "2":
		reportPeriod = year + "-06-30"
	case "3":
		reportPeriod = year + "-09-30"
	case "4":
		reportPeriod = year + "-12-31"
	default:
		return "", fmt.Errorf("invalid quarter: %s", quarter)
	}

	return reportPeriod, nil

}

func (s *InstitutionalOwnershipService) getBasicSharesOutstanding(cik, year, quarter string) (float64, error) {

	reportPeriod, err := createReportPeriod(quarter, year)
	if err != nil {
		return 0, err
	}
	extra := map[string]string{
		"cik":            cik,
		"period_end.lte": reportPeriod,      // Find reports on or before 2024-03-31
		"sort":           "period_end.desc", // Get the newest one first
		"limit":          "1",
	}

	response, err := s.Massive.GetIncomeStatements(extra)
	if err != nil {
		return 0, fmt.Errorf("failed to get income statements for CIK %s: %w", cik, err)
	}

	results, ok := response["results"].([]interface{})
	if !ok || len(results) == 0 {
		return 0, fmt.Errorf("no income statement data found for CIK %s, year %s, quarter %s", cik, year, quarter)
	}

	// Get the first (most recent) result
	firstResult, ok := results[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid result format in income statement response")
	}

	basicShares, ok := firstResult["basic_shares_outstanding"].(float64)
	if !ok {
		// Try to handle if it's returned as a different numeric type
		if basicSharesInt, ok := firstResult["basic_shares_outstanding"].(int64); ok {
			basicShares = float64(basicSharesInt)
		} else if basicSharesInterface, exists := firstResult["basic_shares_outstanding"]; exists {
			return 0, fmt.Errorf("basic_shares_outstanding field exists but has unexpected type: %T", basicSharesInterface)
		} else {
			return 0, fmt.Errorf("basic_shares_outstanding field not found in income statement data")
		}
	}

	return basicShares, nil
}

func (s *InstitutionalOwnershipService) getCompanyDetail(ticker string) (*CompanyDetail, error) {
	tickerDetail, err := s.Massive.GetTickerDetails(ticker)
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
		return nil, fmt.Errorf("company CIK not found for ticker %s", ticker)
	}

	return &CompanyDetail{
		Name: name,
		CIK:  cik,
	}, nil
}

func (s *InstitutionalOwnershipService) TopNSharesOwner(
	ctx context.Context,
	ticker, year, quarter string,
	n int,
) (*TopOwnersResponse, error) {

	// 13F "Report Period" is always the last day of the quarter.
	reportPeriod, err := createReportPeriod(quarter, year)
	if err != nil {
		return nil, err
	}

	companyDetail, err := s.getCompanyDetail(ticker)
	if err != nil {
		return nil, err
	}
	companyName := companyDetail.Name
	cik := companyDetail.CIK

	// Step 1: Resolve company name to CUSIP
	cusip, err := s.resolveCUSIP(companyName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve CUSIP: %w", err)
	}

	// Get basic shares outstanding
	basicShares, err := s.getBasicSharesOutstanding(cik, year, quarter)
	if err != nil {
		return nil, fmt.Errorf("failed to get basic shares outstanding: %w", err)
	}

	// Simplified query following the 5-step logic
	query := `
        SELECT 
            f.manager_name,
            SUM(h.shares) as total_shares,
            SUM(h.value) as total_value,
            (SUM(h.value) / 
                NULLIF((
                    SELECT SUM(h2.value) 
                    FROM holdings h2 
                    JOIN filings f2 ON h2.accession_number = f2.accession_number 
                    WHERE f2.cik = f.cik AND f2.report_period = $1::date
                ), 0)
            ) * 100 as portfolio_pct
        FROM filings f
        JOIN holdings h ON f.accession_number = h.accession_number
        WHERE f.report_period = $1::date
          AND h.cusip = $2
          AND (h.share_type = 'SH' OR h.share_type IS NULL)  -- Only actual shares
        GROUP BY f.cik, f.manager_name
        ORDER BY SUM(h.shares) DESC
        LIMIT $3
    `

	rows, err := s.DB.QueryContext(ctx, query, reportPeriod, cusip, n)
	if err != nil {
		return nil, fmt.Errorf("failed to query top owners: %w", err)
	}
	defer rows.Close()

	var owners []TopOwner
	for rows.Next() {
		var owner TopOwner
		var managerName sql.NullString

		if err := rows.Scan(&managerName, &owner.NumberOfShares, &owner.TotalValue, &owner.PortfolioPercentage); err != nil {
			return nil, err
		}

		owner.Investor = managerName.String

		y, _ := strconv.Atoi(year)
		q, _ := strconv.Atoi(quarter)
		owner.Year = y
		owner.Quarter = q

		// Calculate ownership percentage
		if basicShares > 0 {
			owner.OwnershipPercentage = (owner.NumberOfShares / basicShares) * 100
		} else {
			owner.OwnershipPercentage = 0
		}

		owners = append(owners, owner)
	}

	return &TopOwnersResponse{
		CompanyName: companyName,
		Results:     owners,
	}, nil
}

/*
Let's say we're looking for Apple's top owners in Q1 2023:

Input data:

Filings table has multiple entries per manager (amendments)
Holdings table has millions of records for all companies

After Step 1 (unique_filings):
Only the latest filing per manager for Q1 2023

After Step 2 (company_holdings):
Manager A: 1B Apple shares worth $150B
Manager B: 800M Apple shares worth $120B
Manager C: 500M Apple shares worth $75B


After Step 3 (manager_totals):
Manager A total portfolio: $3T
Manager B total portfolio: $2.5T
Manager C total portfolio: $1T


Final Result:
Manager A: 1B shares, $150B value, 5.0% of portfolio
Manager B: 800M shares, $120B value, 4.8% of portfolio
Manager C: 500M shares, $75B value, 7.5% of portfolio

*/
