package services

import (
	"database/sql"
	"fmt"

	"github.com/dnhan1707/trader/internal/massive"
)

type InstitutionalOwnershipService struct {
	db      *sql.DB
	massive *massive.Client
}

type TopOwner struct {
	ManagerName   string  `json:"manager_name"`
	SharesHeld    int64   `json:"shares_held"`
	TotalValueUSD float64 `json:"total_value_usd"`
	Ownership     float64 `json:"ownership_percentage"` // as percentage
}

type TopOwnersResponse struct {
	CompanyName       string     `json:"company_name"`
	SharesOutstanding int64      `json:"shares_outstanding"`
	PricePerShare     float64    `json:"price_per_share"`
	TopOwners         []TopOwner `json:"top_owners"`
}

type CompanyDetail struct {
	Name              string  `json:"company_name"`
	PricePerShare     float64 `json:"price_per_share"`
	SharesOutstanding int64   `json:"shares_outstanding"`
}

func NewInstitutionalOwnershipService(db *sql.DB, massive *massive.Client) *InstitutionalOwnershipService {
	return &InstitutionalOwnershipService{
		db:      db,
		massive: massive,
	}
}

func (s *InstitutionalOwnershipService) GetTopOwnersByName(companyName string, limit int) (*TopOwnersResponse, error) {
	// 1. Get all owners for companies matching the name
	query := `
        SELECT 
            name_of_issuer,
            manager_name,
            shares_held,
            total_value_usd
        FROM institutional_ownership 
        WHERE LOWER(name_of_issuer) LIKE LOWER($1)
        AND shares_held > 0 
        AND total_value_usd > 0
        ORDER BY shares_held DESC
    `

	// Use LIKE pattern for partial matching
	searchPattern := "%" + companyName + "%"
	rows, err := s.db.Query(query, searchPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()

	var owners []TopOwner
	var actualCompanyName string

	for rows.Next() {
		var owner TopOwner
		var issuerName string

		err := rows.Scan(
			&issuerName,
			&owner.ManagerName,
			&owner.SharesHeld,
			&owner.TotalValueUSD,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Use the first issuer name as the actual company name
		if actualCompanyName == "" {
			actualCompanyName = issuerName
		}

		owners = append(owners, owner)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	if len(owners) == 0 {
		return nil, fmt.Errorf("no owners found for company: %s", companyName)
	}

	// 2. Calculate total institutional shares as fallback
	totalInstitutionalShares := int64(0)
	for _, owner := range owners {
		totalInstitutionalShares += owner.SharesHeld
	}

	// 3. Calculate ownership percentages (will be recalculated if we get actual shares outstanding)
	for i := range owners {
		if totalInstitutionalShares > 0 {
			owners[i].Ownership = (float64(owners[i].SharesHeld) / float64(totalInstitutionalShares)) * 100
		}
	}

	// 4. Limit results
	if limit > 0 && limit < len(owners) {
		owners = owners[:limit]
	}

	response := &TopOwnersResponse{
		CompanyName:       actualCompanyName,
		SharesOutstanding: totalInstitutionalShares,
		TopOwners:         owners,
	}

	return response, nil
}

func (s *InstitutionalOwnershipService) GetCompanyByTicker(ticker string) (*CompanyDetail, error) {
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

// Enhanced version that gets actual shares outstanding if ticker is provided
func (s *InstitutionalOwnershipService) GetTopOwnersByNameWithTicker(companyName, ticker string, limit int) (*TopOwnersResponse, error) {
	response, err := s.GetTopOwnersByName(companyName, limit)
	if err != nil {
		return nil, err
	}

	// If ticker is provided, get actual company details
	if ticker != "" {
		companyDetail, err := s.GetCompanyByTicker(ticker)
		if err == nil {
			// Recalculate ownership percentages with actual shares outstanding
			// and update total value using actual price per share
			for i := range response.TopOwners {
				// Update total value using actual price per share
				response.TopOwners[i].TotalValueUSD = companyDetail.PricePerShare * float64(response.TopOwners[i].SharesHeld)

				// Recalculate ownership percentage with actual shares outstanding
				if companyDetail.SharesOutstanding > 0 {
					response.TopOwners[i].Ownership = (float64(response.TopOwners[i].SharesHeld) / float64(companyDetail.SharesOutstanding)) * 100
				}
			}
			response.SharesOutstanding = companyDetail.SharesOutstanding
			response.PricePerShare = companyDetail.PricePerShare
			response.CompanyName = companyDetail.Name
		}
	}

	return response, nil
}
