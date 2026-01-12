package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"

	_ "github.com/lib/pq"
)

type Config struct {
	DB_USER     string
	DB_PASSWORD string
}

type InsiderOwnership struct {
	IssuerCIK               string
	RptOwnerCIK             string
	DirectIndirectOwnership string
	IssuerName              string
	RptOwnerName            string
	Shares                  float64
	FilingDate              time.Time
	TransCode               string
}

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	// Load config
	c := Config{
		DB_USER:     os.Getenv("DB_USER"),
		DB_PASSWORD: os.Getenv("DB_PASSWORD"),
	}

	// Debug: print what credentials are being used (remove in production)
	fmt.Printf("Using DB_USER: %s\n", c.DB_USER)

	csvFilePath := "internal/database/dataset/345Final.csv"

	// Connect to database
	dsn := fmt.Sprintf("postgres://%s:%s@127.0.0.1:5434/13f_filings?sslmode=disable", c.DB_USER, c.DB_PASSWORD)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	fmt.Println("Connected to database successfully")

	// Ingest CSV data
	if err := ingestCSV(db, csvFilePath); err != nil {
		log.Fatal("Failed to ingest CSV:", err)
	}

	fmt.Println("Data ingestion completed successfully")
}

func ingestCSV(db *sql.DB, csvFilePath string) error {
	// Open CSV file
	file, err := os.Open(csvFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read header row
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	fmt.Printf("CSV Headers: %v\n", headers)

	// Prepare SQL statement
	stmt, err := db.Prepare(`
        INSERT INTO insider_ownership (issuer_cik, rpt_owner_cik, direct_indirect_ownership, issuer_name, rpt_owner_name, shares, filing_date, trans_code)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Begin transaction for better performance
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to read CSV record: %w", err)
		}

		// Skip empty records or records with insufficient columns
		if len(record) < 9 {
			continue
		}

		// Parse CSV record to struct
		insider, err := parseCSVRecord(record)
		if err != nil {
			fmt.Printf("Skipping invalid record: %v, error: %v\n", record, err)
			continue
		}

		// Insert into database
		_, err = tx.Stmt(stmt).Exec(
			insider.IssuerCIK,
			insider.RptOwnerCIK,
			insider.DirectIndirectOwnership,
			insider.IssuerName,
			insider.RptOwnerName,
			insider.Shares,
			insider.FilingDate,
			insider.TransCode,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert record: %w", err)
		}

		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("Processed %d rows...\n", rowCount)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Successfully inserted %d records\n", rowCount)
	return nil
}

func parseCSVRecord(record []string) (*InsiderOwnership, error) {
	if len(record) < 9 {
		return nil, fmt.Errorf("insufficient columns in record")
	}

	// CSV columns: ,ISSUERCIK,RPTOWNERCIK,DIRECT_INDIRECT_OWNERSHIP,ISSUERNAME,RPTOWNERNAME,SHARES,FILING_DATE,TRANS_CODE
	// Index:       0,1,       2,          3,                        4,         5,           6,     7,          8

	// Parse shares
	shares, err := parseFloat64(record[6])
	if err != nil {
		return nil, fmt.Errorf("invalid shares: %w", err)
	}

	// Parse filing date
	filingDate, err := parseDate(record[7])
	if err != nil {
		return nil, fmt.Errorf("invalid filing date: %w", err)
	}

	// Validate direct/indirect ownership
	directIndirect := strings.TrimSpace(record[3])
	if directIndirect != "D" && directIndirect != "I" {
		return nil, fmt.Errorf("invalid direct_indirect_ownership: %s", directIndirect)
	}

	// Validate transaction code (single character)
	transCode := strings.TrimSpace(record[8])
	if len(transCode) != 1 {
		return nil, fmt.Errorf("invalid transaction code: %s", transCode)
	}

	return &InsiderOwnership{
		IssuerCIK:               strings.TrimSpace(record[1]),
		RptOwnerCIK:             strings.TrimSpace(record[2]),
		DirectIndirectOwnership: directIndirect,
		IssuerName:              strings.TrimSpace(record[4]),
		RptOwnerName:            strings.TrimSpace(record[5]),
		Shares:                  shares,
		FilingDate:              filingDate,
		TransCode:               transCode,
	}, nil
}

func parseFloat64(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	// Remove commas if present
	s = strings.ReplaceAll(s, ",", "")
	return strconv.ParseFloat(s, 64)
}

func parseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty date string")
	}

	// Try multiple date formats
	formats := []string{
		"2006-01-02",          // YYYY-MM-DD
		"01/02/2006",          // MM/DD/YYYY
		"2006/01/02",          // YYYY/MM/DD
		"2006-01-02 15:04:05", // YYYY-MM-DD HH:MM:SS
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", s)
}
