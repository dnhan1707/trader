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

	"github.com/joho/godotenv"

	_ "github.com/lib/pq"
)

type Config struct {
	DB_USER     string
	DB_PASSWORD string
}

type InstitutionalOwnership struct {
	CIK           string
	CUSIP         string
	NameOfIssuer  string
	SharesHeld    int64
	TotalValueUSD float64
	ManagerName   string
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

	csvFilePath := "internal/database/dataset/final_ownership_data.csv"

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
        INSERT INTO institutional_ownership (cik, cusip, name_of_issuer, shares_held, total_value_usd, manager_name)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (cik, cusip) DO UPDATE SET
            name_of_issuer = EXCLUDED.name_of_issuer,
            shares_held = EXCLUDED.shares_held,
            total_value_usd = EXCLUDED.total_value_usd,
            manager_name = EXCLUDED.manager_name
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

		// Parse CSV record to struct
		ownership, err := parseCSVRecord(record)
		if err != nil {
			fmt.Printf("Skipping invalid record: %v, error: %v\n", record, err)
			continue
		}

		// Insert into database
		_, err = tx.Stmt(stmt).Exec(
			ownership.CIK,
			ownership.CUSIP,
			ownership.NameOfIssuer,
			ownership.SharesHeld,
			ownership.TotalValueUSD,
			ownership.ManagerName,
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

func parseCSVRecord(record []string) (*InstitutionalOwnership, error) {
	if len(record) < 6 {
		return nil, fmt.Errorf("insufficient columns in record")
	}

	// CSV columns: "CIK","CUSIP","NAMEOFISSUER","SSHPRNAMT","VALUE","FILINGMANAGER_NAME"
	// Parse shares held (SSHPRNAMT)
	sharesHeld, err := parseInt64(record[3])
	if err != nil {
		return nil, fmt.Errorf("invalid shares held: %w", err)
	}

	// Parse total value (VALUE)
	totalValue, err := parseFloat64(record[4])
	if err != nil {
		return nil, fmt.Errorf("invalid total value: %w", err)
	}

	return &InstitutionalOwnership{
		CIK:           strings.TrimSpace(record[0]),
		CUSIP:         strings.TrimSpace(record[1]),
		NameOfIssuer:  strings.TrimSpace(record[2]),
		SharesHeld:    sharesHeld,
		TotalValueUSD: totalValue,
		ManagerName:   strings.TrimSpace(record[5]),
	}, nil
}

func parseInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
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
