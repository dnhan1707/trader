package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Define an enum to control the ingestion phase
type IngestMode int

const (
	ModeFilings IngestMode = iota
	ModeHoldings
)

type Client struct {
	DB_USER     string
	DB_PASSWORD string
}

func (c *Client) main() {
	// DB connection
	dsn := fmt.Sprintf("postgres://%s:%s@127.0.0.1:5434/13f_filings?sslmode=disable", c.DB_USER, c.DB_PASSWORD)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("open db:", err)
	}
	defer db.Close()

	root := "internal/database/dataset"

	// --- PASS 1: FILINGS ---
	log.Println("=== STARTING PASS 1: FILINGS METADATA ===")
	if err := walkDataset(db, root, ModeFilings); err != nil {
		log.Fatal("Pass 1 (Filings) failed:", err)
	}
	log.Println("=== PASS 1 COMPLETE ===")

	// --- PASS 2: HOLDINGS ---
	log.Println("=== STARTING PASS 2: HOLDINGS DATA ===")
	if err := walkDataset(db, root, ModeHoldings); err != nil {
		log.Fatal("Pass 2 (Holdings) failed:", err)
	}
	log.Println("=== PASS 2 COMPLETE ===")

	log.Println("Ingest finished successfully.")
}

// walkDataset wraps the filepath.Walk logic to keep main() clean
func walkDataset(db *sql.DB, root string, mode IngestMode) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(strings.ToLower(info.Name()), ".zip") {
			return nil
		}

		// Optional: Log progress so you know it's working
		if mode == ModeFilings {
			log.Println("[Pass 1] Processing filings in:", filepath.Base(path))
		} else {
			log.Println("[Pass 2] Processing holdings in:", filepath.Base(path))
		}

		return processZip(db, path, mode)
	})
}

func processZip(db *sql.DB, zipPath string, mode IngestMode) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	// We separate logic based on the mode to avoid unnecessary file reads.

	// --- MODE FILINGS: Look for SUBMISSION/COVERPAGE only ---
	if mode == ModeFilings {
		var subRows, coverRows []map[string]string

		for _, f := range r.File {
			name := strings.ToUpper(filepath.Base(f.Name))

			// Skip INFOTABLES in this pass
			if strings.Contains(name, "INFOTABLE") {
				continue
			}

			if strings.Contains(name, "SUBMISSION") && strings.HasSuffix(name, ".TSV") {
				subRows, err = readTSVFile(f)
				if err != nil {
					return fmt.Errorf("read SUBMISSION: %w", err)
				}
			} else if strings.Contains(name, "COVERPAGE") && strings.HasSuffix(name, ".TSV") {
				coverRows, err = readTSVFile(f)
				if err != nil {
					return fmt.Errorf("read COVERPAGE: %w", err)
				}
			}
		}

		if len(subRows) == 0 {
			// Some zips might only be holdings or empty, just return
			return nil
		}

		// Build lookup for cover pages
		coverByAcc := make(map[string]map[string]string, len(coverRows))
		for _, row := range coverRows {
			acc := strings.TrimSpace(row["ACCESSION_NUMBER"])
			if acc != "" {
				coverByAcc[acc] = row
			}
		}

		// Upsert filings
		for _, subRow := range subRows {
			acc := strings.TrimSpace(subRow["ACCESSION_NUMBER"])
			if acc == "" {
				continue
			}
			if err := upsertFiling(db, subRow, coverByAcc[acc]); err != nil {
				return fmt.Errorf("upsert filing %s: %w", acc, err)
			}
		}
		return nil
	}

	// --- MODE HOLDINGS: Look for INFOTABLE only ---
	if mode == ModeHoldings {
		for _, f := range r.File {
			name := strings.ToUpper(filepath.Base(f.Name))

			if strings.Contains(name, "INFOTABLE") && strings.HasSuffix(name, ".TSV") {
				// We found an infotable, ingest it immediately
				if err := ingestHoldingsTSV(db, f); err != nil {
					return fmt.Errorf("ingest INFOTABLE in %s: %w", zipPath, err)
				}
			}
		}
		return nil
	}

	return nil
}

// ... [Keep readTSVFile, readSingleRowTSV, ingestHoldingsTSV, upsertFiling, readTSV, nullableNumeric exactly as they were] ...
// (Include the rest of your helper functions below here unchanged)

func readTSVFile(f *zip.File) ([]map[string]string, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return readTSV(rc)
}

func ingestHoldingsTSV(db *sql.DB, f *zip.File) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	r := csv.NewReader(rc)
	r.Comma = '\t'
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("read INFOTABLE header: %w", err)
	}
	for i := range header {
		header[i] = strings.ToUpper(strings.TrimSpace(header[i]))
	}

	colIdx := func(name string) int {
		name = strings.ToUpper(name)
		for i, h := range header {
			if h == name {
				return i
			}
		}
		return -1
	}

	iAcc := colIdx("ACCESSION_NUMBER")
	iCusip := colIdx("CUSIP")
	iName := colIdx("NAMEOFISSUER")
	iVal := colIdx("VALUE")
	iShares := colIdx("SSHPRNAMT")
	iType := colIdx("SSHPRNAMTTYPE")

	if iAcc < 0 || iCusip < 0 || iName < 0 || iVal < 0 || iShares < 0 || iType < 0 {
		return fmt.Errorf("INFOTABLE missing required columns")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// temp staging table (no unique constraint)
	if _, err := tx.Exec(`
        CREATE TEMP TABLE holdings_stage (
            accession_number TEXT,
            cusip TEXT,
            name_of_issuer TEXT,
            value NUMERIC,
            shares NUMERIC,
            share_type TEXT
        ) ON COMMIT DROP;
    `); err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	stmt, err := tx.Prepare(pq.CopyIn(
		"holdings_stage",
		"accession_number",
		"cusip",
		"name_of_issuer",
		"value",
		"shares",
		"share_type",
	))
	if err != nil {
		return fmt.Errorf("prepare COPY: %w", err)
	}

	rowCount := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			stmt.Close()
			return fmt.Errorf("read INFOTABLE row: %w", err)
		}
		if len(rec) <= iAcc {
			continue
		}

		acc := strings.TrimSpace(rec[iAcc])
		if acc == "" {
			continue
		}

		cusip := ""
		if iCusip >= 0 && iCusip < len(rec) {
			cusip = strings.TrimSpace(rec[iCusip])
		}
		name := ""
		if iName >= 0 && iName < len(rec) {
			name = strings.TrimSpace(rec[iName])
		}
		val := ""
		if iVal >= 0 && iVal < len(rec) {
			val = strings.TrimSpace(rec[iVal])
		}
		shares := ""
		if iShares >= 0 && iShares < len(rec) {
			shares = strings.TrimSpace(rec[iShares])
		}
		shareType := ""
		if iType >= 0 && iType < len(rec) {
			shareType = strings.TrimSpace(rec[iType])
		}

		if _, err := stmt.Exec(
			acc,
			cusip,
			name,
			nullableNumeric(val),
			nullableNumeric(shares),
			shareType,
		); err != nil {
			stmt.Close()
			return fmt.Errorf("COPY holdings_stage row (acc=%s cusip=%s): %w", acc, cusip, err)
		}
		rowCount++
	}

	// end COPY
	if _, err := stmt.Exec(); err != nil {
		stmt.Close()
		return fmt.Errorf("final COPY exec: %w", err)
	}
	stmt.Close() // Close specifically here before the INSERT SELECT

	// move from stage -> real table with ON CONFLICT
	// Note: We are relying on the fact that Pass 1 populated the 'filings' table.
	// We assume 'filings' is the parent table for 'holdings' (if you have foreign keys).
	if _, err := tx.Exec(`
        INSERT INTO holdings (accession_number, cusip, name_of_issuer, value, shares, share_type)
        SELECT accession_number, cusip, name_of_issuer, value, shares, share_type
        FROM holdings_stage
        ON CONFLICT (accession_number, cusip, share_type) DO NOTHING;
    `); err != nil {
		return fmt.Errorf("insert from stage: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit COPY tx: %w", err)
	}

	log.Printf("ingestHoldingsTSV: staged %d rows via COPY", rowCount)
	return nil
}

func upsertFiling(db *sql.DB, sub, cover map[string]string) error {
	acc := sub["ACCESSION_NUMBER"]
	if acc == "" {
		return fmt.Errorf("missing ACCESSION_NUMBER in SUBMISSION")
	}

	parseDate := func(s string) *time.Time {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		t, err := time.Parse("02-Jan-2006", s)
		if err != nil {
			return nil
		}
		return &t
	}

	var (
		cik          = sub["CIK"]
		filingDate   = parseDate(sub["FILING_DATE"])
		reportPeriod = parseDate(sub["PERIODOFREPORT"])
		managerName  string
	)
	if cover != nil {
		managerName = cover["FILINGMANAGER_NAME"]
	}

	// Skip filings with missing required dates
	if filingDate == nil || reportPeriod == nil {
		log.Printf("WARN: Skipping filing %s due to missing dates", acc)
		return nil
	}

	stmt := `
    INSERT INTO filings (accession_number, cik, manager_name, report_period, filing_date)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (accession_number) DO UPDATE
    SET cik = EXCLUDED.cik,
        manager_name = COALESCE(EXCLUDED.manager_name, filings.manager_name),
        report_period = EXCLUDED.report_period,
        filing_date = EXCLUDED.filing_date;
    `

	_, err := db.Exec(stmt, acc, cik, managerName, reportPeriod, filingDate)
	return err
}

func readTSV(r io.Reader) ([]map[string]string, error) {
	c := csv.NewReader(r)
	c.Comma = '\t'
	c.FieldsPerRecord = -1

	header, err := c.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	for i := range header {
		header[i] = strings.ToUpper(strings.TrimSpace(header[i]))
	}

	var rows []map[string]string
	for {
		rec, err := c.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		row := make(map[string]string, len(header))
		for i, col := range header {
			if i < len(rec) {
				row[col] = strings.TrimSpace(rec[i])
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func nullableNumeric(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return s
}

func main() {
	client := &Client{
		DB_USER:     os.Getenv("DB_USER"),
		DB_PASSWORD: os.Getenv("DB_PASSWORD"),
	}

	// // Set defaults if env vars not set
	if client.DB_USER == "" {
		client.DB_USER = "trader_app" // or your default username
	}
	if client.DB_PASSWORD == "" {
		client.DB_PASSWORD = "trader_app_123"
	}

	client.main()
}
