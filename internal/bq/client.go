package bq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Client struct {
	raw      *bigquery.Client
	location string
}

func (c *Client) Close() error { return c.raw.Close() }

// Prefers GOOGLE_APPLICATION_CREDENTIALS_JSON (inline), then file path, then ADC.
func MustFromEnv(ctx context.Context, projectID, location string) (*Client, error) {
	if projectID == "" {
		return nil, errors.New("projectID required")
	}

	if jsonStr := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"); jsonStr != "" {
		cl, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(jsonStr)))
		if err != nil {
			return nil, fmt.Errorf("bq client (json) error: %w", err)
		}
		return &Client{raw: cl, location: location}, nil
	}
	if path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); path != "" {
		cl, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(path))
		if err != nil {
			return nil, fmt.Errorf("bq client (file) error: %w", err)
		}
		return &Client{raw: cl, location: location}, nil
	}
	cl, err := bigquery.NewClient(ctx, projectID) // ADC (local dev)
	if err != nil {
		return nil, fmt.Errorf("bq client (ADC) error: %w", err)
	}
	return &Client{raw: cl, location: location}, nil
}

// ---------------- helpers ----------------

func (c *Client) runQuery(ctx context.Context, sql string) (*bigquery.RowIterator, error) {
	sql = cleanSQL(sql)
	log.Printf("[DEBUG] Running SQL:\n%s", sql)

	q := c.raw.Query(sql)
	q.QueryConfig.UseLegacySQL = false
	if c.location != "" {
		q.Location = c.location
	}
	return q.Read(ctx)
}

// Trim trailing semicolon
func cleanSQL(s string) string {
	return strings.TrimRight(strings.TrimSpace(s), ";")
}

// Universal scanner: works even when schema is nil
func scanAll(it *bigquery.RowIterator) ([]map[string]any, []string, error) {
	var out []map[string]any
	var colNames []string

	for {
		row := make(map[string]bigquery.Value)
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, nil, err
		}

		// infer column names from the first row
		if len(colNames) == 0 {
			for k := range row {
				colNames = append(colNames, k)
			}
		}

		// convert to generic map[string]any
		generic := make(map[string]any, len(row))
		for k, v := range row {
			generic[k] = v
		}
		out = append(out, generic)
	}

	return out, colNames, nil
}

// ---------------- Raw (no paging) ----------------

func (c *Client) FetchRawRows(ctx context.Context) ([]map[string]any, []string, error) {
	sql := os.Getenv("DASH_QUERY")
	if sql == "" {
		return nil, nil, errors.New("DASH_QUERY env is empty; set your SELECT statement")
	}
	log.Printf("[DEBUG] FetchRawRows DASH_QUERY: %s", sql)

	it, err := c.runQuery(ctx, sql)
	if err != nil {
		return nil, nil, fmt.Errorf("query read error: %w", err)
	}
	rows, schema, err := scanAll(it)
	if err != nil {
		return nil, nil, fmt.Errorf("iter error: %w", err)
	}
	return rows, schema, nil
}

// ---------------- Pagination + count ----------------

func (c *Client) FetchPageRows(ctx context.Context, limit, offset int64) ([]map[string]any, []string, error) {
	base := os.Getenv("DASH_QUERY")
	if base == "" {
		return nil, nil, errors.New("DASH_QUERY env is empty; set your SELECT statement")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	if offset < 0 {
		offset = 0
	}

	sql := fmt.Sprintf("SELECT * FROM (%s) LIMIT %d OFFSET %d", cleanSQL(base), limit, offset)
	log.Printf("[DEBUG] FetchPageRows SQL: %s", sql)

	it, err := c.runQuery(ctx, sql)
	if err != nil {
		return nil, nil, fmt.Errorf("paged query error: %w", err)
	}
	rows, schema, err := scanAll(it)
	if err != nil {
		return nil, nil, fmt.Errorf("paged iter error: %w", err)
	}
	return rows, schema, nil
}

// SELECT COUNT(*) FROM (<DASH_QUERY>)
func (c *Client) FetchCount(ctx context.Context) (int64, error) {
	base := os.Getenv("DASH_QUERY")
	if base == "" {
		return 0, errors.New("DASH_QUERY env is empty")
	}
	sql := fmt.Sprintf("SELECT COUNT(*) AS total FROM (%s)", cleanSQL(base))
	log.Printf("[DEBUG] FetchCount SQL: %s", sql)

	it, err := c.runQuery(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("count read error: %w", err)
	}
	row := make(map[string]bigquery.Value)
	if err := it.Next(&row); err != nil {
		return 0, fmt.Errorf("count iter error: %w", err)
	}
	if v, ok := row["total"]; ok {
		switch t := v.(type) {
		case int64:
			return t, nil
		case float64:
			return int64(t), nil
		default:
			return 0, fmt.Errorf("unexpected count type %T", t)
		}
	}
	return 0, errors.New("count column missing")
}

// ---------------- Convenience: fetch EVERYTHING ----------------

func (c *Client) FetchAllRows(ctx context.Context) ([]map[string]any, []string, error) {
	pageSize := int64(5000)
	if s := os.Getenv("PAGE_SIZE"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 {
			pageSize = n
		}
	}
	maxPages := int64(1000)
	if s := os.Getenv("MAX_PAGES"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 {
			maxPages = n
		}
	}

	var all []map[string]any
	var schema []string
	var offset int64 = 0

	for page := int64(0); page < maxPages; page++ {
		rows, sch, err := c.FetchPageRows(ctx, pageSize, offset)
		if err != nil {
			return nil, nil, err
		}
		if schema == nil {
			schema = sch
		}
		if len(rows) == 0 {
			break
		}
		all = append(all, rows...)
		offset += int64(len(rows))
	}

	return all, schema, nil
}
