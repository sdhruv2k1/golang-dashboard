package bq

import (
	"context"
	"errors"
	"fmt"
	"os"

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

// Runs SQL from DASH_QUERY and returns rows as []map[string]any + the column names.
func (c *Client) FetchRawRows(ctx context.Context) ([]map[string]any, []string, error) {
	sql := os.Getenv("DASH_QUERY")
	if sql == "" {
		return nil, nil, errors.New("DASH_QUERY env is empty; set your SELECT statement")
	}

	q := c.raw.Query(sql)
	if c.location != "" {
		q.Location = c.location // must match dataset location
	}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("query read error: %w", err)
	}

	// Schema names
	colNames := make([]string, len(it.Schema))
	for i, f := range it.Schema {
		colNames[i] = f.Name
	}

	var out []map[string]any
	for {
		var vals []bigquery.Value
		if err := it.Next(&vals); err == iterator.Done {
			break
		} else if err != nil {
			return nil, nil, fmt.Errorf("iter error: %w", err)
		}

		row := make(map[string]any, len(colNames))
		for i, name := range colNames {
			if i < len(vals) {
				row[name] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, colNames, nil
}
