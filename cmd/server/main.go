package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/richatamta/guidecx-go-reporting/internal/bq"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	projectID := getenv("GOOGLE_CLOUD_PROJECT", os.Getenv("PROJECT_ID"))
	if projectID == "" {
		log.Fatal("Missing env: GOOGLE_CLOUD_PROJECT (or PROJECT_ID)")
	}
	location := os.Getenv("BQ_LOCATION") // e.g. asia-south2

	// BigQuery client
	ctx := context.Background()
	client, err := bq.MustFromEnv(ctx, projectID, location)
	if err != nil {
		log.Fatalf("BigQuery init failed: %v", err)
	}
	defer client.Close()

	r := gin.New()
	r.Use(gin.Recovery())

	// Serve your dashboard + assets
	r.Static("/static", "./static")
	r.StaticFile("/", "./index.html")

	// Healthcheck
	r.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	// ---- Data API: fetch ALL rows ----
	r.GET("/report", func(c *gin.Context) {
		reqCtx, cancel := context.WithTimeout(c.Request.Context(), 300*time.Second)
		defer cancel()

		rows, schema, err := client.FetchAllRows(reqCtx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		total := len(rows)
		sql := os.Getenv("DASH_QUERY")

		c.JSON(http.StatusOK, gin.H{
			"schema": schema,
			"rows":   rows,
			"count":  total,
			"sql":    sql,
		})
	})

	port := getenv("PORT", "8080") // Render sets PORT
	log.Printf("listening on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}
