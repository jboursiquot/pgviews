package pgviews_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/jboursiquot/pgviews"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRepository(t *testing.T) {
	ctx := context.Background()

	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	defer testcontainers.CleanupContainer(t, pgc)
	require.NoError(t, err)

	err = pgc.Snapshot(ctx, postgres.WithSnapshotName("test-db"))
	require.NoError(t, err)

	dbConnStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	assert.NotEmpty(t, dbConnStr)

	t.Run("Test inserting a Product", func(t *testing.T) {
		t.Cleanup(func() {
			err = pgc.Restore(ctx)
			require.NoError(t, err)
		})

		repo, err := pgviews.NewRepository(ctx, dbConnStr)
		assert.NoError(t, err)

		c, err := repo.CreateProduct(ctx, pgviews.Product{
			Name:     "Test Product",
			Category: "Test Category",
		})
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.Equal(t, "Test Product", c.Name)
		assert.Equal(t, "Test Category", c.Category)
		assert.NotZero(t, c.Id)
	})

	t.Run("Test getting a Product by category", func(t *testing.T) {
		t.Cleanup(func() {
			err = pgc.Restore(ctx)
			require.NoError(t, err)
		})

		repo, err := pgviews.NewRepository(ctx, dbConnStr)
		assert.NoError(t, err)

		_, err = repo.CreateProduct(ctx, pgviews.Product{
			Name:     "Test Product",
			Category: "Test Category",
		})
		require.NoError(t, err)

		products, err := repo.GetProductsByCategory(ctx, "Test Category")
		assert.NoError(t, err)
		assert.NotNil(t, products)
		assert.Equal(t, 1, len(products))
		assert.Equal(t, "Test Product", products[0].Name)
		assert.Equal(t, "Test Category", products[0].Category)
	})
}

func TestGetCategorySales(t *testing.T) {
	ctx := context.Background()

	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	defer testcontainers.CleanupContainer(t, pgc)
	require.NoError(t, err)

	dbConnStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	assert.NotEmpty(t, dbConnStr)

	repo, err := pgviews.NewRepository(ctx, dbConnStr)
	require.NoError(t, err)

	category := "Electronics"
	daysInPast := 30

	// Capture query plan before benchmark
	query := `
		SELECT p.category, SUM(o.quantity) AS total_sold
		FROM orders o
		JOIN products p ON o.product_id = p.id
		WHERE p.category = $1 AND o.order_date >= CURRENT_DATE - $2::interval
		GROUP BY p.category;
	`
	params := map[string]any{
		"category":   category,
		"daysInPast": fmt.Sprintf("%d days", daysInPast),
	}
	if err := captureQueryPlan(ctx, dbConnStr, query, params, "query_plan_view_none.json"); err != nil {
		t.Logf("Failed to capture query plan: %v", err)
	}

	tests := map[string]struct {
		daysInPast int
		category   string
	}{
		"Orders in the past 30 days": {daysInPast: 30, category: category},
		"Orders in the past 60 days": {daysInPast: 60, category: category},
		"Orders in the past 90 days": {daysInPast: 90, category: category},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			orders, err := repo.GetCategorySales(ctx, tc.category, tc.daysInPast)
			assert.NoError(t, err, "fetching orders from last %d days for category %s should not return an error", tc.daysInPast, tc.category)
			assert.NotNil(t, orders)
		})
	}
}

func TestGetCategorySalesFromView(t *testing.T) {
	ctx := context.Background()

	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	defer testcontainers.CleanupContainer(t, pgc)
	require.NoError(t, err)

	dbConnStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	assert.NotEmpty(t, dbConnStr)

	repo, err := pgviews.NewRepository(ctx, dbConnStr)
	require.NoError(t, err)

	category := "Electronics"
	daysInPast := 30

	// Capture query plan before benchmark
	query := `
		SELECT category, SUM(daily_total_sold) AS total_sold
		FROM category_sales_view
		WHERE category = $1 AND order_date >= CURRENT_DATE - $2::interval
		GROUP BY category;
	`
	params := map[string]interface{}{
		"category":   category,
		"daysInPast": fmt.Sprintf("%d days", daysInPast),
	}
	if err := captureQueryPlan(ctx, dbConnStr, query, params, "query_plan_view_plain.json"); err != nil {
		t.Logf("Failed to capture query plan: %v", err)
	}

	tests := map[string]struct {
		daysInPast int
		category   string
	}{
		"Orders in the past 30 days": {daysInPast: 30, category: category},
		"Orders in the past 60 days": {daysInPast: 60, category: category},
		"Orders in the past 90 days": {daysInPast: 90, category: category},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err = repo.RefreshCategorySalesMaterializedView(ctx)
			assert.NoError(t, err)

			sales, err := repo.GetCategorySalesFromMaterializedView(ctx, tc.category, tc.daysInPast)
			assert.NoError(t, err)
			assert.NotNil(t, sales)
			assert.Equal(t, tc.category, sales.Category)
			assert.GreaterOrEqual(t, sales.TotalSold, 0)
		})
	}
}

func TestGetCategorySalesFromMaterializedView(t *testing.T) {
	ctx := context.Background()

	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	defer testcontainers.CleanupContainer(t, pgc)
	require.NoError(t, err)

	dbConnStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	assert.NotEmpty(t, dbConnStr)

	repo, err := pgviews.NewRepository(ctx, dbConnStr)
	require.NoError(t, err)

	category := "Electronics"
	daysInPast := 30

	// Capture query plan before benchmark
	query := `
		SELECT p.category, SUM(o.quantity) AS total_sold
		FROM orders o
		JOIN products p ON o.product_id = p.id
		WHERE p.category = $1 AND o.order_date >= CURRENT_DATE - $2::interval
		GROUP BY p.category;
	`
	params := map[string]interface{}{
		"category":   category,
		"daysInPast": fmt.Sprintf("%d days", daysInPast),
	}
	if err := captureQueryPlan(ctx, dbConnStr, query, params, "query_plan_view_materialized.json"); err != nil {
		t.Logf("Failed to capture query plan: %v", err)
	}

	tests := map[string]struct {
		daysInPast int
		category   string
	}{
		"Orders in the past 30 days": {daysInPast: 30, category: category},
		"Orders in the past 60 days": {daysInPast: 60, category: category},
		"Orders in the past 90 days": {daysInPast: 90, category: category},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err = repo.RefreshCategorySalesMaterializedView(ctx)
			assert.NoError(t, err)

			sales, err := repo.GetCategorySalesFromMaterializedView(ctx, tc.category, tc.daysInPast)
			assert.NoError(t, err)
			assert.NotNil(t, sales)
			assert.Equal(t, tc.category, sales.Category)
			assert.GreaterOrEqual(t, sales.TotalSold, 0)
		})
	}
}

func BenchmarkGetCategorySales(b *testing.B) {
	ctx := context.Background()

	// Setup test container
	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	defer testcontainers.CleanupContainer(b, pgc)
	if err != nil {
		b.Fatalf("Failed to start postgres container: %v", err)
	}

	dbConnStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("Failed to get connection string: %v", err)
	}

	repo, err := pgviews.NewRepository(ctx, dbConnStr)
	if err != nil {
		b.Fatalf("Failed to create repository: %v", err)
	}

	category := "Electronics"
	daysInPast := 30

	// Reset timer to exclude setup time
	b.ResetTimer()

	for b.Loop() {
		start := time.Now()
		_, err := repo.GetCategorySales(ctx, category, daysInPast)
		duration := time.Since(start)

		if err != nil {
			b.Fatalf("GetCategorySales failed: %v", err)
		}

		b.ReportMetric(float64(duration.Nanoseconds()), "ns/op")
	}
}

func BenchmarkGetCategorySalesFromView(b *testing.B) {
	ctx := context.Background()

	// Setup test container
	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	defer testcontainers.CleanupContainer(b, pgc)
	if err != nil {
		b.Fatalf("Failed to start postgres container: %v", err)
	}

	dbConnStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("Failed to get connection string: %v", err)
	}

	repo, err := pgviews.NewRepository(ctx, dbConnStr)
	if err != nil {
		b.Fatalf("Failed to create repository: %v", err)
	}

	category := "Electronics"
	daysInPast := 30

	// Reset timer to exclude setup time

	for b.Loop() {
		start := time.Now()
		_, err := repo.GetCategorySalesFromView(ctx, category, daysInPast)
		duration := time.Since(start)

		if err != nil {
			b.Fatalf("GetCategorySalesFromView failed: %v", err)
		}

		b.ReportMetric(float64(duration.Nanoseconds()), "ns/op")
	}
}

func BenchmarkGetCategorySalesFromMaterializedView(b *testing.B) {
	ctx := context.Background()

	// Setup test container
	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	defer testcontainers.CleanupContainer(b, pgc)
	if err != nil {
		b.Fatalf("Failed to start postgres container: %v", err)
	}

	dbConnStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("Failed to get connection string: %v", err)
	}

	repo, err := pgviews.NewRepository(ctx, dbConnStr)
	if err != nil {
		b.Fatalf("Failed to create repository: %v", err)
	}

	category := "Electronics"
	daysInPast := 30

	// Refresh materialized view before benchmark
	if err := repo.RefreshCategorySalesMaterializedView(ctx); err != nil {
		b.Fatalf("Failed to refresh materialized view: %v", err)
	}

	// Reset timer to exclude setup time

	for b.Loop() {
		start := time.Now()
		_, err := repo.GetCategorySalesFromMaterializedView(ctx, category, daysInPast)
		duration := time.Since(start)

		if err != nil {
			b.Fatalf("GetCategorySalesFromMaterializedView failed: %v", err)
		}

		b.ReportMetric(float64(duration.Nanoseconds()), "ns/op")
	}
}

// captureQueryPlan is a generic function to capture and save the query execution plan to a file
func captureQueryPlan(ctx context.Context, connStr string, query string, params map[string]any, outputFile string) error {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// Build the EXPLAIN query
	explainQuery := fmt.Sprintf("EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) %s", query)
	args := make([]any, 0, len(params))
	for _, v := range params {
		args = append(args, v)
	}

	// fmt.Println(explainQuery)

	var planJSON string
	if err := conn.QueryRow(ctx, explainQuery, args...).Scan(&planJSON); err != nil {
		return fmt.Errorf("failed to execute EXPLAIN: %w", err)
	}

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(planJSON)
	if err != nil {
		return fmt.Errorf("failed to write plan to file: %w", err)
	}

	return nil
}
