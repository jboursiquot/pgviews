package pgviews

import (
	"context"
	"fmt"
	"os"

	pgx "github.com/jackc/pgx/v5"
)

type Repository struct {
	conn *pgx.Conn
}

func NewRepository(ctx context.Context, connStr string) (*Repository, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		return nil, err
	}
	return &Repository{conn: conn}, nil
}

func (r Repository) CreateProduct(ctx context.Context, product Product) (Product, error) {
	err := r.conn.QueryRow(ctx,
		"INSERT INTO products (name, category) VALUES ($1, $2) RETURNING id",
		product.Name, product.Category).Scan(&product.Id)
	return product, err
}

func (r Repository) GetProductsByCategory(ctx context.Context, category string) ([]Product, error) {
	var products []Product
	query := "SELECT id, name, category FROM products WHERE category = $1"
	rows, err := r.conn.Query(ctx, query, category)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var product Product
		if err := rows.Scan(&product.Id, &product.Name, &product.Category); err != nil {
			return nil, err
		}
		products = append(products, product)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return products, nil
}

func (r Repository) GetCategorySales(ctx context.Context, category string, daysInPast int) (CategorySales, error) {
	query := `
    SELECT p.category, SUM(o.quantity) AS total_sold
    FROM orders o
    JOIN products p ON o.product_id = p.id
    WHERE p.category = $1 AND o.order_date >= CURRENT_DATE - $2::interval
    GROUP BY p.category;
    `
	// Convert daysInPast to PostgreSQL interval syntax
	interval := fmt.Sprintf("%d days", daysInPast)

	var categorySales CategorySales
	err := r.conn.QueryRow(ctx, query, category, interval).Scan(&categorySales.Category, &categorySales.TotalSold)
	if err != nil {
		return CategorySales{}, err
	}

	return categorySales, nil
}

// GetCategorySalesFromView uses a regular SQL view
func (r Repository) GetCategorySalesFromView(ctx context.Context, category string, daysInPast int) (CategorySales, error) {
	query := `
    SELECT category, SUM(daily_total_sold) AS total_sold
    FROM category_sales_view
    WHERE category = $1 AND order_date >= CURRENT_DATE - $2::interval
    GROUP BY category;
    `
	// Convert daysInPast to PostgreSQL interval syntax
	interval := fmt.Sprintf("%d days", daysInPast)

	var categorySales CategorySales
	err := r.conn.QueryRow(ctx, query, category, interval).Scan(&categorySales.Category, &categorySales.TotalSold)
	if err != nil {
		return CategorySales{}, err
	}

	return categorySales, nil
}

// GetCategorySalesFromMaterializedView uses the materialized view for improved performance
func (r Repository) GetCategorySalesFromMaterializedView(ctx context.Context, category string, daysInPast int) (CategorySales, error) {
	query := `
    SELECT category, SUM(daily_total_sold) AS total_sold
    FROM category_sales_materialized_view
    WHERE category = $1 AND order_date >= CURRENT_DATE - $2::interval
    GROUP BY category;
    `
	// Convert daysInPast to PostgreSQL interval syntax
	interval := fmt.Sprintf("%d days", daysInPast)

	var categorySales CategorySales
	err := r.conn.QueryRow(ctx, query, category, interval).Scan(&categorySales.Category, &categorySales.TotalSold)
	if err != nil {
		return CategorySales{}, err
	}

	return categorySales, nil
}

// RefreshCategorySalesMaterializedView refreshes the materialized view to update with latest data
func (r Repository) RefreshCategorySalesMaterializedView(ctx context.Context) error {
	_, err := r.conn.Exec(ctx, "REFRESH MATERIALIZED VIEW category_sales_materialized_view")
	return err
}
