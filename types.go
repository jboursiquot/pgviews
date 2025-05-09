package pgviews

import (
	"time"
)

type Product struct {
	Id       int
	Name     string
	Category string
}

type Order struct {
	Id        int
	ProductId int
	Quantity  int
	OrderDate time.Time
}

type CategorySales struct {
	Category  string
	TotalSold int
}
