.PHONY: help test bench bench_view_none bench_view_plain bench_view_materialized bench_all

default: help

help: ## show help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[$$()% a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

test: ## run tests
	go test -v -race ./...

bench_view_none: ## run benchmark for GetCategorySales
	go test -benchmem -count=10 -run=^$$ -bench=^BenchmarkGetCategorySales$$ ./... > bench_view_none.txt

bench_view_plain: ## run benchmark for GetCategorySalesFromView
	go test -benchmem -count=10 -run=^$$ -bench=^BenchmarkGetCategorySalesFromView$$ ./... > bench_view_plain.txt

bench_view_materialized: ## run benchmark for GetCategorySalesFromMaterializedView
	go test -benchmem -count=10 -run=^$$ -bench=^BenchmarkGetCategorySalesFromMaterializedView$$ ./... > bench_view_materialized.txt

bench_all: bench_view_none bench_view_plain bench_view_materialized ## run all benchmarks and compare results
	benchstat bench_view_none.txt bench_view_plain.txt bench_view_materialized.txt