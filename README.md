# pgviews
This project demonstrates the use of PostgreSQL views for efficient data retrieval.

## Dependencies
- [Docker](https://www.docker.com/)
- [Testcontainers](https://www.testcontainers.org/) | See [Getting Started for Go](https://testcontainers.com/guides/getting-started-with-testcontainers-for-go/) 
- [PostgreSQL module](https://golang.testcontainers.org/modules/postgres/) for testcontainers
- [Benchstat](https://golang.org/x/perf/cmd/benchstat)

## Running the Project

There's a `Makefile` in the root directory that contains commands to run the project. You can use the following command to list them out:

```bash
make help
```

## Running the Tests

To run the tests and ensure you have the de, use the following command:

```bash
make test
```

The first time you run the tests, it may take some time to download the PostgreSQL image and start the container. Subsequent runs will be faster as the container will already be running.

## Running the Benchmarks

To run the benchmarks, use the following command:

```bash
make bench_all
```

This will run all the benchmarks and output the results to files that can be compared using `benchstat`.

## Query Plans

When the tests are run, query plans are generated for each query. These plans are stored locally in files with names starting with `query_plan_`. You can use a tool like https://explain.dalibo.com/ to visualize the query plans.

## General Findings
- The use of views can significantly improve query performance, especially for complex queries that involve multiple joins and aggregations.
- Materialized views can provide even better performance for read-heavy workloads, but they require additional maintenance to keep them up-to-date.