CONFIG ?= dev.env

.PHONY: run test fmt vet check

run: ## Run the worker with $(CONFIG)
	go run . --config_file $(CONFIG)

test: ## Run all tests with the race detector
	go test -race ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

check: vet test
