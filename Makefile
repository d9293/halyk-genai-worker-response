CONFIG ?= dev.env

.PHONY: run test mocks fmt vet check

run: ## Run the worker with $(CONFIG)
	go run . --config_file $(CONFIG)

test: ## Run all tests with the race detector
	go test -race ./...

mocks: ## Regenerate moq mocks (moq is pinned in the go:generate lines)
	go generate ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

check: vet test
