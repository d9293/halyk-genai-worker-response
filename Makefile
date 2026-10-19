CONFIG ?= dev.env

.PHONY: run test mocks fmt vet check docker

IMAGE ?= halyk-genai-worker-response:local

run: ## Run the worker with $(CONFIG)
	go run . --config_file $(CONFIG)

docker: ## Build the Docker image $(IMAGE)
	docker build -t $(IMAGE) .

test: ## Run all tests with the race detector
	go test -race ./...

mocks: ## Regenerate moq mocks (moq is pinned in the go:generate lines)
	go generate ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

check: vet test
