CONFIG ?= dev.env

.PHONY: run test mocks fmt vet check docker sonar

IMAGE ?= halyk-genai-worker-response:local

run: ## Run the worker with $(CONFIG)
	go run . --config_file $(CONFIG)

docker: ## Build the Docker image $(IMAGE)
	docker build -t $(IMAGE) .

test: ## Run all tests with the race detector
	go test -race ./...

SONAR_URL ?= http://sonarqube:9000
# Scanner token; by default from the local infra's .env. Exported, never echoed.
SONAR_TOKEN ?= $(shell sed -n 's/^SONAR_TOKEN=//p' $(HOME)/Documents/work/projects/infra/.env 2>/dev/null)
export SONAR_TOKEN
SCANNER := sonarsource/sonar-scanner-cli:12.2.0.4256_8.1.0

sonar: ## Tests with coverage, then analysis on the local SonarQube (infra)
	@mkdir -p .sonar
	go test -race -count=1 -coverpkg=./... -coverprofile=.sonar/coverage.out -json ./... > .sonar/tests.json
	docker run --rm --network infra -e SONAR_HOST_URL=$(SONAR_URL) -e SONAR_TOKEN \
		-v "$(CURDIR):/usr/src" $(SCANNER)

mocks: ## Regenerate moq mocks (moq is pinned in the go:generate lines)
	go generate ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

check: vet test
