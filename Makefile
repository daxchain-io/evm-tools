# Developer convenience targets. The release build is driven by goreleaser; these
# wrap the common local commands documented in CLAUDE.md.
.PHONY: build test lint integration tidy

build: ## Build all binaries and packages.
	go build ./...

test: ## Unit tests (offline; golden + httptest), race-enabled.
	go test -race ./...

lint: ## gofmt check + go vet + golangci-lint.
	gofmt -l . | (! grep .) && go vet ./... && golangci-lint run

tidy: ## gofmt + go mod tidy (must be clean).
	gofmt -w . && go mod tidy

integration: ## Bring up the compose stack, run the build-tagged live tests, tear down.
	./scripts/integration.sh
