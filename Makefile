MAKEFLAGS += --no-print-directory

.PHONY: proto build test test-verbose test-pkg test-e2e vet tidy tools

GOBIN := $(shell go env GOPATH)/bin

# PKG is the package selector for test-pkg. Defaults to ./... when
# unset, matching the full-suite `test` target.
PKG ?= ./...

proto:
	PATH="$(GOBIN):$$PATH" buf generate

build:
	go build ./...

# tools installs the dev binaries the targets below assume on PATH.
# Currently just gotestsum, which gives compact one-line-per-package
# output on PASS and shows only the failing tests' output on FAIL.
tools:
	@test -x $(GOBIN)/gotestsum || { \
		echo "installing gotestsum..."; \
		go install gotest.tools/gotestsum@latest; \
	}

# test runs the full unit + integration suite with -race. On success
# each package prints one line; on failure only the failed tests'
# output surfaces, ending with a `DONE N tests, X failures` summary.
test: tools
	$(GOBIN)/gotestsum --format pkgname-and-test-fails -- -race ./...

# test-verbose is the escape hatch for when you actually want every
# log line (e.g. debugging a flaky test). Same as the old `make test`.
test-verbose:
	go test -race -v ./...

# test-pkg scopes the run to one package or subtree. Usage:
#   make test-pkg PKG=./internal/auth/...
#   make test-pkg PKG=./internal/engine/ RUN=TestSoloBootstrap
RUN ?=
test-pkg: tools
	$(GOBIN)/gotestsum --format pkgname-and-test-fails -- -race $(if $(RUN),-run $(RUN),) $(PKG)

# test-e2e runs the containerized chaos / eventsource / kms / snapshot
# suites under internal/e2e/... behind the `e2e` build tag. Requires a
# working Docker daemon; individual tests Skip when Docker is absent.
# Override the reflowd image with REFLOW_E2E_IMAGE=... (CI prebuild) to
# skip the in-test image build.
test-e2e:
	go test -tags=e2e -timeout=30m ./internal/e2e/...

vet:
	go vet ./...

tidy:
	go mod tidy
