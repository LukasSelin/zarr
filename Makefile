# Everything CI runs, runnable here. MODULES is the three modules of this
# repository; each is built, tested and linted on its own.
MODULES := . s3 zstd

GOLANGCI_VERSION := v2.13.2
CONFIG := $(abspath .golangci.yml)
FUZZTIME ?= 30s
# Tracked and new-but-not-yet-committed alike, ignored files left out.
GOFILES = $(shell git ls-files --cached --others --exclude-standard '*.go')

.PHONY: all check test race lint vuln tidy tidy-check fmt fmt-check fuzz tools clean

## check: what CI checks, in the order it fails fastest
check: fmt-check lint test vuln

## test: every module's tests, with the race detector and goleak
test race:
	@for m in $(MODULES); do \
	    echo "==> test $$m"; \
	    (cd $$m && go test -race -count=1 ./...) || exit 1; \
	done

## lint: golangci-lint over every module, with the config at the root
lint:
	@for m in $(MODULES); do \
	    echo "==> lint $$m"; \
	    (cd $$m && golangci-lint run --config $(CONFIG) ./...) || exit 1; \
	done

## vuln: govulncheck over every module. It reports the standard library too,
## so an out-of-date toolchain shows up here rather than in production.
vuln:
	@for m in $(MODULES); do \
	    echo "==> govulncheck $$m"; \
	    (cd $$m && go run golang.org/x/vuln/cmd/govulncheck@latest ./...) || exit 1; \
	done

## tidy: go mod tidy every module
tidy:
	@for m in $(MODULES); do \
	    echo "==> tidy $$m"; \
	    (cd $$m && go mod tidy) || exit 1; \
	done

## tidy-check: fail if tidy would change anything. It wants a clean tree, so
## it is for CI; here, run `make tidy` and read what it did.
tidy-check: tidy
	@git diff --exit-code -- '*go.mod' '*go.sum' \
	    || { echo "go.mod or go.sum was not tidy: commit the change above"; exit 1; }

## fmt: gofmt every Go file in place
fmt:
	@out=$$(gofmt -l -w $(GOFILES)); \
	if [ -n "$$out" ]; then echo "rewrote:"; echo "$$out"; else echo "already formatted"; fi

## fmt-check: fail if anything is not gofmt'd, without writing to the tree
fmt-check:
	@out=$$(gofmt -l $(GOFILES)); \
	if [ -n "$$out" ]; then echo "not gofmt'd:"; echo "$$out"; exit 1; fi; \
	echo "gofmt: clean"

## fuzz: run each fuzz target for FUZZTIME (default 30s)
fuzz:
	@for t in FuzzMetadata FuzzOpenAndRead FuzzBytesCodec FuzzGzipCodec \
	          FuzzCRC32CCodec FuzzShard FuzzShardIndex; do \
	    echo "==> fuzz $$t"; \
	    go test -run '^$$' -fuzz "^$$t\$$" -fuzztime $(FUZZTIME) . || exit 1; \
	done
	@echo "==> fuzz FuzzDecode (zstd)"
	@cd zstd && go test -run '^$$' -fuzz '^FuzzDecode$$' -fuzztime $(FUZZTIME) .

## tools: install the linters this repository uses
tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@latest

## clean: remove test binaries and the fuzz cache
clean:
	go clean -testcache -fuzzcache
	rm -f *.test *.test.exe

all: check
