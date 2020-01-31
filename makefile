version=0.1.0

.PHONY: all

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  build         - build the source code"
	@echo "  lint          - lint the source code"
	@echo "  test          - test the source code"
	@echo "  fmt           - format the source code"
	@echo "  install       - install vendored dependencies"

lint:
	@go vet ./...
	@go list ./... | grep -v /vendor/ | xargs -L1 golint

test:
	@env GO111MODULE=on go test ./...

fmt:
	@env GO111MODULE=on go fmt ./...

build: lint
	@env GO111MODULE=on go build ./...

install:
	@go get -u golang.org/x/lint/golint
	@env GO111MODULE=on go mod vendor
