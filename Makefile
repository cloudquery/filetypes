.PHONY: test
test:
	go test -race -tags=assert ./...

.PHONY: lint
lint:
	golangci-lint run
