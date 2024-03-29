.PHONY: test
test:
	go test -race -tags=assert ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: gen-spec-schema
gen-spec-schema:
	# vendor for gen
	go mod vendor
	go run schemagen/main.go

# All gen targets
.PHONY: gen
gen: gen-spec-schema
