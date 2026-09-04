.PHONY: generate test demo

generate:
	go tool buf generate

test:
	go test -race ./...
