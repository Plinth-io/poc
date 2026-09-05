.PHONY: generate test demo

generate:
	go tool buf generate

test:
	go test -race ./...

demo:
	@go build -o bin/ ./cmd/demo-service ./cmd/hub ./cmd/agent
	@echo "starting three processes, stop with Ctrl-C"
	@./bin/demo-service & \
	 HUB_TOKENS=mac-1:secret1 ./bin/hub & \
	 sleep 2; AGENT_TOKEN=secret1 ./bin/agent & \
	 wait
