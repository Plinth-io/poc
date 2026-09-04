.PHONY: generate test demo

generate:
	go tool buf generate

test:
	go test -race ./...

demo:
	@echo "drei Prozesse starten, mit Ctrl-C beenden"
	@go run ./cmd/demo-service & \
	 HUB_TOKENS=mac-1:secret1 go run ./cmd/hub & \
	 sleep 2; AGENT_TOKEN=secret1 go run ./cmd/agent & \
	 wait
