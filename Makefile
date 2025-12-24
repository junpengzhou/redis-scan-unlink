.PHONY: build build-linux build-mac build-windows clean test run

BINARY_NAME = redis-scan-unlink
VERSION = 1.0.0

build:
	go build -o bin/$(BINARY_NAME) cmd/cli/main.go

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/$(BINARY_NAME)-linux cmd/cli/main.go

build-mac:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o bin/$(BINARY_NAME)-mac cmd/cli/main.go

build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/$(BINARY_NAME)-windows.exe cmd/cli/main.go

clean:
	rm -rf bin/

test:
	go test ./... -v

run: build
	./bin/$(BINARY_NAME) -pattern "user:*" -progress true

install: build
	sudo cp bin/$(BINARY_NAME) /usr/local/bin/

help:
	@echo "可用命令:"
	@echo "  make build        构建可执行文件"
	@echo "  make build-linux  构建 Linux 版本"
	@echo "  make build-mac    构建 Mac 版本"
	@echo "  make build-windows 构建 Windows 版本"
	@echo "  make clean        清理构建文件"
	@echo "  make test         运行测试"
	@echo "  make run          构建并运行"
	@echo "  make install      安装到系统"