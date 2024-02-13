DOCKER_COMPOSE=docker-compose
NAME=product-channel-worker

.PHONY: all down
all: up
down: clean

.PHONY: clean
clean:
	-rm ${NAME}
	go clean -testcache

.env:
	cp .env.dist .env

.PHONY: dep
dep:
	go mod vendor

.PHONY: up
up: .env dep
	@echo
	@ echo ".. waiting five seconds for services to start"
	@echo
	@sleep 5

.PHONY: build
build: dep
	go build -mod vendor -race -o ${NAME} -v *.go

.PHONY: staticanalysis
staticanalysis:
	which staticcheck > /dev/null || go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck ./...

.PHONY: unittests
unittests:
	go clean -testcache
	go test -mod vendor -timeout 300s -race -v ./...

.PHONY: test
test: unittests staticanalysis

.PHONY: test-report
test-report:
	CGO_ENABLED=1 go install gotest.tools/gotestsum@latest
	CGO_ENABLED=1 gotestsum --junitfile report.xml --format testname -- -mod vendor -timeout 300s -coverprofile=cover.out -race ./...

.PHONY: cover
cover: test-report
	go tool cover -func=cover.out

.PHONY: cover-html
cover-html: test-report
	go tool cover -html=cover.out -o cover.html
