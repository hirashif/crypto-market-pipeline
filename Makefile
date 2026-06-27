COMPOSE = docker compose -f deploy/docker-compose.yml

.PHONY: tidy fmt build test up down logs ps sample

tidy:
	go mod tidy

fmt:
	gofmt -w .

build:
	go build ./...

test:
	go test ./...

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=80

ps:
	$(COMPOSE) ps

sample:
	curl -s localhost:8080/prices | jq .
