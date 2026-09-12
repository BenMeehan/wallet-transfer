.PHONY: run down test build burst

run:
	docker compose up --build

down:
	docker compose down

test:
	TEST_DATABASE_URL="postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable" go test ./... -count=1

build:
	docker build -t paytm-wallet .

burst:
	python3 scripts/burst.py
