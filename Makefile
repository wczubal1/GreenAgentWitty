SHELL := /bin/bash

GREEN_HOST ?= 127.0.0.1
GREEN_PORT ?= 9009
GREEN_URL ?= http://$(GREEN_HOST):$(GREEN_PORT)
PURPLE_URL ?= http://127.0.0.1:9010
TARGET_MONTH ?= 5
SAMPLE_SIZE ?= 10
HTTP_TIMEOUT ?= 180

.PHONY: run
run:
	uv run src/server.py --host $(GREEN_HOST) --port $(GREEN_PORT)

.PHONY: send
send:
	@test -n "$$FINRA_CLIENT_ID" || (echo "Set FINRA_CLIENT_ID" >&2; exit 1)
	@test -n "$$FINRA_CLIENT_SECRET" || (echo "Set FINRA_CLIENT_SECRET" >&2; exit 1)
	python send_assessment.py \
		--green-url $(GREEN_URL) \
		--purple-url $(PURPLE_URL) \
		--target-month $(TARGET_MONTH) \
		--sample-size $(SAMPLE_SIZE) \
		--http-timeout $(HTTP_TIMEOUT) \
		--finra-client-id "$$FINRA_CLIENT_ID" \
		--finra-client-secret "$$FINRA_CLIENT_SECRET"

.PHONY: docker-build
docker-build:
	docker build -t green-agent .

.PHONY: docker-run
docker-run:
	docker run -p $(GREEN_PORT):9009 green-agent --host 0.0.0.0 --port 9009
