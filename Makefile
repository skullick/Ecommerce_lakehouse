
# Parameters
PYTHON = .venv/bin/python
GEN_SCRIPT = src/generator.py
AVG_QPS = 50
MAX_ITER = 500

# Help
.PHONY: help
help:
	@echo "E-Commerce Lakehouse Simulation Management"
	@echo "Usage:"
	@echo "  make infra-up          Start all Docker infrastructure"
	@echo "  make infra-down        Stop all Docker infrastructure"
	@echo "  make infra-status      Check status of Docker containers"
	@echo ""
	@echo "  make sim-init          Initialize database schema and seed reference data"
	@echo "  make sim-run           Run the e-commerce simulation (default QPS: $(AVG_QPS))"
	@echo "  make sim-demo          Run a short simulation for demo purposes"
	@echo ""
	@echo "  make cdc-register      Register the Debezium Postgres connector"
	@echo "  make cdc-status        Check the status of the Debezium connector"
	@echo "  make cdc-delete        Delete the Debezium connector"
	@echo "  make cdc-topics        List all Kafka topics"
	@echo ""
	@echo "  make db-shell          Open a psql shell into the Postgres database"
	@echo "  make db-stats          Show table counts in the 'demo' schema"
	@echo ""
	@echo "  make lakehouse-purge   Purge the Polaris catalog realm"
	@echo "  make lakehouse-clean   Wipe all Lakehouse data (MinIO & Polaris DB)"
	@echo "  make spark-ingest      Run the Spark CDC ingestion job"
	@echo ""
	@echo "  make clean             Remove temporary files and pyc"

# Infrastructure
infra-up:
	docker-compose up -d

infra-down:
	docker-compose down

infra-status:
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Simulation
sim-init:
	$(PYTHON) $(GEN_SCRIPT) --max-iter 0

sim-run:
	$(PYTHON) $(GEN_SCRIPT) --avg-qps $(AVG_QPS)

sim-demo:
	$(PYTHON) $(GEN_SCRIPT) --max-iter $(MAX_ITER) --avg-qps $(AVG_QPS)

# CDC
cdc-register:
	./register_connector.sh

cdc-status:
	@curl -s localhost:8083/connectors/ecommerce-postgres-connector/status | python3 -m json.tool || echo "Connector not found or Kafka Connect down."

cdc-delete:
	curl -X DELETE localhost:8083/connectors/ecommerce-postgres-connector

cdc-topics:
	docker exec lakehouse-kafka kafka-topics --list --bootstrap-server localhost:9092

# Utility to consume latest records from Kafka
cdc-peek-orders:
	docker exec lakehouse-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce.demo.order --max-messages 5 --from-beginning

cdc-peek-products:
	docker exec lakehouse-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce.demo.product --max-messages 5 --from-beginning

# Database
db-shell:
	docker exec -it lakehouse-postgres psql -U postgres -d default_db

db-stats:
	@echo "Table Row Counts (demo schema):"
	@for table in customer address product order order_detail transaction order_status_history; do \
		count=$$(docker exec -t lakehouse-postgres psql -U postgres -d default_db -t -c "SELECT count(*) FROM demo.$$table"); \
		printf "%-25s : %s\n" $$table $$count; \
	done

# Lakehouse Management
lakehouse-purge:
	@# Load REALM_NAME from .env if it exists, otherwise use default
	@REALM=$$(grep REALM_NAME .env | cut -d '=' -f2); \
	echo "Purging realm: $$REALM"; \
	docker exec -it polaris-admin-tool \
		java -jar /deployments/polaris-admin-tool.jar purge \
		-r $$REALM

lakehouse-clean:
	docker-compose down -v
	@echo "Wiped all named volumes (MinIO, Polaris Metadata, etc.)"

spark-ingest:
	docker exec -it spark-notebook spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/home/jovyan/work/ingest_cdc.py

# Cleanup and Teardown
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

infra-purge:
	docker-compose down -v
	rm -rf postgres-data/

cdc-clean: cdc-delete
	@echo "Deleting all ecommerce Kafka topics..."
	docker exec lakehouse-kafka bash -c "for topic in \$$(kafka-topics --list --bootstrap-server localhost:9092 | grep '^ecommerce\.'); do kafka-topics --delete --bootstrap-server localhost:9092 --topic \$$topic; done"

reset: infra-down infra-up
	@echo "Waiting for services to stabilize..."
	@sleep 10
	make sim-init
	make cdc-register
	@echo "System has been fully reset and re-initialized."

start-fresh: infra-purge infra-up
	@echo "Waiting for Kafka Connect to start (approx 30s)..."
	@sleep 30
	make cdc-register
	make sim-init
	@echo "Ready! Open localhost:8080 in browser, then run 'make sim-run' to start simulation."

cdc-watch-orders:
	docker exec lakehouse-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce.demo.order

cdc-watch-events:
	docker exec lakehouse-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce.events.clickstream

start: infra-up cdc-register sim-run

bootstrap-1:
	docker exec -it polaris-admin-tool \
		java -jar /deployments/polaris-admin-tool.jar bootstrap \
		-r realm_1 \
		-c realm_1,root,root

purge-1:
	docker exec -it polaris-admin-tool \
    	java -jar /deployments/polaris-admin-tool.jar purge \
		-r realm_1

bootstrap-2:
	docker exec -it polaris-admin-tool \
		java -jar /deployments/polaris-admin-tool.jar bootstrap \
		-r realm_2 \
		-c realm_2,root,root

purge-2:
	docker exec -it polaris-admin-tool \
    	java -jar /deployments/polaris-admin-tool.jar purge \
		-r realm_2
