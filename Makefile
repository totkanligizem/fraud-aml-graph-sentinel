PYTHON := python3
GCP_KEY_PATH ?= .secrets/gcp-service-account.json
GCP_PROJECT_ID ?= fraud-aml-graph
BQ_DATASET ?= fraud_aml_graph_dev
BQ_LOCATION ?= EU

.PHONY: check-datasets ingest-smoke ingest-core ingest-ibm-1m warehouse-smoke warehouse-core train-fraud-smoke train-fraud train-fraud-benchmark train-fraud-tree score-fraud-smoke score-fraud-benchmark score-fraud-tree build-queue-smoke build-queue-benchmark build-queue-tree validate-state cleanup-incomplete-dry-run cleanup-incomplete-apply graph-build graph-validate setup-gcp-local bq-test sqlite-to-bq-smoke sqlite-to-bq-core sqlite-to-bq-full sqlite-graph-to-bq bq-create-analytics bq-create-graph-analytics bq-create-executive-views validate-executive-sql bq-validate-executive-views validate-analyst-sql agent-casebook agent-casebook-validate agent-prompt-pack agent-prompt-pack-validate agent-vertex-smoke agent-vertex-validate agent-vertex-batch agent-vertex-batch-validate vertex-to-bq bq-create-analyst-views bq-validate-analyst-views bq-analyst-check bq-run-validation-sql bq-validate-state bq-validate-graph-state bq-full-check bq-graph-check bq-refresh-from-local-full report-checkpoint dashboard-build dashboard-check report-briefing report-master report-master-en report-model-compare model-benchmark-pipeline tree-benchmark-pipeline sample-generate sample-warehouse sample-train sample-score sample-queue sample-graph sample-validate sample-e2e

check-datasets:
	./scripts/check_dataset_layout.sh

ingest-smoke:
	$(PYTHON) scripts/ingest_canonical.py --dataset all --max-rows 1000 --chunksize 50000

ingest-core:
	$(PYTHON) scripts/ingest_canonical.py --dataset ieee_cis --dataset creditcard_fraud --dataset paysim --chunksize 200000

ingest-ibm-1m:
	$(PYTHON) scripts/ingest_canonical.py --dataset ibm_aml_data --max-rows 1000000 --chunksize 100000

warehouse-smoke:
	$(PYTHON) scripts/build_sqlite_warehouse.py --datasets ieee_cis creditcard_fraud paysim ibm_aml_data --max-rows-per-dataset 300000 --chunksize 50000 --feature-base-mode per_dataset --feature-base-limit 50000 --recreate

warehouse-core:
	$(PYTHON) scripts/build_sqlite_warehouse.py --datasets ieee_cis creditcard_fraud paysim ibm_aml_data --max-rows-per-dataset 300000 --chunksize 100000 --feature-base-mode per_dataset --feature-base-limit 300000 --recreate

train-fraud-smoke:
	$(PYTHON) scripts/train_fraud_baseline_numpy.py --max-rows-per-dataset 50000 --sample-fraction 0.4 --epochs 3 --batch-size 32768 --learning-rate 0.05 --split-mode per_dataset_time

train-fraud:
	$(PYTHON) scripts/train_fraud_baseline_numpy.py --max-rows-per-dataset 300000 --sample-fraction 1.0 --epochs 8 --batch-size 65536 --learning-rate 0.05 --split-mode per_dataset_time

train-fraud-benchmark:
	$(PYTHON) scripts/train_fraud_benchmark_numpy.py --max-rows-per-dataset 300000 --sample-fraction 1.0 --epochs 10 --batch-size 65536 --learning-rate 0.05 --split-mode per_dataset_time

train-fraud-tree:
	$(PYTHON) scripts/train_fraud_tree_benchmark.py --max-rows-per-dataset 300000 --sample-fraction 1.0 --split-mode per_dataset_time

score-fraud-smoke:
	$(PYTHON) scripts/score_fraud_baseline_numpy.py --model-path artifacts/models/fraud_baseline/latest/model.npz --max-rows-per-dataset 50000 --chunksize 50000

score-fraud-benchmark:
	$(PYTHON) scripts/score_fraud_benchmark_numpy.py --model-path artifacts/models/fraud_benchmark_numpy/latest/model.npz --max-rows-per-dataset 300000 --chunksize 100000 --destination-table fraud_scores_benchmark

score-fraud-tree:
	$(PYTHON) scripts/score_fraud_tree_benchmark.py --model-path artifacts/models/fraud_tree_benchmark/latest/model.pkl --max-rows-per-dataset 300000 --chunksize 100000 --destination-table fraud_scores_tree

build-queue-smoke:
	$(PYTHON) scripts/build_investigation_queue.py --top-k 50

build-queue-benchmark:
	$(PYTHON) scripts/build_investigation_queue.py --scores-table fraud_scores_benchmark --queue-table alert_queue_benchmark --score-column fraud_score --top-k 50 --output-root artifacts/models/ranking_benchmark

build-queue-tree:
	$(PYTHON) scripts/build_investigation_queue.py --scores-table fraud_scores_tree --queue-table alert_queue_tree --score-column fraud_score --top-k 50 --output-root artifacts/models/ranking_tree

validate-state:
	$(PYTHON) scripts/validate_pipeline_state.py --min-feature-coverage-over-payer 0.8 --min-graph-feature-coverage-over-payer 0.8

graph-build:
	$(PYTHON) scripts/build_graph_layer.py

graph-validate:
	$(PYTHON) scripts/validate_graph_state.py

cleanup-incomplete-dry-run:
	$(PYTHON) scripts/cleanup_incomplete_runs.py

cleanup-incomplete-apply:
	$(PYTHON) scripts/cleanup_incomplete_runs.py --apply

setup-gcp-local:
	bash scripts/setup_gcp_local.sh "$(GCP_KEY_PATH)" "$(GCP_PROJECT_ID)" "$(BQ_DATASET)" "$(BQ_LOCATION)"

bq-test:
	$(PYTHON) scripts/bigquery_test_connection.py

sqlite-to-bq-smoke:
	$(PYTHON) scripts/sqlite_to_bigquery.py --tables transaction_mart feature_payer_24h feature_graph_24h fraud_scores alert_queue --max-rows-per-table 50000 --chunksize 10000 --table-prefix dev_

sqlite-to-bq-core:
	$(PYTHON) scripts/sqlite_to_bigquery.py --tables transaction_mart feature_payer_24h feature_graph_24h monitoring_mart fraud_scores alert_queue --max-rows-per-table 300000 --chunksize 25000 --table-prefix dev_

sqlite-to-bq-full:
	$(PYTHON) scripts/sqlite_to_bigquery.py --tables transaction_mart feature_payer_24h feature_graph_24h monitoring_mart fraud_scores alert_queue --max-rows-per-table -1 --chunksize 25000 --table-prefix dev_

sqlite-graph-to-bq:
	$(PYTHON) scripts/sqlite_to_bigquery.py --tables graph_party_node graph_party_edge graph_account_node graph_account_edge graph_party_cluster_membership graph_party_cluster_summary --max-rows-per-table -1 --chunksize 25000 --table-prefix dev_

bq-create-analytics:
	$(PYTHON) scripts/run_bigquery_sql_bundle.py --sql-path sql/bigquery/analytics

bq-create-graph-analytics:
	$(PYTHON) scripts/run_bigquery_sql_bundle.py --sql-path sql/bigquery/graph_analytics

bq-create-executive-views:
	$(PYTHON) scripts/run_bigquery_sql_bundle.py --sql-path sql/bigquery/executive_views

validate-executive-sql:
	$(PYTHON) scripts/validate_executive_sql_bundle.py

bq-validate-executive-views:
	$(PYTHON) scripts/run_bigquery_sql_bundle.py --sql-path sql/bigquery/executive_validation --write-select-results

validate-analyst-sql:
	$(PYTHON) scripts/validate_analyst_sql_bundle.py

agent-casebook:
	$(PYTHON) scripts/build_analyst_casebook.py

agent-casebook-validate: agent-casebook
	$(PYTHON) scripts/validate_analyst_casebook.py

agent-prompt-pack: agent-casebook
	$(PYTHON) scripts/build_analyst_prompt_pack.py

agent-prompt-pack-validate: agent-prompt-pack
	$(PYTHON) scripts/validate_analyst_prompt_pack.py

agent-vertex-smoke: agent-prompt-pack
	$(PYTHON) scripts/run_vertex_analyst_copilot.py --max-prompts 1 --model gemini-2.5-flash --location europe-west4 --credentials-path "api keys/fraud-aml-graph-.json" --skip-latest-on-error

agent-vertex-validate: agent-vertex-smoke
	$(PYTHON) scripts/validate_vertex_analyst_outputs.py --output-dir artifacts/agent/vertex_responses/last

agent-vertex-batch: agent-prompt-pack
	$(PYTHON) scripts/run_vertex_analyst_copilot.py --max-prompts 3 --selection-strategy round_robin_dataset --request-delay-seconds 8 --model gemini-2.5-flash --fallback-model gemini-2.5-pro --location europe-west4 --credentials-path "api keys/fraud-aml-graph-.json" --skip-latest-on-error

agent-vertex-batch-validate: agent-vertex-batch
	$(PYTHON) scripts/validate_vertex_analyst_outputs.py --output-dir artifacts/agent/vertex_responses/last --min-response-count 3 --min-dataset-count 3

vertex-to-bq:
	$(PYTHON) scripts/vertex_outputs_to_bigquery.py

bq-create-analyst-views:
	$(PYTHON) scripts/run_bigquery_sql_bundle.py --sql-path sql/bigquery/analyst_views

bq-validate-analyst-views:
	$(PYTHON) scripts/run_bigquery_sql_bundle.py --sql-path sql/bigquery/analyst_validation --write-select-results

bq-analyst-check: validate-analyst-sql vertex-to-bq bq-create-analyst-views bq-validate-analyst-views

bq-run-validation-sql:
	$(PYTHON) scripts/run_bigquery_sql_bundle.py --sql-path sql/bigquery/validation --write-select-results

bq-validate-state:
	$(PYTHON) scripts/validate_bigquery_state.py

bq-validate-graph-state:
	$(PYTHON) scripts/validate_bigquery_state.py --require-graph

bq-full-check: bq-test bq-create-analytics bq-validate-state bq-run-validation-sql

bq-graph-check: bq-test bq-validate-graph-state bq-create-graph-analytics

bq-refresh-from-local-full: sqlite-to-bq-full bq-full-check

report-checkpoint:
	$(PYTHON) scripts/generate_checkpoint_reports.py

dashboard-build: report-checkpoint
	$(PYTHON) scripts/build_dashboard_bundle.py

dashboard-check: dashboard-build
	$(PYTHON) scripts/validate_dashboard_bundle.py

report-briefing: dashboard-check
	$(PYTHON) scripts/generate_project_briefing_report.py

report-master: report-briefing
	$(PYTHON) scripts/generate_master_final_report.py

report-master-en: report-master
	$(PYTHON) scripts/generate_master_final_report_en.py

report-model-compare:
	$(PYTHON) scripts/generate_model_comparison_report.py

model-benchmark-pipeline: train-fraud-benchmark score-fraud-benchmark build-queue-benchmark report-model-compare

tree-benchmark-pipeline: train-fraud-tree score-fraud-tree build-queue-tree report-model-compare

sample-generate:
	$(PYTHON) scripts/generate_synthetic_sample_data.py --output-root data/sample/transaction_event --rows-per-dataset 2500

sample-warehouse: sample-generate
	$(PYTHON) scripts/build_sqlite_warehouse.py --canonical-root data/sample/transaction_event --db-path data/sample/warehouse/ledger_sentinel_sample.db --datasets ieee_cis creditcard_fraud paysim ibm_aml_data --max-rows-per-dataset 2500 --chunksize 2500 --feature-base-mode per_dataset --feature-base-limit 2500 --recreate

sample-train: sample-warehouse
	$(PYTHON) scripts/train_fraud_baseline_numpy.py --db-path data/sample/warehouse/ledger_sentinel_sample.db --max-rows-per-dataset 2000 --epochs 3 --batch-size 2048 --learning-rate 0.05 --split-mode per_dataset_time --output-root artifacts/models/fraud_baseline_sample

sample-score: sample-train
	$(PYTHON) scripts/score_fraud_baseline_numpy.py --db-path data/sample/warehouse/ledger_sentinel_sample.db --model-path artifacts/models/fraud_baseline_sample/latest/model.npz --max-rows-per-dataset 2500 --chunksize 2500 --output-root artifacts/models/fraud_scoring_sample

sample-queue: sample-score
	$(PYTHON) scripts/build_investigation_queue.py --db-path data/sample/warehouse/ledger_sentinel_sample.db --top-k 25 --output-root artifacts/models/ranking_sample

sample-graph: sample-queue
	$(PYTHON) scripts/build_graph_layer.py --db-path data/sample/warehouse/ledger_sentinel_sample.db

sample-validate: sample-graph
	$(PYTHON) scripts/validate_pipeline_state.py --db-path data/sample/warehouse/ledger_sentinel_sample.db --canonical-root data/sample/transaction_event --min-feature-coverage-over-payer 0.8 --min-graph-feature-coverage-over-payer 0.8
	$(PYTHON) scripts/validate_graph_state.py --db-path data/sample/warehouse/ledger_sentinel_sample.db

sample-e2e: sample-validate
