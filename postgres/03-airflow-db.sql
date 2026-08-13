-- Separate physical database for Airflow's own metadata
-- (task state, DAG runs, etc.) — kept apart from the ecommerce
-- business schemas (raw / processed / analytics) on purpose.
CREATE DATABASE airflow_meta;
