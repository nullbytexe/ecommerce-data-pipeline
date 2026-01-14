---Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

---Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;
CREATE SCHEMA IF NOT EXISTS analytics;

---Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA processed TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO dataeng;
