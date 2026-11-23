CREATE DATABASE airflow;
CREATE DATABASE superset;
CREATE DATABASE analytics;
\connect analytics
CREATE SCHEMA demo;
CREATE SCHEMA gold;
CREATE ROLE dashboard_reader LOGIN PASSWORD :'reader_password';
GRANT CONNECT ON DATABASE analytics TO dashboard_reader;
GRANT USAGE ON SCHEMA demo, gold TO dashboard_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE il05 IN SCHEMA demo GRANT SELECT ON TABLES TO dashboard_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE il05 IN SCHEMA gold GRANT SELECT ON TABLES TO dashboard_reader;
