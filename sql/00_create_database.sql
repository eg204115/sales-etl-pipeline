-- psql -U postgres -v etl_password=<password> -f sql/00_create_database.sql

\if :{?etl_password}
\else
    \echo 'ERROR: pass the password with  -v etl_password=...'
    \quit
\endif

CREATE ROLE etl_user WITH LOGIN PASSWORD :'etl_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;

CREATE DATABASE sales_dw OWNER etl_user;

\connect sales_dw

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT  USAGE, CREATE ON SCHEMA public TO etl_user;

CREATE ROLE analyst_ro NOLOGIN;
GRANT CONNECT ON DATABASE sales_dw TO analyst_ro;
GRANT USAGE ON SCHEMA public TO analyst_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA public GRANT SELECT ON TABLES TO analyst_ro;
