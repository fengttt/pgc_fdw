/* contrib/pgc_fdw/pgc_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgc_fdw" to load this file. \quit

CREATE FUNCTION pgc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pgc_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pgc_fdw
  HANDLER pgc_fdw_handler
  VALIDATOR pgc_fdw_validator;
