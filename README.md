A Caching Postgresql fdw
========================

Shamelessly copied everything from postgres fdw, added a poorman's query
cache for remote data. Most code is just copy pasted from postgres fdw, renamed 
to pgc fdw so that user can use/load two extensions at the same time.

Build
========================

Requirement
-----------
1. Install all requirements need to build postgres.   configure, make, make install
2. Install foundationdb server and client

Build
-------
The easist way to build fdw is just put this dir in postgresql/contrib and
run make.

Usage
========================

We will use foundationdb and default cluster conf file.   Usage is the same as postgres 
fdw, except an additional option in create foreign table. cache time is seconds, default 
value is 3600.    Set it to 0 to disable caching.

```
CREATE FOREIGN TABLE foreign_table (
		id int NOT NULL,
		data text
) SERVER foreign_server OPTIONS(shcema_name 'foo', table_name 'bar', cache_time = 3600);
```


