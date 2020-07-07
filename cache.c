/*-------------------------------------------------------------------------
 *
 * cache.c
 *		  FDB Cache for FDW.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "pgc_fdw.h"
#include "utils/builtins.h"
#include "utils/varlena.h"

#include "cache.h"

#define FDB_API_VERSION 620
#include "foundationdb/fdb_c.h"
#include "foundationdb/fdb_c_options.g.h"

#include <pthread.h>

static FDBDatabase *fdb;
static pthread_t pth;

static void* runNetwork() {
	fdb_run_network();
	return NULL;
}

void pgcache_init()
{
	CHECK_COND(fdb == 0, "Reinit foundation db");
	CHECK_ERR(fdb_setup_network(), "Cannot setup fdb network.");
	CHECK_ERR(pthread_create(&pth, NULL, &runNetwork, NULL), "Cannot create fdb network thread");
	CHECK_ERR(fdb_create_database(NULL, &fdb), "Cannot create fdb");
}

void pgcache_fini()
{
	CHECK_ERR(fdb_stop_network(), "Cannot stop fdb network.");
	CHECK_ERR(pthread_join(pth, NULL), "Cannot join fdb network thread");
	fdb = 0;
}
