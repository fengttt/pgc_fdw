/*-------------------------------------------------------------------------
 *
 * cache.c
 *		  FDB Cache for FDW.
 *-------------------------------------------------------------------------
 */
#include "cache.h"
#include <pthread.h>

static bool fdb_inited;
static FDBDatabase *fdb;
static pthread_t pth;

FDBDatabase *get_fdb() {
	return fdb;
}

static void* runNetwork() {
	fdb_run_network();
	return NULL;
}

void pgcache_init()
{
	if (!fdb_inited) {
		CHECK_COND( fdb == 0, "Reinit foundation db");
		CHECK_ERR( fdb_select_api_version(FDB_API_VERSION), "Canot select fdb api version");
		CHECK_ERR( fdb_setup_network(), "Cannot setup fdb network.");
		fdb_inited = true;
	}

	CHECK_ERR( pthread_create(&pth, NULL, &runNetwork, NULL), "Cannot create fdb network thread");
	CHECK_ERR( fdb_create_database(NULL, &fdb), "Cannot create fdb");
}

void pgcache_fini()
{
	CHECK_ERR(fdb_stop_network(), "Cannot stop fdb network.");
	CHECK_ERR(pthread_join(pth, NULL), "Cannot join fdb network thread");
	fdb = 0;
}
