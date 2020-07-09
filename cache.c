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

int32_t pgcache_get_status(const qry_key_t *qk, int64_t ts, int64_t *to, const char* qstr) 
{
	FDBTransaction *tr = 0;
	FDBFuture *f = 0;
	fdb_error_t err;
	int32_t ret = QRY_FAIL;

	fdb_bool_t found;
	const qry_val_t *qvbuf = 0;
	qry_val_t *qv = 0;
	int qvsz;
	int qstrsz;
	
	/* TODO: Magic 10 */
	for (int i = 0; i < 10; i++) {
		ERR_DONE( fdb_database_create_transaction(get_fdb(), &tr), "cannot begin fdb transaction");
		ERR_DONE( fdb_transaction_get(tr, (const uint8_t *) qk, sizeof(qry_key_t), 0), "fdb get failed");
		ERR_DONE( fdb_wait_error(f), "fdb future failed");
		ERR_DONE( fdb_future_get_value(f, &found, (const uint8_t **) &qvbuf, &qvsz), "fdb get value failed");

		/* If not found, or, qv is very old, we add a new entry to fetch remote ... */
		if (!found || qvbuf->ts + *to < ts) {
			fdb_future_destroy(f);
			f = 0;
			qstrsz = strlen(qstr); 
			qvsz = qry_val_sz(qstrsz); 
			qv = (qry_val_t *) palloc(qvsz);
			qv->ts = ts;
			qv->status = QRY_FETCH;
			qv->txtsz = qstrsz;
			memcpy(qv->qrytxt, qstr, qstrsz); 
			qv->qrytxt[qstrsz] = 0;

			fdb_transaction_set(tr, (const uint8_t *) qk, sizeof(qry_key_t), (const uint8_t *) qv, qvsz);
			f = fdb_transaction_commit(tr);
			err = fdb_wait_error(f);
			pfree(qv);
			qv = 0;
			if (!err) {
				ret = QRY_FETCH;
				*to = ts;
			}
		} else if (qv->status >= 0) {
			ret = qv->status;
			*to = qv->ts;
		} else {
			fdb_future_destroy(f);
			f = fdb_transaction_watch(tr, (const uint8_t *) qk, sizeof(qry_key_t)); 
			fdb_wait_error(f);
		}

done:
		if (qv) {
			pfree(qv);
			qv = 0;
		}

		if (f) {
			fdb_future_destroy(f);
			f = 0;
		}

		if (tr) {
			fdb_transaction_destroy(tr);
			tr = 0;
		}
		if (ret != QRY_FAIL) {
			return ret;
		}
	}

	elog(LOG, "pgcache_get_status time out ...");
	return QRY_FAIL;
}

int32_t pgcache_retrieve(const qry_key_t *qk, int64_t ts, int *ntup, HeapTuple **tups)
{
	FDBTransaction *tr = 0;
	FDBFuture *f = 0;
	int32_t ret = QRY_FAIL;

	fdb_bool_t found;
	qry_val_t *qv = 0;
	int qvsz;

	tup_key_t ka;
	tup_key_t kz;
	const FDBKeyValue *outkv;
	int kvcnt;

	ERR_DONE( fdb_database_create_transaction(get_fdb(), &tr), "cannot begin fdb transaction");
	ERR_DONE( fdb_transaction_get(tr, (const uint8_t *) qk, sizeof(qry_key_t), 0), "fdb get failed");
	ERR_DONE( fdb_wait_error(f), "fdb future failed");
	ERR_DONE( fdb_future_get_value(f, &found, (const uint8_t **) &qv, &qvsz), "fdb get value failed");
	ERR_DONE( !found || qv->ts != ts, "qry key not found");
	ERR_DONE( qv->status < 0, "qry race.");

	*ntup = qv->status;
	*tups = (HeapTuple *) palloc0(*ntup * sizeof(HeapTuple));
	
	tup_key_initsha(&ka, qk->SHA, 0);
	tup_key_initsha(&kz, qk->SHA, -1); 

	fdb_future_destroy(f);
	f = fdb_transaction_get_range(tr, 
			(const uint8_t *)&ka, sizeof(tup_key_t), 1, 0,
			(const uint8_t *)&kz, sizeof(tup_key_t), 1, 0,
			0, 0, FDB_STREAMING_MODE_WANT_ALL, 1, 0, 0);
	ERR_DONE( fdb_wait_error(f), "get range failed");
	ERR_DONE( fdb_future_get_keyvalue_array(f, &outkv, &kvcnt, &found), "retrieve kv array failed.");
	ERR_DONE( kvcnt != *ntup, "kvcount mismatch");

	for (int i = 0; i < kvcnt; i++) {
		*tups[i] = (HeapTuple) palloc(outkv[i].value_length);
		memcpy(*tups[i], outkv[i].value, outkv[i].value_length);
	}
	ret = *ntup;

done:
	if (f) {
		fdb_future_destroy(f);
		f = 0;
	}

	if (tr) {
		fdb_transaction_destroy(tr);
		tr = 0;
	}

	return ret;
}

int32_t pgcache_populate(const qry_key_t *qk, int64_t ts, int ntup, HeapTuple *tups)
{
	FDBTransaction *tr = 0;
	FDBFuture *f = 0;
	int32_t ret = QRY_FAIL;
	fdb_error_t err;

	fdb_bool_t found;
	qry_val_t *qv = 0;
	uint8_t *qvbuf;
	int qvsz;

	tup_key_t ka;
	tup_key_t kz;

	for (int i = 0; i < 10; i++) {
		ERR_DONE( fdb_database_create_transaction(get_fdb(), &tr), "cannot begin fdb transaction");
		ERR_DONE( fdb_transaction_get(tr, (const uint8_t *) qk, sizeof(qry_key_t), 0), "fdb get failed");
		ERR_DONE( fdb_wait_error(f), "fdb future failed");
		ERR_DONE( fdb_future_get_value(f, &found, (const uint8_t **) &qvbuf, &qvsz), "fdb get value failed");
		if (!found) {
			ret = QRY_FAIL_NO_RETRY;
			goto done;
		}

		qv = (qry_val_t *) palloc(qvsz);
		memcpy(qv, qvbuf, qvsz);
		if (qv->ts > ts) {
			ret = QRY_FAIL_NO_RETRY;
			goto done;
		}
		fdb_future_destroy(f);
		f = 0;

		tup_key_initsha(&ka, qk->SHA, 0);
		tup_key_initsha(&kz, qk->SHA, -1); 

		fdb_transaction_clear_range(tr, (const uint8_t *) &ka, sizeof(ka), 
				(const uint8_t *) &kz, sizeof(kz));

		/* Now put all tuples in */
		for (int i = 0; i < ntup; i++) {
			ka.seq = i;
			fdb_transaction_set(tr, 
					(const uint8_t *) &ka, sizeof(ka), 
					(const uint8_t *) tups[i], HEAPTUPLESIZE + tups[i]->t_len);
		}

		/* Finally update meta */
		qv->status = ntup;
		fdb_transaction_set(tr, (const uint8_t *) qk, sizeof(qry_key_t),
				(const uint8_t *) qv, qvsz);

		f = fdb_transaction_commit(tr);
		err = fdb_wait_error(f);
		pfree(qv);
		ERR_DONE(err, "cache populate transaction error.");
		ret = ntup;

done:
		if (f) {
			fdb_future_destroy(f);
			f = 0;
		}

		if (tr) {
			fdb_transaction_destroy(tr);
			tr = 0;
		}
		if (ret != QRY_FAIL) {
			return ret;
		}
	}

	return QRY_FAIL;
}

