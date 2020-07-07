#include "cache.h"

typedef struct cache_info_ctxt_t {
	TupleDesc tupdesc;
	fdb_error_t err;
	FDBTransaction *tr;
	FDBFuture *f;
	const FDBKeyValue *kv;
	int kvCnt;
	fdb_bool_t hasMore;
} cache_info_ctxt_t;

static fdb_error_t cache_info_cleanup(cache_info_ctxt_t *ctxt)
{
	fdb_error_t err = 0;

	if (ctxt->f) {
		fdb_future_destroy(ctxt->f);
		ctxt->f = 0;
	}

	if (ctxt->tr) {
		if (ctxt->err == 0) {
			FDBFuture *f = fdb_transaction_commit(ctxt->tr);
			err = fdb_wait_error(f);
			fdb_future_destroy(f);
		} else {
			err = ctxt->err;
		}

		fdb_transaction_destroy(ctxt->tr);
		ctxt->tr = 0;
	}
	return err;
}

#define CTXT_CHECK_ERR(ctxt, errcode)	\
	ctxt->err = (errcode);				\
	if (ctxt->err) {					\
		cache_info_cleanup(ctxt);		\
		elog(ERROR, "fdb error at %s:%d", __FILE__, __LINE__);	\
	} else (void) 0
	

PG_FUNCTION_INFO_V1(pgc_fdw_cache_info);
Datum pgc_fdw_cache_info(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctxt = 0;
	MemoryContext oldctxt;
	cache_info_ctxt_t *fnctxt;

	if (SRF_IS_FIRSTCALL()) {
		TupleDesc tupdesc;
		funcctxt = SRF_FIRSTCALL_INIT();

		oldctxt = MemoryContextSwitchTo(funcctxt->multi_call_memory_ctx);

		fnctxt = (cache_info_ctxt_t *) palloc0(sizeof(cache_info_ctxt_t));
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						    errmsg("function resturning record called in context that cannot accept record")
						));
		}
		fnctxt->tupdesc = BlessTupleDesc(tupdesc);

		// transaction stuff.
		CTXT_CHECK_ERR( fnctxt, fdb_database_create_transaction(get_fdb(), &fnctxt->tr)); 
		fnctxt->f = fdb_transaction_get_range(fnctxt->tr,
				(const uint8_t*) "PGCQ", 4, 1, 0,
				(const uint8_t*) "PGCZ", 4, 1, 0,
				0, 0,
				FDB_STREAMING_MODE_WANT_ALL, 1, 0, 0);
		CTXT_CHECK_ERR( fnctxt, fdb_wait_error(fnctxt->f));

		CTXT_CHECK_ERR( fnctxt, fdb_future_get_keyvalue_array(fnctxt->f, 
					&fnctxt->kv, &fnctxt->kvCnt, &fnctxt->hasMore));

		if (fnctxt->kvCnt > 0) {
			funcctxt->max_calls = fnctxt->kvCnt;
			funcctxt->user_fctx = fnctxt; 
		} else {
			// Fast path for empty results.
			MemoryContextSwitchTo(oldctxt);
			cache_info_cleanup(fnctxt);
			SRF_RETURN_DONE(funcctxt);
		}
		MemoryContextSwitchTo(oldctxt);
	}

	funcctxt = SRF_PERCALL_SETUP();
	fnctxt = funcctxt->user_fctx;
	if (funcctxt->call_cntr < funcctxt->max_calls) {
		Datum values[4];
		bool nulls[4];
		HeapTuple htup;
		Datum result;

		qry_key_t *qk = (qry_key_t *) fnctxt->kv[funcctxt->call_cntr].key;
		qry_val_t *qv = (qry_val_t *) fnctxt->kv[funcctxt->call_cntr].value; 

		char shahex[40];
		hex_encode(qk->SHA, 20, shahex);
		values[0] = (Datum) cstring_to_text_with_len(shahex, 40);
		nulls[0] = false;
		values[1] = qv->ts;
		nulls[1] = false;
		values[2] = Int32GetDatum(qv->status);
		nulls[2] = false;
		values[3] = (Datum) cstring_to_text_with_len(qv->qrytxt, qv->txtsz);
		nulls[3] = false;

		htup = heap_form_tuple(fnctxt->tupdesc, values, nulls);
		result = HeapTupleGetDatum(htup);

		SRF_RETURN_NEXT(funcctxt, result);
	}

	cache_info_cleanup(fnctxt);
	SRF_RETURN_DONE(funcctxt);
}

PG_FUNCTION_INFO_V1(pgc_fdw_set);
Datum pgc_fdw_set(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pgc_fdw_wait);
Datum pgc_fdw_wait(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pgc_fdw_invalidate);
Datum pgc_fdw_invalidate(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}
