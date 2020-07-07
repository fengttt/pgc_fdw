/*-------------------------------------------------------------------------
 *
 * cache.c
 *		  FDB Cache for FDW.
 *-------------------------------------------------------------------------
 */
#ifndef PGC_FDW_CACHE_H
#define PGC_FDW_CACHE_H

#include "postgres.h"
#include "access/htup_details.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "funcapi.h"
#include "miscadmin.h"

#include <stdint.h>
#include <openssl/sha.h>

#define FDB_API_VERSION 620
#include "foundationdb/fdb_c.h"
#include "foundationdb/fdb_c_options.g.h"

#define UNUSED(x) (void)(x)

#define CHECK_ERR(err, msg, ...)			\
	if (err) {								\
		elog(ERROR, msg, ##__VA_ARGS__);	\
	} else (void) 0

#define CHECK_COND(cond, msg, ...)			\
	if (!(cond)) {							\
		elog(ERROR, msg, ##__VA_ARGS__);	\
	} else (void) 0


static inline int64_t get_ts() {
	// current timestamp, as postgres timestamptz
	return GetCurrentTimestamp();
}

FDBDatabase *get_fdb(void);

static const int32_t QRY_RUNNING = -1; 
static const int32_t QRY_TOOBIG = -2;
static const int32_t QRY_FAIL = -3;

typedef struct qry_key_t {
	char PREFIX[4];
	char SHA[20];
} qry_key_t;

typedef struct qry_val_t {
	int64_t ts;
	int32_t status;
	int32_t txtsz;
	char qrytxt[1];
} qry_val_t;

static inline void qry_key_init(qry_key_t *k, const char *sha) {
	memcpy(k->PREFIX, "PGCQ", 4);
	if (sha) {
		memcpy(k->SHA, sha, 20);
	} else {
		memset(k->SHA, 0, 20);
	}
}

static inline void qry_key_build(qry_key_t *k, const char* qry) {
	memcpy(k->PREFIX, "PGCQ", 4);
	SHA1((const unsigned char*) qry, strlen(qry), (unsigned char*) k->SHA);
}

static inline ssize_t qry_val_sz(int32_t txtsz) 
{
	return offsetof(qry_val_t, qrytxt) + txtsz + 1;
}

#define QRY_VAL_LOCAL(local, ts, status, txt, txtsz)			\
	ssize_t _qry_val_sz_of_ ## local = qry_val_sz(txtsz);		\
	char _qry_val_buf_of_ ## local [_qry_val_sz_of_ ## local];	\
	qry_val_t *local = (qry_val_t *) _qry_val_buf_of_ ## local; \
	local->ts = ts;												\
	local->status = status;										\
	local->txtsz = txtsz;										\
	memcpy(local->qrytxt, txt, txtsz);							\
	local->qrytxt[txtsz] = 0;									\

typedef struct tup_key_t {
	char PREFIX[4]; 
	char SHA[20];
	int32_t seq;
} tup_key_t;

static inline void tup_key_init(tup_key_t *k, const char *sha) {
	memcpy(k->PREFIX, "TUPL", 4);
	memcpy(k->SHA, sha, 20);
	k->seq = 0;
}

static inline fdb_error_t fdb_wait_error(FDBFuture *f) {
	fdb_error_t blkErr = fdb_future_block_until_ready(f);
	if (!blkErr) {
		return fdb_future_get_error(f);
	} else {
		return blkErr;
	}
}

void pgcache_init(void);
void pgcache_fini(void);



#endif /* PGC_FDW_CACHE_H */
