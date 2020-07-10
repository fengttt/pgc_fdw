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

#define PGC_STRINGIFY(x) #x
#define PGC_TOSTRING(x) PGC_STRINGIFY(x)
#define PGC_FLINE __FILE__ ":" PGC_TOSTRING(__LINE__) " " 

#define CHECK_ERR(err, msg, ...)					\
	if (err) {										\
		elog(ERROR, PGC_FLINE msg, ##__VA_ARGS__);	\
	} else (void) 0

#define CHECK_COND(cond, msg, ...)					\
	if (!(cond)) {									\
		elog(ERROR, PGC_FLINE msg, ##__VA_ARGS__);	\
	} else (void) 0

#define ERR_DONE(err, msg, ...)						\
	if (err) {										\
		elog(LOG, PGC_FLINE msg, ##__VA_ARGS__);	\
		goto done;									\
	} else (void) 0
		

static inline int64_t get_ts() {
	// current timestamp, as postgres timestamptz
	return GetCurrentTimestamp();
}

FDBDatabase *get_fdb(void);

static const int32_t QRY_FETCH = -1; 
static const int32_t QRY_FAIL = -2;
static const int32_t QRY_FAIL_NO_RETRY = -3;

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

static inline void qry_key_init(qry_key_t *k, const char *shastr) {
	memcpy(k->PREFIX, "PGCQ", 4);
	if (!shastr) {
		memset(k->SHA, 0, 20);
	} else {
		hex_decode(shastr, 40, k->SHA);
	}
}

static inline void qry_key_init_az(qry_key_t *k, int az) {
	memcpy(k->PREFIX, "PGCQ", 4);
	memset(k->SHA, az, 20);
}

#define QK_DUMP_SZ 46
static inline void qry_key_dump(const qry_key_t *k, char *buf) {
	memcpy(buf, k->PREFIX, 4);
	buf[4] = '-';
	hex_encode(k->SHA, 20, buf+5);
	buf[45] = 0;
}

static inline void qry_key_build(qry_key_t *k, const char* qry) {
	memcpy(k->PREFIX, "PGCQ", 4);
	SHA1((const unsigned char*) qry, strlen(qry), (unsigned char*) k->SHA);
}

static inline ssize_t qry_val_sz(int32_t txtsz) 
{
	return offsetof(qry_val_t, qrytxt) + txtsz + 1;
}

typedef struct tup_key_t {
	char PREFIX[4]; 
	char SHA[20];
	int32_t seq;
} tup_key_t;

static inline void tup_key_init(tup_key_t *k, const char *shastr, int seq) {
	memcpy(k->PREFIX, "TUPL", 4);
	if (!shastr) {
		memset(k->SHA, 0, 20);
	} else {
		hex_decode(shastr, 40, k->SHA);
	}
	k->seq = seq;
}

static inline void tup_key_initsha(tup_key_t *k, const char *sha, int seq) { 
	memcpy(k->PREFIX, "TUPL", 4);
	memcpy(k->SHA, sha, 20);
	k->seq = seq;
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
int32_t pgcache_get_status(const qry_key_t* qk, int64_t ts, int64_t *to, const char *data); 
int32_t pgcache_populate(const qry_key_t* qk, int64_t ts, int ntup, HeapTuple *tups); 
int32_t pgcache_retrieve(const qry_key_t* qk, int64_t ts, int *ntup, HeapTuple **tups); 



#endif /* PGC_FDW_CACHE_H */
