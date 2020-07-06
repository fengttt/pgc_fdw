/*-------------------------------------------------------------------------
 *
 * cache.c
 *		  FDB Cache for FDW.
 *-------------------------------------------------------------------------
 */
#ifndef PGC_FDW_CACHE_H
#define PGC_FDW_CACHE_H

#include <stdint.h>

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

const int32_t QRY_RUNNING = -1; 
const int32_t QRY_TOOBIG = -2;
const int32_t QRY_FAIL = -3;

typedef struct qry_key_t {
	char PREFIX[4];
	char SHA[20];
} qry_key_t;

typedef struct qry_val_t {
	int64_t ts;
	int32_t status;
	char qrytxt[1];
} qry_val_t;

typedef struct tup_key_t {
	char PREFIX[4]; 
	char SHA[20];
	int32_t seq;
} tup_key_t;


#endif /* PGC_FDW_CACHE_H */
