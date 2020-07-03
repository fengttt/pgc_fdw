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
