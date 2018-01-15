// tools::package_native_routine_registration_skeleton(".")


#include <R.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL
#include <R_ext/Rdynload.h>

/* FIXME:
   Check these declarations against the C/Fortran source code.
*/

/* .Call calls */
extern SEXP _RClickhouse_clearResult(SEXP);
extern SEXP _RClickhouse_connect(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP _RClickhouse_disconnect(SEXP);
extern SEXP _RClickhouse_fetch(SEXP, SEXP);
extern SEXP _RClickhouse_getRowCount(SEXP);
extern SEXP _RClickhouse_getRowsAffected(SEXP);
extern SEXP _RClickhouse_getStatement(SEXP);
extern SEXP _RClickhouse_hasCompleted(SEXP);
extern SEXP _RClickhouse_insert(SEXP, SEXP, SEXP);
extern SEXP _RClickhouse_RcppExport_registerCCallable();
extern SEXP _RClickhouse_resultTypes(SEXP);
extern SEXP _RClickhouse_select(SEXP, SEXP);
extern SEXP _RClickhouse_validPtr(SEXP);

static const R_CallMethodDef CallEntries[] = {
    {"_RClickhouse_clearResult",                  (DL_FUNC) &_RClickhouse_clearResult,                  1},
    {"_RClickhouse_connect",                      (DL_FUNC) &_RClickhouse_connect,                      6},
    {"_RClickhouse_disconnect",                   (DL_FUNC) &_RClickhouse_disconnect,                   1},
    {"_RClickhouse_fetch",                        (DL_FUNC) &_RClickhouse_fetch,                        2},
    {"_RClickhouse_getRowCount",                  (DL_FUNC) &_RClickhouse_getRowCount,                  1},
    {"_RClickhouse_getRowsAffected",              (DL_FUNC) &_RClickhouse_getRowsAffected,              1},
    {"_RClickhouse_getStatement",                 (DL_FUNC) &_RClickhouse_getStatement,                 1},
    {"_RClickhouse_hasCompleted",                 (DL_FUNC) &_RClickhouse_hasCompleted,                 1},
    {"_RClickhouse_insert",                       (DL_FUNC) &_RClickhouse_insert,                       3},
    {"_RClickhouse_RcppExport_registerCCallable", (DL_FUNC) &_RClickhouse_RcppExport_registerCCallable, 0},
    {"_RClickhouse_resultTypes",                  (DL_FUNC) &_RClickhouse_resultTypes,                  1},
    {"_RClickhouse_select",                       (DL_FUNC) &_RClickhouse_select,                       2},
    {"_RClickhouse_validPtr",                     (DL_FUNC) &_RClickhouse_validPtr,                     1},
    {NULL, NULL, 0}
};

void R_init_RClickhouse(DllInfo *dll)
{
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}

