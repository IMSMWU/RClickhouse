// tools::package_native_routine_registration_skeleton(".")


#include <R.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL
#include <R_ext/Rdynload.h>

/* FIXME: 
   Check these declarations against the C/Fortran source code.
*/

/* .Call calls */
extern SEXP RClickhouse_clearResult(SEXP);
extern SEXP RClickhouse_connect(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP RClickhouse_disconnect(SEXP);
extern SEXP RClickhouse_fetch(SEXP, SEXP);
extern SEXP RClickhouse_getRowCount(SEXP);
extern SEXP RClickhouse_getRowsAffected(SEXP);
extern SEXP RClickhouse_getStatement(SEXP);
extern SEXP RClickhouse_hasCompleted(SEXP);
extern SEXP RClickhouse_insert(SEXP, SEXP, SEXP);
extern SEXP RClickhouse_RcppExport_registerCCallable();
extern SEXP RClickhouse_resultTypes(SEXP);
extern SEXP RClickhouse_select(SEXP, SEXP);
extern SEXP RClickhouse_validPtr(SEXP);

static const R_CallMethodDef CallEntries[] = {
    {"RClickhouse_clearResult",                  (DL_FUNC) &RClickhouse_clearResult,                  1},
    {"RClickhouse_connect",                      (DL_FUNC) &RClickhouse_connect,                      6},
    {"RClickhouse_disconnect",                   (DL_FUNC) &RClickhouse_disconnect,                   1},
    {"RClickhouse_fetch",                        (DL_FUNC) &RClickhouse_fetch,                        2},
    {"RClickhouse_getRowCount",                  (DL_FUNC) &RClickhouse_getRowCount,                  1},
    {"RClickhouse_getRowsAffected",              (DL_FUNC) &RClickhouse_getRowsAffected,              1},
    {"RClickhouse_getStatement",                 (DL_FUNC) &RClickhouse_getStatement,                 1},
    {"RClickhouse_hasCompleted",                 (DL_FUNC) &RClickhouse_hasCompleted,                 1},
    {"RClickhouse_insert",                       (DL_FUNC) &RClickhouse_insert,                       3},
    {"RClickhouse_RcppExport_registerCCallable", (DL_FUNC) &RClickhouse_RcppExport_registerCCallable, 0},
    {"RClickhouse_resultTypes",                  (DL_FUNC) &RClickhouse_resultTypes,                  1},
    {"RClickhouse_select",                       (DL_FUNC) &RClickhouse_select,                       2},
    {"RClickhouse_validPtr",                     (DL_FUNC) &RClickhouse_validPtr,                     1},
    {NULL, NULL, 0}
};

void R_init_RClickhouse(DllInfo *dll)
{
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}

