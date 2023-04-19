#pragma once

#include <cstddef>
#include <arrow/c/abi.h>

#define REQUIRE(V) assert(V)

#define REQUIRE_RESULT(OUT, IN)                                                                                        \
	REQUIRE(IN.ok());                                                                                                  \
	OUT = IN.ValueUnsafe()

struct CXArray {
    ArrowArray *array;
    ArrowSchema *schema;
};

template<typename T>
struct CXSlice {
    T *ptr;
    size_t len;
    size_t capacity;
};

struct CXTable {
    const char *name;
    CXSlice<const char *> columns;
};

struct CXResult {
    CXSlice<CXSlice<CXArray>> data;
    CXSlice<char *> header;
};

typedef struct CXIterator CXIterator;

struct CXSchema {
    CXSlice<CXArray> types;
    CXSlice<char *> headers;
};

struct CXConnectionInfo {
    const char *name;
    const char *conn;
    CXSlice<CXTable> schema;
    bool is_local;
};

struct CXFederatedPlan {
    char *db_name;
    char *db_alias;
    char *sql;
    size_t cardinality;
};

extern "C" {
    extern CXResult connectorx_scan(const char *conn, const char *query);
    extern void free_result(CXResult *);

    extern CXSlice<CXFederatedPlan> connectorx_rewrite(const CXSlice<CXConnectionInfo> *conn_list, const char *query);
    extern void free_plans(CXSlice<CXFederatedPlan> *);

    extern CXIterator *connectorx_scan_iter(const char *conn, CXSlice<const char*> *queries, size_t batch_size);
    extern CXSchema *connectorx_get_schema(CXIterator *);
    extern CXSchema *connectorx_prepare(CXIterator *);
    extern CXSlice<CXArray> *connectorx_iter_next(CXIterator *);
    extern void free_iter(CXIterator *);
    extern void free_schema(CXSchema *);
    extern void free_record_batch(CXSlice<CXArray> *);

    extern void connectorx_set_thread_num(size_t num); // can be called exactly once
}
