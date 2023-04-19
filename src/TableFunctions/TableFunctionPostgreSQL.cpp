#include <TableFunctions/TableFunctionPostgreSQL.h>

#if USE_LIBPQXX
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include "registerTableFunctions.h"
#include <Common/parseRemoteDescription.h>

#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include "connectorx_lib.hpp"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = configuration->query.size() > 0 ? getTableSchemaFromConnectorX() : getActualTableStructure(context);
    auto result = std::make_shared<StoragePostgreSQL>(
        StorageID(getDatabaseName(), table_name),
        connection_pool,
        configuration->table,
        columns,
        ConstraintsDescription{},
        String{},
        configuration->schema,
        configuration->on_conflict,
        configuration->query);

    result->startup();
    return result;
}

ColumnsDescription TableFunctionPostgreSQL::getTableSchemaFromConnectorX() const
{
    std::string conn = "postgresql://postgres:postgres@10.155.96.80:5432/tpch";
    std::vector<const char*> queries;
    queries.push_back(configuration->query.c_str());

    auto cx_queries = CXSlice<const char*> {&queries[0], queries.size(), queries.capacity()};
    
    CXIterator *iter = connectorx_scan_iter(conn.c_str(), &cx_queries, 32768);
    CXSchema *schema = connectorx_get_schema(iter);

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t c = 0; c < schema->types.len; ++c) {
        auto type_result = arrow::ImportType(schema->types.ptr[c].schema);
        REQUIRE_RESULT(auto type, type_result);
        auto field_name = std::string(schema->headers.ptr[c]);
        fields.push_back(arrow::field(field_name, type));
    }
    std::shared_ptr<arrow::Schema> arrow_schema = arrow::schema(fields);
    Block header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*arrow_schema, "arrow");
    LOG_DEBUG(&Poco::Logger::get("TableFunctionPostgreSQL"), "header: {}", header.dumpNames());

    free_schema(schema);
    free_iter(iter);

    return ColumnsDescription{header.getNamesAndTypesList()};
}


ColumnsDescription TableFunctionPostgreSQL::getActualTableStructure(ContextPtr context) const
{
    const bool use_nulls = context->getSettingsRef().external_table_functions_use_nulls;
    auto connection_holder = connection_pool->get();
    auto columns_info = fetchPostgreSQLTableStructure(
            connection_holder->get(), configuration->table, configuration->schema, use_nulls).physical_columns;

    if (!columns_info)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table structure not returned");

    return ColumnsDescription{columns_info->columns};
}


void TableFunctionPostgreSQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'PostgreSQL' must have arguments.");

    configuration.emplace(StoragePostgreSQL::getConfiguration(func_args.arguments->children, context));
    const auto & settings = context->getSettingsRef();
    connection_pool = std::make_shared<postgres::PoolWithFailover>(
        *configuration,
        settings.postgresql_connection_pool_size,
        settings.postgresql_connection_pool_wait_timeout,
        POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
        settings.postgresql_connection_pool_auto_close_connection);
}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>();
}

}

#endif
