#include <TableFunctions/TableFunctionConnectorX.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
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

StoragePtr TableFunctionConnectorX::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{    
    auto columns = getActualTableStructure(context);
    auto result = std::make_shared<StorageConnectorX>(
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        configuration->conn,
        configuration->queries,
        iter);
    result->startup();
    return result;
}

ColumnsDescription TableFunctionConnectorX::getActualTableStructure(ContextPtr /*context*/) const
{
    Block header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "arrow");
    LOG_DEBUG(&Poco::Logger::get("TableFunctionConnectorX"), "header: {}", header.dumpNames());

    return ColumnsDescription{header.getNamesAndTypesList()};
}


void TableFunctionConnectorX::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'ConnectorX' must have arguments.");

    configuration.emplace(StorageConnectorX::getConfiguration(func_args.arguments->children, context));
    LOG_DEBUG(&Poco::Logger::get("TableFunctionConnectorX"), "got config, conn: {} with {} queries:", configuration->conn, configuration->queries.size());
    for (auto &query: configuration->queries) {
        LOG_DEBUG(&Poco::Logger::get("TableFunctionConnectorX"), "{}", query);
    }

    // create iterator
    std::vector<const char*> queries;
    for (auto &query: configuration->queries) {
        queries.push_back(query.c_str()); 
    }
    auto cx_queries = CXSlice<const char*> {&queries[0], queries.size(), queries.capacity()};
    
    iter = connectorx_scan_iter(configuration->conn.c_str(), &cx_queries, 32767);
    CXSchema *cx_schema = connectorx_get_schema(iter);

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t c = 0; c < cx_schema->types.len; ++c) {
        auto type_result = arrow::ImportType(cx_schema->types.ptr[c].schema);
        REQUIRE_RESULT(auto type, type_result);
        auto field_name = std::string(cx_schema->headers.ptr[c]);
        fields.push_back(arrow::field(field_name, type));
    }
    schema = arrow::schema(fields);
    LOG_DEBUG(&Poco::Logger::get("TableFunctionConnectorX"), "got iterator and schema");

    Block header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "arrow");
    LOG_DEBUG(&Poco::Logger::get("TableFunctionConnectorX"), "header: {}", header.dumpNames());

    free_schema(cx_schema);
}


void registerTableFunctionConnectorX(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionConnectorX>();
}

}
