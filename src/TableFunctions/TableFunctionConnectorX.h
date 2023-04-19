#pragma once
#include "config.h"


#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageConnectorX.h>

namespace DB
{

class TableFunctionConnectorX : public ITableFunction
{
public:
    static constexpr auto name = "connectorx";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "ConnectorX"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::optional<StorageConnectorX::Configuration> configuration;
    CXIterator *iter;
    std::shared_ptr<arrow::Schema> schema;
};

}
