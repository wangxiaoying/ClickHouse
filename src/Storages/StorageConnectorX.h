#pragma once

#include "config.h"
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include <arrow/api.h>
#include "connectorx_lib.hpp"

namespace Poco
{
class Logger;
}

namespace DB
{
class NamedCollection;

class StorageConnectorX final : public IStorage
{
public:
    StorageConnectorX(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const String & conn = "",
        const std::vector<String> & queries = {},
        CXIterator *iter = nullptr);
    
    ~StorageConnectorX() override;

    String getName() const override { return "ConnectorX"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    struct Configuration
    {
        String conn;
        std::vector<String> queries;
    };

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context);

    static Configuration processNamedCollectionResult(const NamedCollection & named_collection);

private:
    String conn;
    std::vector<String> queries;
    CXIterator *iter;
    Poco::Logger * log;
};

}
