#include "StorageConnectorX.h"

#include <DataTypes/DataTypeString.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/logger_useful.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/ConnectorXSource.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

#include <arrow/c/bridge.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageConnectorX::~StorageConnectorX() {
    if (iter != nullptr) {
        LOG_DEBUG(&Poco::Logger::get("StorageConnectorX"), "free iterator");
        free_iter(iter);
    }
}

StorageConnectorX::StorageConnectorX(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const String & conn_,
    const std::vector<String> &queries_,
    CXIterator *iter_)
    : IStorage(table_id_)
    , conn(conn_)
    , queries(queries_)
    , iter(iter_)
    , log(&Poco::Logger::get("StorageConnectorX (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageConnectorX::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names_);

    Block sample_block;
    for (const String & column_name : storage_snapshot->metadata->getColumns().getNamesOfPhysical())
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        WhichDataType which(column_data.type);
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }

    if (iter == nullptr) {
         // create iterator
        std::vector<const char*> tmp;
        for (auto &query: queries) {
            tmp.push_back(query.c_str()); 
        }
        auto cx_queries = CXSlice<const char*> {&tmp[0], tmp.size(), tmp.capacity()};
        iter = connectorx_scan_iter(conn.c_str(), &cx_queries, 32767);
    }

    auto res =  Pipe(std::make_shared<ConnectorXSource>(std::move(sample_block), iter));
    iter = nullptr;

    return res;
}


StorageConnectorX::Configuration StorageConnectorX::processNamedCollectionResult(const NamedCollection & named_collection)
{
    StorageConnectorX::Configuration configuration;
    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"conn", "queries"};

    validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(
        named_collection, required_arguments, {});

    configuration.conn = named_collection.get<String>("conn");
    String query = named_collection.get<String>("query");
    size_t last_pos = 0;
    while (true) {
        size_t pos = query.find(';', last_pos);
        std::string sql = query.substr(last_pos, pos-last_pos);
        if (sql.empty()) {
            break;
        }
        configuration.queries.push_back(sql);
        if (pos == std::string::npos) {
            break;
        }
        last_pos = pos+1;
    }
    return configuration;
}

StorageConnectorX::Configuration StorageConnectorX::getConfiguration(ASTs engine_args, ContextPtr context)
{
    StorageConnectorX::Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        configuration = StorageConnectorX::processNamedCollectionResult(*named_collection);
    }
    else
    {
        if (engine_args.size() != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage ConnectorX requires 2 parameters: "
                "ConnectorX('conn', 'queries'."
                "Got: {}",
                engine_args.size());
        }

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        configuration.conn = checkAndGetLiteralArgument<String>(engine_args[0], "conn");
        String query = checkAndGetLiteralArgument<String>(engine_args[1], "queries");
        size_t last_pos = 0;
        while (true)
        {
            size_t pos = query.find(';', last_pos);
            std::string sql = query.substr(last_pos, pos - last_pos);
            if (sql.empty())
            {
                break;
            }
            configuration.queries.push_back(sql);
            if (pos == std::string::npos)
            {
                break;
            }
            last_pos = pos + 1;
        }
    }
    return configuration;
}


void registerStorageConnectorX(StorageFactory & factory)
{
    factory.registerStorage("ConnectorX", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StorageConnectorX::getConfiguration(args.engine_args, args.getLocalContext());
        return std::make_shared<StorageConnectorX>(
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            configuration.conn,
            configuration.queries,
            nullptr);
    },
    {
        .source_access_type = AccessType::CONNECTORX,
    });
}

}

