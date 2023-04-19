#include "ConnectorXSource.h"
#include <Common/logger_useful.h>
#include <arrow/record_batch.h>
#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/c/bridge.h>

namespace DB
{
ConnectorXSource::ConnectorXSource(const Block sample_block, CXIterator *iter_)
: ISource(sample_block.cloneEmpty())
, iter(iter_)
, header(sample_block)
, arrow_to_ch(header, "ConnectorX", false, false, true) {}

ConnectorXSource::~ConnectorXSource() {
    LOG_DEBUG(&Poco::Logger::get("ConnectorXSource"), "free iterator");
    free_iter(iter);
}

IProcessor::Status ConnectorXSource::prepare()
{
    if (!started)
    {
        onStart();
        started = true;
    }

    auto status = ISource::prepare();
    if (status == Status::Finished)
        onFinish();

    return status;
}

void ConnectorXSource::onStart() {
    connectorx_prepare(iter);
}

Chunk ConnectorXSource::generate() {
    // currently we only create a single Pipe, so don't need lock when fetch next batch
    Chunk chunk;

    CXSlice<CXArray> *rb = connectorx_iter_next(iter);
    if (nullptr == rb) return chunk;

    ArrowColumnToCHColumn::NameToColumnPtr name_to_column;

    int64_t num_rows = -1;
    for (int i = 0; static_cast<size_t>(i) < rb->len; ++i) {
        auto array_result = arrow::ImportArray(rb->ptr[i].array, rb->ptr[i].schema);
        REQUIRE_RESULT(auto array, array_result);
        if (num_rows < 0) {
            num_rows = array->length();
        }
        name_to_column[header.getByPosition(i).name] = std::make_shared<arrow::ChunkedArray>(array);
    }
    free_record_batch(rb);
    
    arrow_to_ch.arrowColumnsToCHChunk(chunk, name_to_column, num_rows);
    return chunk;
}

void ConnectorXSource::onFinish() {  }
}
