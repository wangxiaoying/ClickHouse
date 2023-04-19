#pragma once

#include "config.h"

#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Core/ExternalResultDescription.h>
#include <Core/Field.h>
#include <Core/PostgreSQL/insertPostgreSQLValue.h>
#include <Core/PostgreSQL/ConnectionHolder.h>
#include <Core/PostgreSQL/Utils.h>

#include <arrow/api.h>
#include "connectorx_lib.hpp"


namespace DB
{

class ConnectorXSource : public ISource
{

public:
    ConnectorXSource(const Block sample_block, CXIterator * iter_);
    ~ConnectorXSource() override;

    String getName() const override { return "ConnectorX"; }

protected:
    CXIterator *iter;
    Block header;
    ArrowColumnToCHColumn arrow_to_ch;
    bool started = false;
    
    Status prepare() override;

    void onStart();
    Chunk generate() override;
    void onFinish();
};

}
