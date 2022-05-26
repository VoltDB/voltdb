/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

#include "storage/table.h"
#include "storage/tablefactory.h"
#include "common/TupleSchemaBuilder.h"

#include "test_utils/ScopedTupleSchema.hpp"
#include "common/debuglog.h"

#ifdef VOLT_TRACE_ENABLED
#define IF_VOLT_TRACE(x) x
#else
#define IF_VOLT_TRACE(x)
#endif

namespace voltdb {
/**
 * Load a table from a SerializeInput object.  We get the
 * schema from the input itself.  This is used only for testing.
 *
 * Note that the sizes of the columns must be predictable from the
 * column types.  No columns may have variable sized types.  This
 * includes array types.
 *
 * The caller owns the table object, and is responsible for
 * deleting it.
 */
TempTable *loadTableFrom(ReferenceSerializeInputBE& result, bool skipMsgHeader = false)
{
    // These variables are only used if
    // VOLT_TRACE is defined.  But their
    // values need to be calculated, because the
    // calculations have side effects.  If we
    // define the variables but don't use them the
    // compiler will complain.
    VOLT_TRACE("\n");
    if (! skipMsgHeader) {
        IF_VOLT_TRACE(int8_t  status           = ) result.readByte(); // status
        IF_VOLT_TRACE(int32_t drbuffer_size    = ) result.readInt();  // dr buffer length.
        IF_VOLT_TRACE(int32_t msg_size         = ) result.readInt();  // message length.

        VOLT_TRACE("  msg size:              %d\n",   msg_size);
        VOLT_TRACE("  drbuffer size:         %d\n",   drbuffer_size);
        VOLT_TRACE("  status:                %hhd\n", status);
    }
    IF_VOLT_TRACE(int32_t icl              = ) result.readInt();  // inter cluster latency
    IF_VOLT_TRACE(int32_t serialized_exp   = ) result.readInt();  // serialized exception
    IF_VOLT_TRACE(int32_t tbl_len          = ) result.readInt();  // table length
    IF_VOLT_TRACE(int32_t tbl_metadata_len = ) result.readInt();  // table metadata length
    IF_VOLT_TRACE(int8_t  tbl_status       = ) result.readByte();
    int16_t column_count = result.readShort();
    VOLT_TRACE("  inter cluster latency: %d\n",   icl);
    VOLT_TRACE("  serialized exception:  %d\n",   serialized_exp);
    VOLT_TRACE("  table length:          %d\n",   tbl_len);
    VOLT_TRACE("  table metadata length: %d\n",   tbl_metadata_len);
    VOLT_TRACE("  table_status:          %hhd\n", tbl_status);
    VOLT_TRACE("  column count:          %d\n",   column_count);
    /*
     * Read the schema information.
     */
    std::vector<std::string> columnNames;
    voltdb::TupleSchemaBuilder builder(column_count);
    for (int idx = 0; idx < column_count; idx += 1) {
        ValueType colType = static_cast<ValueType>(result.readByte());
        VOLT_TRACE("  column %02d type:         %hd\n",
                    idx,
                    column_count);
        assert(colType != ValueType::tARRAY);
        if (colType == ValueType::tVARCHAR
            || colType == ValueType::tVARBINARY) {
            // Note that in the tests we do not have the schema handy, setting this to 256
            // which seems to be large enough for the cpp unit tests now.
            builder.setColumnAtIndex(idx, colType, 256);
        }
        else {
            builder.setColumnAtIndex(idx, colType);
        }
    }
    TupleSchema *schema = builder.build();
    for (int idx = 0; idx < column_count; idx += 1) {
        columnNames.push_back(result.readTextString());
        VOLT_TRACE("  column %02d name:         %s\n",
                    idx,
                    columnNames[idx].c_str());
    }
    TempTable *table;
    table = TableFactory::buildTempTable("result",
                                       schema, // Transfers ownership to the table.
                                       columnNames,
                                       NULL);
    table->loadTuplesFromNoHeader(result, ExecutorContext::getTempStringPool());
    return table;
}

TempTable *loadTableFrom(const char *buffer, size_t size)
{
    ReferenceSerializeInputBE result(buffer, size);
    return loadTableFrom(result);
}

} // namespace voltdb

