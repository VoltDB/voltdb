/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
#ifndef TESTS_EE_TEST_UTILS_LOADTABLEFROM_HPP_
#define TESTS_EE_TEST_UTILS_LOADTABLEFROM_HPP_

#include "storage/table.h"
#include "storage/tablefactory.h"
#include "common/TupleSchemaBuilder.h"

#include "test_utils/ScopedTupleSchema.hpp"

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
bool debug_print = false;
TempTable *loadTableFrom(const char *buffer,
                     size_t size,
                     Pool * pool= NULL,
                     ReferenceSerializeOutput *uniqueViolationOutput = NULL,
                     bool shouldDRStreamRows = false)
{
    ReferenceSerializeInputBE result(buffer, size);
    int32_t msg_size         = result.readInt();  // message length.
    int8_t  status           = result.readByte(); // status
    int32_t icl              = result.readInt();  // inter cluster latency
    int32_t serialized_exp   = result.readInt();  // serialized exception
    int32_t tbl_len          = result.readInt();  // table length
    int32_t tbl_metadata_len = result.readInt();  // table metadata length
    int8_t  tbl_status       = result.readByte();
    int16_t column_count     = result.readShort();
    if (debug_print) {
        printf("\n");
        printf("  msg size:              %d\n",   msg_size);
        printf("  status:                %hhd\n", status);
        printf("  inter cluster latency: %d\n",   icl);
        printf("  serialized exception:  %d\n",   serialized_exp);
        printf("  table length:          %d\n",   tbl_len);
        printf("  table metadata length: %d\n",   tbl_metadata_len);
        printf("  table_status:          %hhd\n", tbl_status);
        printf("  column count:          %d\n",   column_count);
    }
    /*
     * Read the schema information.
     */
    std::vector<string> columnNames;
    voltdb::TupleSchemaBuilder builder(column_count);
    for (int idx = 0; idx < column_count; idx += 1) {
        ValueType colType = static_cast<ValueType>(result.readByte());
        if (debug_print) {
            printf("  column %02d type:         %hd\n",
                   idx,
                   column_count);
        }
        assert(colType != VALUE_TYPE_ARRAY);
        builder.setColumnAtIndex(idx, colType);
    }
    TupleSchema *schema = builder.build();
    for (int idx = 0; idx < column_count; idx += 1) {
        columnNames.push_back(result.readTextString());
        if (debug_print) {
            printf("  column %02d name:         %s\n",
                   idx,
                   columnNames[idx].c_str());
        }
    }
    TempTable *table;
    table = TableFactory::getTempTable(0,
                                       "result",
                                       schema, // Transfers ownership to the table.
                                       columnNames,
                                       NULL);
    table->loadTuplesFromNoHeader(result);
    return table;
}
}

#endif /* TESTS_EE_TEST_UTILS_LOADTABLEFROM_HPP_ */
