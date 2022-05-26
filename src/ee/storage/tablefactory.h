/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

#include "persistenttable.h"

#include "common/ids.h"
#include "common/types.h"

#include <string>
#include <vector>

namespace voltdb {

class ExecutorVector;
class ExportTupleStream;
class StreamedTable;
class LargeTempTable;
class TempTable;
class TempTableLimits;

class TableFactory {
public:
    //
    // Every PersistentTable must be instantiated via one of these methods.
    //

    /**
    * Creates an empty persistent table with given name, columns, PK index, other indexes, partition column, etc.
    * Every PersistentTable must be instantiated via this method.
    * Also, columns can't be added/changed/removed after a PersistentTable
    * instance is made. TableColumn is immutable.
    */
    static Table* getPersistentTable(
        voltdb::CatalogId databaseId, char const* name, TupleSchema* schema,
        const std::vector<std::string> &columnNames, char *signature, bool tableIsMaterialized = false,
        int partitionColumn = 0, // defaults provided for ease of testing.
        TableType tableType = PERSISTENT, int tableAllocationTargetSize = 0,
        int32_t compactionThreshold = 95, bool drEnabled = false, bool isReplicated = false);

    static StreamedTable* getStreamedTableForTest(
            voltdb::CatalogId databaseId, const std::string &name, TupleSchema* schema,
            const std::vector<std::string> &columnNames, ExportTupleStream* mockWrapper = NULL,
            bool exportEnabled = false, int32_t compactionThreshold = 95);

    /**
     * Creates an empty temp table with given name and columns.
     * Every TempTable must be instantiated via these factory methods.
     * TempTable doesn't have constraints or indexes. Also, insert/delete/update
     * of tuples doesn't involve Undolog.
     */
    static TempTable* buildTempTable(
        const std::string &name, TupleSchema* schema, const std::vector<std::string> &columnNames,
        TempTableLimits const* limits);

    static LargeTempTable* buildLargeTempTable(const std::string &name, TupleSchema* schema,
            const std::vector<std::string> &columnNames);

    /**
     * Creates an empty temp table from the given template table.
     */
    static AbstractTempTable* buildCopiedTempTable(const std::string &name,
            const Table* templateTable, const ExecutorVector& executorVector);

    /**
     * Creates an empty (normal, non-large) temp table from the given
     * template table.
     */
    static TempTable* buildCopiedTempTable(const std::string &name, const Table* templateTable);

    /**
     * Creates an empty large temp table from the given
     * template table.
     */
    static LargeTempTable* buildCopiedLargeTempTable(const std::string &name, const Table* templateTable);

private:
    static void initCommon(voltdb::CatalogId databaseId, Table *table, const std::string &name,
            TupleSchema *schema, const std::vector<std::string> &columnNames, const bool ownsTupleSchema,
            const int32_t compactionThreshold = 95);

    static void configureStats(char const* name, TableStats *tableStats);
};

}// namespace voltdb

