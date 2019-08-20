/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

#pragma once

#include "common/serializeio.h"

namespace voltdb {

class PersistentTable;
class Pool;
class VoltDBEngine;

class BinaryLog;

/*
 * Responsible for applying binary logs to table data
 */
class BinaryLogSink {
public:
    BinaryLogSink();
    /**
     * Apply the binary logs. The logs can either be on log for multiple transactions to the same partition or multiple
     * logs which correspond to a single multi-partition transaction.
     *
     * The format of logs should be:
     *  number of logs: int32
     *  for each log:
     *      size of log: int32
     *      log contents
     */
    int64_t apply(const char *logs, std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool,
            VoltDBEngine *engine, int32_t remoteClusterId, int64_t localUniqueId);

private:
    /**
     * Apply all transactions within one log
     */
    int64_t applyLog(BinaryLog *log, std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool,
            VoltDBEngine *engine, int32_t remoteClusterId, int64_t localUniqueId);

    /**
     * Apply all records within a single transaction from the binary log.
     */
    int64_t applyTxn(BinaryLog *log, std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool,
            VoltDBEngine *engine, int32_t remoteClusterId, int64_t localUniqueId, bool replicatedTable);

    /**
     * Apply a single transaction to replicated tables
     */
    int64_t applyReplicatedTxn(BinaryLog *log, std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool,
            VoltDBEngine *engine, int32_t remoteClusterId, int64_t localUniqueId);

    /**
     * Apply multiple logs from a single MP transaction
     */
    int64_t applyMpTxn(const char *logs, int32_t logCount, std::unordered_map<int64_t, PersistentTable*> &tables,
            Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId, int64_t localUniqueId);

    /**
     * Apply a single record from a binary log performing any necessary conflict handling.
     */
    int64_t applyRecord(BinaryLog *log, const DRRecordType type,
            std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine,
            int32_t remoteClusterId, bool replicatedTable, bool skipRow);
};


}
