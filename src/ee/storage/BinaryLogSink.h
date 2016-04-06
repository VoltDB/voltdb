/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef BINARYLOGSINK_H
#define BINARYLOGSINK_H

#include "common/serializeio.h"

#include <boost/unordered_map.hpp>
#include <boost/shared_ptr.hpp>

namespace voltdb {

class PersistentTable;
class Pool;
class VoltDBEngine;

/*
 * Responsible for applying binary logs to table data
 */
class BinaryLogSink {
public:
    BinaryLogSink();
    int64_t applyTxn(ReferenceSerializeInputLE *taskInfo,
                     boost::unordered_map<int64_t, PersistentTable*> &tables,
                     Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId,
                     const char *txnStart);

private:
    int64_t apply(ReferenceSerializeInputLE *taskInfo, const DRRecordType type,
                  boost::unordered_map<int64_t, PersistentTable*> &tables,
                  Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId,
                  const char *txnStart, int64_t sequenceNumber, int64_t uniqueId, bool skipRow);
};


}
#endif
