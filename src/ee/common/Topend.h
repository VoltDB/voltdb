/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#ifndef TOPEND_H_
#define TOPEND_H_
#include "common/ids.h"
#include <string>
#include "common/FatalException.hpp"
#include "storage/StreamBlock.h"

namespace voltdb {
class Table;
class Pool;

/*
 * Topend abstracts the EE's calling interface to Java to
 * allow the engine to cleanly integrate both the JNI and
 * the IPC communication paths.
 */
class Topend {
  public:
    virtual int loadNextDependency(
        int32_t dependencyId, voltdb::Pool *pool, Table* destination) = 0;

    // Update the topend on query progress and give the topend a chance to tell the
    // query to stop.
    // Return 0 if the Topend wants the EE to stop processing the current fragment
    // or the number of tuples the EE should process before repeating this call.
    virtual int64_t fragmentProgressUpdate(int32_t batchIndex, std::string planNodeName,
                std::string targetTableName, int64_t targetTableSize, int64_t tuplesProcessed) = 0;

    virtual std::string planForFragmentId(int64_t fragmentId) = 0;

    virtual void crashVoltDB(voltdb::FatalException e) = 0;

    virtual int64_t getQueuedExportBytes(int32_t partitionId, std::string signature) = 0;
    virtual void pushExportBuffer(
            int64_t exportGeneration,
            int32_t partitionId,
            std::string signature,
            StreamBlock *block,
            bool sync,
            bool endOfStream) = 0;

    virtual void fallbackToEEAllocatedBuffer(char *buffer, size_t length) = 0;
    virtual ~Topend()
    {
    }
};

}
#endif /* TOPEND_H_ */
