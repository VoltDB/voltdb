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

#ifndef TOPEND_H_
#define TOPEND_H_
#include "common/ids.h"
#include "common/FatalException.hpp"
#include "common/types.h"

#include <string>
#include <queue>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

namespace voltdb {
class Table;
class Pool;
class StreamBlock;

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
    virtual int64_t fragmentProgressUpdate(
                int32_t batchIndex,
                PlanNodeType planNodeType,
                int64_t tuplesProcessed,
                int64_t currMemoryInBytes,
                int64_t peakMemoryInBytes) = 0;

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

    virtual int64_t pushDRBuffer(int32_t partitionId, StreamBlock *block) = 0;

    virtual int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, DRRecordType action,
            DRConflictType deleteConflict, Table *existingMetaTableForDelete, Table *existingTupleTableForDelete,
            Table *expectedMetaTableForDelete, Table *expectedTupleTableForDelete,
            DRConflictType insertConflict, Table *existingMetaTableForInsert, Table *existingTupleTableForInsert,
            Table *newMetaTableForInsert, Table *newTupleTableForInsert) = 0;

    virtual void fallbackToEEAllocatedBuffer(char *buffer, size_t length) = 0;

    /** Calls the java method in org.voltdb.utils.Encoder */
    virtual std::string decodeBase64AndDecompress(const std::string& buffer) = 0;

    virtual ~Topend()
    {
    }
};

class DummyTopend : public Topend {
public:
    DummyTopend();

    int loadNextDependency(
        int32_t dependencyId, voltdb::Pool *pool, Table* destination);

    virtual int64_t fragmentProgressUpdate(
            int32_t batchIndex,
            PlanNodeType planNodeType,
            int64_t tuplesFound,
            int64_t currMemoryInBytes,
            int64_t peakMemoryInBytes);

    std::string planForFragmentId(int64_t fragmentId);

    void crashVoltDB(voltdb::FatalException e);

    int64_t getQueuedExportBytes(int32_t partitionId, std::string signature);

    virtual void pushExportBuffer(int64_t generation, int32_t partitionId, std::string signature, StreamBlock *block, bool sync, bool endOfStream);

    int64_t pushDRBuffer(int32_t partitionId, voltdb::StreamBlock *block);

    int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, DRRecordType action,
            DRConflictType deleteConflict, Table *existingMetaTableForDelete, Table *existingTupleTableForDelete,
            Table *expectedMetaTableForDelete, Table *expectedTupleTableForDelete,
            DRConflictType insertConflict, Table *existingMetaTableForInsert, Table *existingTupleTableForInsert,
            Table *newMetaTableForInsert, Table *newTupleTableForInsert);

    void fallbackToEEAllocatedBuffer(char *buffer, size_t length);

    std::string decodeBase64AndDecompress(const std::string& buffer);

    std::queue<int32_t> partitionIds;
    std::queue<std::string> signatures;
    std::deque<boost::shared_ptr<StreamBlock> > blocks;
    std::deque<boost::shared_array<char> > data;
    bool receivedDRBuffer;
    bool receivedExportBuffer;
    int64_t pushDRBufferRetval;
    DRRecordType actionType;
    DRConflictType deleteConflictType;
    DRConflictType insertConflictType;
    int32_t remoteClusterId;
    int64_t remoteTimestamp;
    boost::shared_ptr<Table> existingMetaRowsForDelete;
    boost::shared_ptr<Table> existingTupleRowsForDelete;
    boost::shared_ptr<Table> expectedMetaRowsForDelete;
    boost::shared_ptr<Table> expectedTupleRowsForDelete;
    boost::shared_ptr<Table> existingMetaRowsForInsert;
    boost::shared_ptr<Table> existingTupleRowsForInsert;
    boost::shared_ptr<Table> newMetaRowsForInsert;
    boost::shared_ptr<Table> newTupleRowsForInsert;
};

}
#endif /* TOPEND_H_ */
