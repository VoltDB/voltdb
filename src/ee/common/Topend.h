/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include <string>
#include <queue>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

#include "common/ids.h"
#include "common/FatalException.hpp"
#include "common/LargeTempTableBlockId.hpp"
#include "common/types.h"

namespace voltdb {
class Table;
class Pool;
class ExportStreamBlock;
class DrStreamBlock;
class LargeTempTableBlock;

/*
 * Topend abstracts the EE's calling interface to Java to
 * allow the engine to cleanly integrate both the JNI and
 * the IPC communication paths.
 */
class Topend {
  public:
    virtual int loadNextDependency(
        int32_t dependencyId, voltdb::Pool *pool, Table* destination) = 0;

    virtual void traceLog(bool isBegin,
                          const char *name,
                          const char *args) {};

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

    virtual void pushExportBuffer(
            int32_t partitionId,
            std::string tableName,
            ExportStreamBlock *block) = 0;

    virtual int64_t pushDRBuffer(int32_t partitionId, DrStreamBlock *block) = 0;

    virtual void reportDRBuffer(int32_t partitionId, const char *reason, const char *buffer, size_t length) = 0;

    virtual void pushPoisonPill(int32_t partitionId, std::string& reason, DrStreamBlock *block) = 0;

    virtual int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp,
            std::string tableName, bool isReplicatedTable, DRRecordType action,
            DRConflictType deleteConflict, Table *existingMetaTableForDelete, Table *existingTupleTableForDelete,
            Table *expectedMetaTableForDelete, Table *expectedTupleTableForDelete,
            DRConflictType insertConflict, Table *existingMetaTableForInsert, Table *existingTupleTableForInsert,
            Table *newMetaTableForInsert, Table *newTupleTableForInsert) = 0;

    virtual void fallbackToEEAllocatedBuffer(char *buffer, size_t length) = 0;

    /** Calls the java method in org.voltdb.utils.Encoder */
    virtual std::string decodeBase64AndDecompress(const std::string& buffer) = 0;

    /** Store the given block to disk to make room for more large temp table data. */
    virtual bool storeLargeTempTableBlock(LargeTempTableBlock* block) = 0;

    /** Load the given block into memory from disk. */
    virtual bool loadLargeTempTableBlock(LargeTempTableBlock* block) = 0;

    /** Delete any data for the specified block that is stored on disk. */
    virtual bool releaseLargeTempTableBlock(LargeTempTableBlockId blockId) = 0;

    // Call into the Java top end to execute a user-defined function.
    // The function ID for the function to be called and the parameter data is stored in a
    // buffer shared by the top end and the EE.
    // The VoltDBEngine will serialize them into the buffer before calling this function.
    virtual int32_t callJavaUserDefinedFunction() = 0;
    virtual int32_t callJavaUserDefinedAggregateStart(int functionId) = 0;
    virtual int32_t callJavaUserDefinedAggregateAssemble() = 0;
    virtual int32_t callJavaUserDefinedAggregateCombine() = 0;
    virtual int32_t callJavaUserDefinedAggregateWorkerEnd() = 0;
    virtual int32_t callJavaUserDefinedAggregateCoordinatorEnd() = 0;

    // Call into the Java top end to resize the ByteBuffer allocated for the UDF
    // when the current buffer size is not large enough to hold all the parameters.
    // All the buffers in the IPC mode have the same size as MAX_MSG_SZ = 10MB.
    // This function will not do anything under IPC mode.
    // The buffer size in the IPC mode is always MAX_MSG_SZ (10M)
    virtual void resizeUDFBuffer(int32_t size) = 0;

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

    int64_t getFlushedExportBytes(int32_t partitionId);
    virtual void pushExportBuffer(int32_t partitionId, std::string signature, ExportStreamBlock *block);

    int64_t pushDRBuffer(int32_t partitionId, DrStreamBlock *block);

    void reportDRBuffer(int32_t partitionId, const char *reason, const char *buffer, size_t length);

    void pushPoisonPill(int32_t partitionId, std::string& reason, DrStreamBlock *block);

    int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp,
            std::string tableName, bool isReplicatedTable, DRRecordType action,
            DRConflictType deleteConflict, Table *existingMetaTableForDelete, Table *existingTupleTableForDelete,
            Table *expectedMetaTableForDelete, Table *expectedTupleTableForDelete,
            DRConflictType insertConflict, Table *existingMetaTableForInsert, Table *existingTupleTableForInsert,
            Table *newMetaTableForInsert, Table *newTupleTableForInsert);

    void fallbackToEEAllocatedBuffer(char *buffer, size_t length);

    std::string decodeBase64AndDecompress(const std::string& buffer);

    virtual bool storeLargeTempTableBlock(LargeTempTableBlock* block);

    virtual bool loadLargeTempTableBlock(LargeTempTableBlock* block);

    virtual bool releaseLargeTempTableBlock(LargeTempTableBlockId blockId);

    int32_t callJavaUserDefinedFunction();
    int32_t callJavaUserDefinedAggregateStart(int functionId);
    int32_t callJavaUserDefinedAggregateAssemble();
    int32_t callJavaUserDefinedAggregateCombine();
    int32_t callJavaUserDefinedAggregateWorkerEnd();
    int32_t callJavaUserDefinedAggregateCoordinatorEnd();
    void resizeUDFBuffer(int32_t size);

    std::queue<int32_t> partitionIds;
    std::queue<std::string> signatures;
    std::deque<boost::shared_ptr<DrStreamBlock> > drBlocks;
    std::deque<boost::shared_ptr<ExportStreamBlock> > exportBlocks;
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
