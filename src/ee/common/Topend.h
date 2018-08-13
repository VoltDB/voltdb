/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
#include <string>
#include <queue>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

#include "common/ids.h"
#include "common/types.h"

namespace voltdb {
class Table;
class Pool;
class StreamBlock;
class LargeTempTableBlock;
class FatalException;
class LargeTempTableBlockId;

/*
 * Topend abstracts the EE's calling interface to Java to
 * allow the engine to cleanly integrate both the JNI and
 * the IPC communication paths.
 */
class Topend {
  public:
    virtual int loadNextDependency(int32_t dependencyId, Pool *pool, Table* destination) = 0;
    virtual void traceLog(bool isBegin, const char *name, const char *args) {};
    // Update the topend on query progress and give the topend a chance to tell the
    // query to stop.
    // Return 0 if the Topend wants the EE to stop processing the current fragment
    // or the number of tuples the EE should process before repeating this call.
    virtual int64_t fragmentProgressUpdate(int32_t batchIndex, PlanNodeType planNodeType,
          int64_t tuplesProcessed, int64_t currMemoryInBytes, int64_t peakMemoryInBytes) = 0;
    virtual std::string planForFragmentId(int64_t fragmentId) = 0;
    virtual void crashVoltDB(FatalException const& e) = 0;
    virtual int64_t getQueuedExportBytes(int32_t partitionId, std::string const& signature) = 0;
    virtual void pushExportBuffer(int32_t partitionId, std::string const& signature,
            StreamBlock *block, bool sync) = 0;
    virtual void pushEndOfStream(int32_t partitionId, std::string const& signature) = 0;
    virtual int64_t pushDRBuffer(int32_t partitionId, StreamBlock *block) = 0;
    virtual void pushPoisonPill(int32_t partitionId, std::string& reason, StreamBlock *block) = 0;
    virtual int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName,
          DRRecordType action, DRConflictType deleteConflict, Table* existingMetaTableForDelete,
          Table* existingTupleTableForDelete, Table* expectedMetaTableForDelete, Table* expectedTupleTableForDelete,
          DRConflictType insertConflict, Table* existingMetaTableForInsert, Table* existingTupleTableForInsert,
          Table* newMetaTableForInsert, Table* newTupleTableForInsert) = 0;
    virtual void fallbackToEEAllocatedBuffer(char *buffer, size_t length) = 0;
    /** Calls the java method in org.voltdb.utils.Encoder */
    virtual std::string decodeBase64AndDecompress(const std::string& buffer) = 0;
    /** Store the given block to disk to make room for more large temp table data. */
    virtual bool storeLargeTempTableBlock(LargeTempTableBlock* block) = 0;
    /** Load the given block into memory from disk. */
    virtual bool loadLargeTempTableBlock(LargeTempTableBlock* block) = 0;
    /** Delete any data for the specified block that is stored on disk. */
    virtual bool releaseLargeTempTableBlock(LargeTempTableBlockId const& blockId) = 0;

    // Call into the Java top end to execute a user-defined function.
    // The function ID for the function to be called and the parameter data is stored in a
    // buffer shared by the top end and the EE.
    // The VoltDBEngine will serialize them into the buffer before calling this function.
    virtual int32_t callJavaUserDefinedFunction() = 0;

    // Call into the Java top end to resize the ByteBuffer allocated for the UDF
    // when the current buffer size is not large enough to hold all the parameters.
    // All the buffers in the IPC mode have the same size as MAX_MSG_SZ = 10MB.
    // This function will not do anything under IPC mode.
    // The buffer size in the IPC mode is always MAX_MSG_SZ (10M)
    virtual void resizeUDFBuffer(int32_t size) = 0;
    virtual ~Topend() {}
};

class DummyTopend : public Topend {
    using table_ptr_t = std::unique_ptr<Table>;
    std::queue<int32_t> m_partitionIds;
    std::queue<std::string> m_signatures;
    std::deque<std::unique_ptr<StreamBlock> > m_blocks;
    std::deque<boost::shared_array<char> > m_data;
    bool m_receivedDRBuffer = false;
    bool m_receivedExportBuffer = false;
    int64_t m_pushDRBufferRetval = -1;
    DRRecordType m_actionType;
    DRConflictType m_deleteConflictType;
    DRConflictType m_insertConflictType;
    int32_t m_remoteClusterId;
    int64_t m_remoteTimestamp;
    table_ptr_t m_existingMetaRowsForDelete;
    table_ptr_t m_existingTupleRowsForDelete;
    table_ptr_t m_expectedMetaRowsForDelete;
    table_ptr_t m_expectedTupleRowsForDelete;
    table_ptr_t m_existingMetaRowsForInsert;
    table_ptr_t m_existingTupleRowsForInsert;
    table_ptr_t m_newMetaRowsForInsert;
    table_ptr_t m_newTupleRowsForInsert;
public:
    DummyTopend() = default;
    int loadNextDependency(int32_t dependencyId, Pool *pool, Table* destination) override;
    int64_t fragmentProgressUpdate(int32_t batchIndex, PlanNodeType planNodeType,
            int64_t tuplesFound, int64_t currMemoryInBytes, int64_t peakMemoryInBytes) override;
    std::string planForFragmentId(int64_t fragmentId) override;
    void crashVoltDB(FatalException const& e) override;
    int64_t getQueuedExportBytes(int32_t partitionId, std::string const& signature) override;
    void pushExportBuffer(int32_t partitionId, std::string const& signature, StreamBlock *block, bool sync) override;
    void pushEndOfStream(int32_t partitionId, std::string const& signature) override;
    int64_t pushDRBuffer(int32_t partitionId, StreamBlock *block) override;
    void pushPoisonPill(int32_t partitionId, std::string& reason, StreamBlock *block) override;
    int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, DRRecordType action,
            DRConflictType deleteConflict, Table* existingMetaTableForDelete, Table* existingTupleTableForDelete,
            Table* expectedMetaTableForDelete, Table* expectedTupleTableForDelete,
            DRConflictType insertConflict, Table* existingMetaTableForInsert, Table* existingTupleTableForInsert,
            Table* newMetaTableForInsert, Table* newTupleTableForInsert) override;
    void fallbackToEEAllocatedBuffer(char *buffer, size_t length) override;
    std::string decodeBase64AndDecompress(const std::string& buffer) override;
    bool storeLargeTempTableBlock(LargeTempTableBlock* block) override;
    bool loadLargeTempTableBlock(LargeTempTableBlock* block) override;
    bool releaseLargeTempTableBlock(LargeTempTableBlockId const& blockId) override;
    int32_t callJavaUserDefinedFunction() override;
    void resizeUDFBuffer(int32_t size) override;
};

}
