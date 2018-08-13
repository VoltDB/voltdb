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
#include "common/LargeTempTableBlockId.hpp"
#include "common/Topend.h"
#include "common/StreamBlock.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"

using namespace voltdb;
static Table* copyTable(const std::string &name, Table* template_table, char *signature) {
   Table* t = TableFactory::getPersistentTable(0, name,
         TupleSchema::createTupleSchema(template_table->schema()),
         template_table->getColumnNames(), signature);
    TableTuple tuple(template_table->schema());
    TableIterator iterator = template_table->iterator();
    while (iterator.next(tuple)) {
        t->insertTuple(tuple);
    }
    return t;
}

int DummyTopend::loadNextDependency(
    int32_t dependencyId, Pool *pool, Table* destination) {
    return 0;
}

int64_t DummyTopend::fragmentProgressUpdate(int32_t batchIndex, PlanNodeType planNodeType,
        int64_t tuplesFound, int64_t currMemoryInBytes, int64_t peakMemoryInBytes) {
    return 1000000000; // larger means less likely/frequent callbacks to ignore
}

std::string DummyTopend::planForFragmentId(int64_t fragmentId) {
    return "";
}

void DummyTopend::crashVoltDB(FatalException const& e) {
}

int64_t DummyTopend::getQueuedExportBytes(int32_t partitionId, std::string const& signature) {
    int64_t bytes = 0;
    for (int ii = 0; ii < m_blocks.size(); ii++) {
        bytes += m_blocks[ii]->rawLength();
    }
    return bytes;
}

void DummyTopend::pushExportBuffer(int32_t partitionId, std::string const& signature, StreamBlock *block, bool sync) {
    if (sync) {
        return;
    }
    m_partitionIds.push(partitionId);
    m_signatures.push(signature);
    m_blocks.push_back(std::unique_ptr<StreamBlock>(new StreamBlock(block)));
    m_data.push_back(boost::shared_array<char>(block->rawPtr()));
    m_receivedExportBuffer = true;
}

void DummyTopend::pushEndOfStream(int32_t partitionId, std::string const& signature) {
    m_partitionIds.push(partitionId);
    m_signatures.push(signature);
    m_receivedExportBuffer = true;
}

int64_t DummyTopend::pushDRBuffer(int32_t partitionId, StreamBlock *block) {
    m_receivedDRBuffer = true;
    m_partitionIds.push(partitionId);
    m_blocks.push_back(std::unique_ptr<StreamBlock>(new StreamBlock(block)));
    m_data.push_back(boost::shared_array<char>(block->rawPtr()));
    return m_pushDRBufferRetval;
}


void DummyTopend::pushPoisonPill(int32_t partitionId, std::string& reason, StreamBlock *block) {
    m_partitionIds.push(partitionId);
    m_blocks.push_back(std::unique_ptr<StreamBlock>(new StreamBlock(block)));
    m_data.push_back(boost::shared_array<char>(block->rawPtr()));
}


int DummyTopend::reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName,
      DRRecordType action, DRConflictType deleteConflict, Table* existingMetaTableForDelete, Table* existingTupleTableForDelete,
        Table* expectedMetaTableForDelete, Table* expectedTupleTableForDelete, DRConflictType insertConflict,
        Table* existingMetaTableForInsert, Table* existingTupleTableForInsert, Table* newMetaTableForInsert,
        Table* newTupleTableForInsert) {
    m_actionType = action;
    m_deleteConflictType = deleteConflict;
    m_insertConflictType = insertConflict;
    m_remoteClusterId = remoteClusterId;
    m_remoteTimestamp = remoteTimestamp;
    char signature[20];

    if (existingMetaTableForDelete) {
        m_existingMetaRowsForDelete = table_ptr_t(copyTable("existingMeta", existingMetaTableForDelete, signature));
        m_existingTupleRowsForDelete = table_ptr_t(copyTable("existing", existingTupleTableForDelete, signature));
    }

    if (expectedMetaTableForDelete) {
        m_expectedMetaRowsForDelete = table_ptr_t(copyTable("expectedMeta", expectedMetaTableForDelete, signature));
        m_expectedTupleRowsForDelete = table_ptr_t(copyTable("expected", expectedTupleTableForDelete, signature));
    }

    if (existingMetaTableForInsert) {
        m_existingMetaRowsForInsert = table_ptr_t(copyTable("existingMeta", existingMetaTableForInsert, signature));
        m_existingTupleRowsForInsert = table_ptr_t(copyTable("existing", existingTupleTableForInsert, signature));
    }

    if (newMetaTableForInsert) {
        m_newMetaRowsForInsert = table_ptr_t(copyTable("newMeta", newMetaTableForInsert, signature));
        m_newTupleRowsForInsert = table_ptr_t(copyTable("new", newTupleTableForInsert, signature));
    }
    return 2; /*resolved but not apply remote change*/
}

void DummyTopend::fallbackToEEAllocatedBuffer(char *buffer, size_t length) {}

std::string DummyTopend::decodeBase64AndDecompress(const std::string& buffer) {
    return "";
}

bool DummyTopend::storeLargeTempTableBlock(LargeTempTableBlock* block) {
    return false;
}

bool DummyTopend::loadLargeTempTableBlock(LargeTempTableBlock* block) {
    return false;
}

bool DummyTopend::releaseLargeTempTableBlock(LargeTempTableBlockId const& blockId) {
    return false;
}

int32_t DummyTopend::callJavaUserDefinedFunction() {
    // We do not call any UDF here, directly return zero which means success.
    return 0;
}

void DummyTopend::resizeUDFBuffer(int32_t size) {
    // We do nothing here.
}

