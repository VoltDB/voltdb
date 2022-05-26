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
#include "common/Topend.h"
#include "common/StreamBlock.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"

namespace voltdb {
    static Table* copyTable(const std::string &name, Table* template_table, char *signature) {
        Table* t = TableFactory::getPersistentTable(0, name.c_str(),
                TupleSchema::createTupleSchema(template_table->schema()),
                template_table->getColumnNames(), signature);
        TableTuple tuple(template_table->schema());
        TableIterator iterator = template_table->iterator();
        while (iterator.next(tuple)) {
            t->insertTuple(tuple);
        }
        return t;
    }

    DummyTopend::DummyTopend() : receivedDRBuffer(false), receivedExportBuffer(false), pushDRBufferRetval(-1) {

    }

    int DummyTopend::loadNextDependency(
        int32_t dependencyId, voltdb::Pool *pool, Table* destination) {
        return 0;
    }

    int64_t DummyTopend::fragmentProgressUpdate(
            int32_t batchIndex,
            PlanNodeType planNodeType,
            int64_t tuplesFound,
            int64_t currMemoryInBytes,
            int64_t peakMemoryInBytes) {
        return 1000000000; // larger means less likely/frequent callbacks to ignore
    }

    std::string DummyTopend::planForFragmentId(int64_t fragmentId) {
        return "";
    }

    void DummyTopend::crashVoltDB(voltdb::FatalException e) {
    }

    int64_t DummyTopend::getFlushedExportBytes(int32_t partitionId) {
        int64_t bytes = 0;
        for (int ii = 0; ii < exportBlocks.size(); ii++) {
            bytes += exportBlocks[ii]->rawLength();
        }
        return bytes;
    }

    void DummyTopend::pushExportBuffer(int32_t partitionId, std::string signature,
            ExportStreamBlock *block) {
        if (!block) {
            return;
        }
        partitionIds.push(partitionId);
        signatures.push(signature);
        exportBlocks.push_back(boost::shared_ptr<ExportStreamBlock>(block));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
        receivedExportBuffer = true;
    }

    // only used for EE Test, ignore the committedSpHandle for now
    int64_t DummyTopend::pushDRBuffer(int32_t partitionId, DrStreamBlock *block) {
        receivedDRBuffer = true;
        partitionIds.push(partitionId);
        drBlocks.push_back(boost::shared_ptr<DrStreamBlock>(block));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
        return pushDRBufferRetval;
    }

    void DummyTopend::reportDRBuffer(int32_t partitionId, const char *reason, const char *buffer, size_t length) {
        return;
    }

    void DummyTopend::pushPoisonPill(int32_t partitionId, std::string& reason, DrStreamBlock *block) {
        partitionIds.push(partitionId);
        drBlocks.push_back(boost::shared_ptr<DrStreamBlock>(block));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
    }


    int DummyTopend::reportDRConflict(int32_t partitionId, int32_t remoteClusterId,
            int64_t remoteTimestamp, std::string tableName, bool isReplicatedTable, DRRecordType action,
            DRConflictType deleteConflict, Table *existingMetaTableForDelete, Table *existingTupleTableForDelete,
            Table *expectedMetaTableForDelete, Table *expectedTupleTableForDelete,
            DRConflictType insertConflict, Table *existingMetaTableForInsert, Table *existingTupleTableForInsert,
            Table *newMetaTableForInsert, Table *newTupleTableForInsert) {
        this->actionType = action;
        this->deleteConflictType = deleteConflict;
        this->insertConflictType = insertConflict;
        this->remoteClusterId = remoteClusterId;
        this->remoteTimestamp = remoteTimestamp;
        char signature[20];

        if (existingMetaTableForDelete) {
            this->existingMetaRowsForDelete = boost::shared_ptr<Table>(copyTable("existingMeta",
                                                                                 existingMetaTableForDelete,
                                                                                 signature));
            this->existingTupleRowsForDelete = boost::shared_ptr<Table>(copyTable("existing",
                                                                                  existingTupleTableForDelete,
                                                                                  signature));
        }

        if (expectedMetaTableForDelete) {
            this->expectedMetaRowsForDelete = boost::shared_ptr<Table>(copyTable("expectedMeta",
                                                                                 expectedMetaTableForDelete,
                                                                                 signature));
            this->expectedTupleRowsForDelete = boost::shared_ptr<Table>(copyTable("expected",
                                                                                  expectedTupleTableForDelete,
                                                                                  signature));
        }

        if (existingMetaTableForInsert) {
            this->existingMetaRowsForInsert = boost::shared_ptr<Table>(copyTable("existingMeta",
                                                                                 existingMetaTableForInsert,
                                                                                 signature));
            this->existingTupleRowsForInsert = boost::shared_ptr<Table>(copyTable("existing",
                                                                                  existingTupleTableForInsert,
                                                                                  signature));
        }

        if (newMetaTableForInsert) {
            this->newMetaRowsForInsert = boost::shared_ptr<Table>(copyTable("newMeta",
                                                                            newMetaTableForInsert,
                                                                            signature));
            this->newTupleRowsForInsert = boost::shared_ptr<Table>(copyTable("new",
                                                                             newTupleTableForInsert,
                                                                             signature));
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

    bool DummyTopend::releaseLargeTempTableBlock(LargeTempTableBlockId blockId) {
        return false;
    }

    int32_t DummyTopend::callJavaUserDefinedFunction() {
        // We do not call any UDF here, directly return zero which means success.
        return 0;
    }

    int32_t DummyTopend::callJavaUserDefinedAggregateStart(int functionId) {
        return 0;
    }

    int32_t DummyTopend::callJavaUserDefinedAggregateAssemble() {
        return 0;
    }

    int32_t DummyTopend::callJavaUserDefinedAggregateCombine() {
        return 0;
    }

    int32_t DummyTopend::callJavaUserDefinedAggregateWorkerEnd() {
        return 0;
    }

    int32_t DummyTopend::callJavaUserDefinedAggregateCoordinatorEnd() {
        return 0;
    }

    void DummyTopend::resizeUDFBuffer(int32_t size) {
        // We do nothing here.
    }

} // end namespace voltdb
