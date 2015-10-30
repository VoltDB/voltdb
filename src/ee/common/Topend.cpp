/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

    int64_t DummyTopend::getQueuedExportBytes(int32_t partitionId, std::string signature) {
        int64_t bytes = 0;
        for (int ii = 0; ii < blocks.size(); ii++) {
            bytes += blocks[ii]->rawLength();
        }
        return bytes;
    }

    void DummyTopend::pushExportBuffer(int64_t generation, int32_t partitionId, std::string signature, StreamBlock *block, bool sync, bool endOfStream) {
        if (sync) {
            return;
        }
        partitionIds.push(partitionId);
        signatures.push(signature);
        blocks.push_back(boost::shared_ptr<StreamBlock>(new StreamBlock(block)));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
        receivedExportBuffer = true;
    }

    int64_t DummyTopend::pushDRBuffer(int32_t partitionId, voltdb::StreamBlock *block) {
        receivedDRBuffer = true;
        partitionIds.push(partitionId);
        blocks.push_back(boost::shared_ptr<StreamBlock>(new StreamBlock(block)));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
        return pushDRBufferRetval;
    }


    int DummyTopend::reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, DRRecordType action,
            DRConflictType deleteConflict, Table *existingTableForDelete, Table *expectedTableForDelete,
            DRConflictType insertConflict, Table *existingTableForInsert, Table *newTableForInsert) {
        this->actionType = action;
        this->deleteConflictType = deleteConflict;
        this->insertConflictType = insertConflict;
        char signature[20];
        if (deleteConflict != NO_CONFLICT) {
            this->existingRowsForDelete = boost::shared_ptr<Table>(TableFactory::getPersistentTable(0, "existing", TupleSchema::createTupleSchema(existingTableForDelete->schema()), existingTableForDelete->getColumnNames(), signature));
            TableTuple tempTuple(existingTableForDelete->schema());
            TableIterator iterator = existingTableForDelete->iterator();
            while (iterator.next(tempTuple)) {
                this->existingRowsForDelete->insertTuple(tempTuple);
            }

            this->expectedRowsForDelete = boost::shared_ptr<Table>(TableFactory::getPersistentTable(0, "expected", TupleSchema::createTupleSchema(expectedTableForDelete->schema()), expectedTableForDelete->getColumnNames(), signature));
            iterator = expectedTableForDelete->iterator();
            while (iterator.next(tempTuple)) {
                this->expectedRowsForDelete->insertTuple(tempTuple);
            }
        }
        if (insertConflict != NO_CONFLICT) {
            this->existingRowsForInsert = boost::shared_ptr<Table>(TableFactory::getPersistentTable(0, "existing", TupleSchema::createTupleSchema(existingTableForInsert->schema()), existingTableForInsert->getColumnNames(), signature));
            TableTuple tempTuple(existingTableForInsert->schema());
            TableIterator iterator = existingTableForInsert->iterator();
            while (iterator.next(tempTuple)) {
                this->existingRowsForInsert->insertTuple(tempTuple);
            }

            this->newRowsForInsert = boost::shared_ptr<Table>(TableFactory::getPersistentTable(0, "new", TupleSchema::createTupleSchema(newTableForInsert->schema()), newTableForInsert->getColumnNames(), signature));
            iterator = newTableForInsert->iterator();
            while (iterator.next(tempTuple)) {
                this->newRowsForInsert->insertTuple(tempTuple);
            }
        }

        // TODO: implement a mock conflict resolver so we can test the resolution part of code in EE.
        return true;
    }

    void DummyTopend::fallbackToEEAllocatedBuffer(char *buffer, size_t length) {}

    std::string DummyTopend::decodeBase64AndDecompress(const std::string& buffer) {
        return "";
    }

}
