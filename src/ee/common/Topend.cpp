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

namespace voltdb {
    DummyTopend::DummyTopend() : receivedDRBuffer(false), receivedExportBuffer(false) {

    }

    int DummyTopend::loadNextDependency(
        int32_t dependencyId, voltdb::Pool *pool, Table* destination) {
        return 0;
    }

    int64_t DummyTopend::fragmentProgressUpdate(int32_t batchIndex, std::string planNodeName,
            std::string targetTableName, int64_t targetTableSize, int64_t tuplesFound,
            int64_t currMemoryInBytes, int64_t peakMemoryInBytes) {
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

    void DummyTopend::pushDRBuffer(int32_t partitionId, voltdb::StreamBlock *block) {
        receivedDRBuffer = true;
        partitionIds.push(partitionId);
        blocks.push_back(boost::shared_ptr<StreamBlock>(new StreamBlock(block)));
        data.push_back(boost::shared_array<char>(block->rawPtr()));
    }

    void DummyTopend::fallbackToEEAllocatedBuffer(char *buffer, size_t length) {}

    std::string DummyTopend::decodeBase64AndDecompress(const std::string& buffer) {
        return "";
    }

}
