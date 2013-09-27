/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "IPCTopend.h"
#include "voltdbipc.h"

#include "common/debuglog.h"
#include "storage/table.h"

#include <stdexcept>

namespace voltdb {

IPCTopend::IPCTopend(VoltDBIPC *vdbipc) : m_vdbipc(vdbipc) {}

int IPCTopend::loadNextDependency(int32_t dependencyId, voltdb::Pool *stringPool, Table* destination) {
    VOLT_DEBUG("iterating java dependency for id %d\n", dependencyId);
    size_t dependencySz;
    char* buf = m_vdbipc->retrieveDependency(dependencyId, &dependencySz);
    char *origBuf = buf;

    if (!buf) {
        return 0;
    }

    if (dependencySz > 0) {
        ReferenceSerializeInput serialize_in(buf, dependencySz);
        destination->loadTuplesFrom(serialize_in, stringPool);
        delete [] origBuf;
        return 1;
    }
    else {
        delete [] origBuf;
        return 0;
    }
}

bool IPCTopend::fragmentProgressUpdate(int32_t batchIndex,
        std::string planNodeName,
        std::string lastAccessedTable,
        int64_t lastAccessedTableSize,
        int64_t tuplesFound) {
    return m_vdbipc->fragmentProgressUpdate(batchIndex, planNodeName, lastAccessedTable, lastAccessedTableSize,
            tuplesFound);
}

std::string IPCTopend::planForFragmentId(int64_t fragmentId) {
    return m_vdbipc->planForFragmentId(fragmentId);
}

void IPCTopend::crashVoltDB(FatalException e) {
    m_vdbipc->crashVoltDB(e);
}

int64_t IPCTopend::getQueuedExportBytes(int32_t partitionId, std::string signature) {
    return m_vdbipc->getQueuedExportBytes( partitionId, signature);
}

void IPCTopend::pushExportBuffer(
        int64_t exportGeneration,
        int32_t partitionId,
        std::string signature,
        StreamBlock *block,
        bool sync,
        bool endOfStream) {
    m_vdbipc->pushExportBuffer(exportGeneration, partitionId, signature, block, sync, endOfStream);
}
}

