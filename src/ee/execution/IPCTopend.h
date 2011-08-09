/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef IPCTOPEND_H_
#define IPCTOPEND_H_
#include "common/Topend.h"
#include "common/Pool.hpp"
#include "common/FatalException.hpp"

class VoltDBIPC;

namespace voltdb {

class IPCTopend : public Topend {
public:
    IPCTopend( VoltDBIPC *vdbipc);
    int loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination);
    void crashVoltDB(FatalException e);
    int64_t getQueuedExportBytes(int32_t partitionId, std::string signature);
    void pushExportBuffer(
            int64_t exportGeneration,
            int32_t partitionId,
            std::string signature,
            StreamBlock *block,
            bool sync,
            bool endOfStream);
    void fallbackToEEAllocatedBuffer(char *buffer, size_t length) {

    }
private:
    ::VoltDBIPC *m_vdbipc;
};
}

#endif /* IPCTOPEND_H_ */
