/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

class VoltDBIPC;

namespace voltdb {

class IPCTopend : public Topend {
public:
    IPCTopend( VoltDBIPC *vdbipc);

    void handoffReadyELBuffer(char* bufferPtr, int32_t bytesUsed, CatalogId tableId);

    char* claimManagedBuffer(int32_t desiredSizeInBytes);

    void releaseManagedBuffer(char* bufferPtr);

    int loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination);

private:
    ::VoltDBIPC *m_vdbipc;
};
}

#endif /* IPCTOPEND_H_ */
