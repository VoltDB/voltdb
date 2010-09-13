/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
        destination->loadTuplesFrom(true, serialize_in, stringPool);
        delete [] origBuf;
        return 1;
    }
    else {
        delete [] origBuf;
        return 0;
    }
}

void IPCTopend::crashVoltDB(FatalException e) {
    m_vdbipc->crashVoltDB(e);
}
}

