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

#include "BinaryLogSinkWrapper.h"

#include "storage/DRTupleStream.h"
#include "common/serializeio.h"

using namespace std;
using namespace voltdb;

int64_t BinaryLogSinkWrapper::apply(const char *logs,
        std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine,
        int32_t remoteClusterId, int64_t localUniqueId) {
    return m_sink.apply(logs, tables, pool, engine, remoteClusterId, localUniqueId);
}
