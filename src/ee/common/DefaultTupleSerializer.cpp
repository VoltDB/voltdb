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
#include "common/DefaultTupleSerializer.h"
#include "common/serializeio.h"
#include "common/TupleSchema.h"

namespace voltdb {
/**
 * Serialize the provided tuple to the provide serialize output
 */
void DefaultTupleSerializer::serializeTo(TableTuple tuple, ReferenceSerializeOutput *out) {
    tuple.serializeTo(*out);
}

/**
 * Calculate the maximum size of a serialized tuple based upon the schema of the table/tuple
 */
int DefaultTupleSerializer::getMaxSerializedTupleSize(const TupleSchema *schema) {
    size_t size = 4;
    size += static_cast<size_t>(schema->tupleLength());
    for (int ii = 0; ii < schema->columnCount(); ii++) {
        if (!schema->columnIsInlined(ii)) {
            size -= sizeof(void*);
            size += 4 + schema->columnLength(ii);
        } else if ((schema->columnType(ii) == VALUE_TYPE_VARCHAR) || (schema->columnType(ii) == VALUE_TYPE_VARBINARY)) {
            size += 3;//Serialization always uses a 4-byte length prefix
        }
    }
    return static_cast<int>(size);
}
}

