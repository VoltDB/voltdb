/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef FULLTUPLESERIALIZER_H_
#define FULLTUPLESERIALIZER_H_
#include "common/TupleSerializer.h"
#include "common/tabletuple.h"

namespace voltdb {
class ReferenceSerializeOutput;
class TupleSchema;

/**
 * FullTupleSerializer provides delegate methods to serialize visible and hidden columns
 * of the given tuple. It also gives corresponding max serialization size for buffer allocation.
 */
class FullTupleSerializer : public TupleSerializer {
public:
    /**
     * Serialize the provided tuple to the provide serialize output
     */
    void serializeTo(TableTuple tuple, ReferenceSerializeOutput *out);

    /**
     * Calculate the maximum size of a serialized tuple based upon the schema of the table/tuple
     */
    size_t getMaxSerializedTupleSize(const TupleSchema *schema);

    virtual ~FullTupleSerializer() {}
};
}

#endif /* FULLTUPLESERIALIZER_H_ */
