/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#ifndef THEHASHINATOR_H_
#define THEHASHINATOR_H_

#include "common/FatalException.hpp"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"

#include <cstring>
#include <string>
#include <cassert>
#include <stdlib.h>

namespace voltdb {

/**
 * Class that maps values to partitions. It's rather simple
 * really. It'll get more complicated if you give it time.
 */
class TheHashinator {
  public:

    /**
     * Given an NValue, pick a partition to store the data
     *
     * @param value the NValue to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully
     * pretty evenly distributed
     *
     * FUTURE: This could get pushed into NValue at some point.
     * However, since we currently have two matching implementations
     * of hashinate, it's nice to centralize and isolate the code here.
     */
    static int32_t hashinate(NValue value, int32_t partitionCount)
    {
        // All null values hash to partition 0
        if (value.isNull())
        {
            return 0;
        }
        ValueType val_type = ValuePeeker::peekValueType(value);
        switch (val_type)
        {
        case VALUE_TYPE_TINYINT:
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        {
            return hashinate(ValuePeeker::peekAsRawInt64(value),
                             partitionCount);
        }
        case VALUE_TYPE_VARBINARY:
        case VALUE_TYPE_VARCHAR:
        {
            return hashinate(reinterpret_cast<char*>(ValuePeeker::peekObjectValue(value)),
                             ValuePeeker::peekObjectLength(value),
                             partitionCount);
        }
        default:
            throwDynamicSQLException("Attempted to hashinate an unsupported type: %s",
                                     getTypeName(val_type).c_str());
        }
    }

 private:

    /**
     * Given a long value, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int32_t hashinate(int64_t value, int32_t partitionCount) {
        // special case this hard to hash value to 0 (in both c++ and java)
        if (value == INT64_MIN) return 0;

        // hash the same way java does
        int32_t index = static_cast<int32_t>(value^(static_cast<uint64_t>(value) >> 32));
        return abs(index % partitionCount);
    }

    /**
     * Designed to mimic Java string hashing where the hash function is defined as
     * s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
     *
     */
    static int32_t hashinate(const char *string, int32_t length, int32_t partitionCount) {
        int32_t hashCode = 0;
        int32_t offset = 0;
        if (length < 0) {
            throwDynamicSQLException("Attempted to hashinate a value with length(%d) < 0", length);
        }
        for (int32_t ii = 0; ii < length; ii++) {
           hashCode = 31 * hashCode + string[offset++];
        }
        return abs(hashCode % partitionCount);
    }

};

} // namespace voltdb

#endif // THEHASHINATOR_H_
