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

#ifndef THEHASHINATOR_H_
#define THEHASHINATOR_H_

#include <cstring>
#include <string>
#include <cassert>
#include <vector>
#include <stdlib.h>
#include <boost/foreach.hpp>
#include "common/FatalException.hpp"

namespace voltdb {

/**
 * Class that maps values to partitions. It's rather simple
 * really. It'll get more complicated if you give it time.
 */
class TheHashinator {
  public:

    /**
     * Given a long value, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int32_t hashinate(int64_t value, int32_t partitionCount) {
        uint64_t uvalue = static_cast<uint64_t>(value);

        uint32_t shiftAmount = 32;

        uint32_t left = static_cast<uint32_t>((uvalue >> shiftAmount) + (2 << ~shiftAmount));
        uint32_t right = static_cast<uint32_t>(uvalue);

        int32_t index = static_cast<int32_t>(left ^ right);
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
            throwFatalException("Attempted to hashinate a 0 length or less string %d", length);
        }
        for (int32_t ii = 0; ii < length; ii++) {
           hashCode = 31 * hashCode + string[offset++];
        }
        return abs(hashCode % partitionCount);
    }
};

} // namespace voltdb

#endif // THEHASHINATOR_H_
