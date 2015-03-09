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

#ifndef LEGACYHASHINATOR_H_
#define LEGACYHASHINATOR_H_

#include "common/FatalException.hpp"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/serializeio.h"

#include <cstring>
#include <string>
#include <cassert>
#include <stdlib.h>

namespace voltdb {
/*
 * Hashinator implementation of the original hash function that does modulus partition count
 * for integer types and mimics Java for character/binary types
 */
class LegacyHashinator : public TheHashinator {
public:
    /*
     * Static factory method that does most constructor work so fields can
     * be const
     */
    static LegacyHashinator* newInstance(const char *config) {
        ReferenceSerializeInputBE input(config, 4);
        return new LegacyHashinator(input.readInt());
    }

    ~LegacyHashinator() {}
protected:

   /**
    * Given a long value, pick a partition to store the data.
    *
    * @param value The value to hash.
    * @param partitionCount The number of partitions to choose from.
    * @return A value between 0 and partitionCount-1, hopefully pretty evenly
    * distributed.
    */
   int32_t hashinate(int64_t value) const {
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
   int32_t hashinate(const char *string, int32_t length) const {
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

private:
   LegacyHashinator(int32_t count) : partitionCount(count) {}
   const int32_t partitionCount;
};
}
#endif /* LEGACYHASHINATOR_H_ */
