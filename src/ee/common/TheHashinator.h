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

#ifndef THEHASHINATOR_H_
#define THEHASHINATOR_H_

#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/FatalException.hpp"
#include "common/types.h"

namespace voltdb {

/**
 *  Abstract base class for hashing SQL values to partition ids
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
    int32_t hashinate(NValue value) const
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
            return hashinate(ValuePeeker::peekAsRawInt64(value));
        }
        case VALUE_TYPE_VARBINARY:
        case VALUE_TYPE_VARCHAR:
        {
            return hashinate(reinterpret_cast<char*>(ValuePeeker::peekObjectValue_withoutNull(value)),
                    ValuePeeker::peekObjectLength_withoutNull(value));
        }
        default:
            throwDynamicSQLException("Attempted to hashinate an unsupported type: %s",
                    getTypeName(val_type).c_str());
            break;
        }
    }
    virtual ~TheHashinator() {}

  protected:
    TheHashinator() {}

    /**
     * Given a long value, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    virtual int32_t hashinate(int64_t value) const = 0;

    /*
     * Given a piece of UTF-8 encoded character data OR binary data
     * pick a partition to store the data
     */
    virtual int32_t hashinate(const char *string, int32_t length) const = 0;
};

} // namespace voltdb

#endif // THEHASHINATOR_H_
