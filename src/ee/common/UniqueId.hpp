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

#ifndef UNIQUEID_H_
#define UNIQUEID_H_

#include "common/types.h"
#include <cassert>

namespace voltdb
{
class UniqueId {
public:
    const static int64_t TIMESTAMP_BITS = 40;
    const static int64_t COUNTER_BITS = 9;
    const static int64_t PARTITIONID_BITS = 14;
    const static int64_t VOLT_EPOCH = 1199145600000L;
    const static int64_t TIMESTAMP_MAX_VALUE = (1L << TIMESTAMP_BITS) - 1L;
    const static int64_t COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    const static int64_t PARTITIONID_MAX_VALUE = (1L << PARTITIONID_BITS) - 1L;
    const static int64_t PARTITION_ID_MASK = (1 << 14) - 1;
    const static int64_t MP_INIT_PID = PARTITION_ID_MASK;

    static UniqueId makeIdFromComponents(int64_t ts, int64_t seqNo, int64_t partitionId) {
        // compute the time in millis since VOLT_EPOCH
        int64_t uniqueId = ts - VOLT_EPOCH;
        // verify all fields are the right size
        assert(uniqueId <= TIMESTAMP_MAX_VALUE);
        assert(seqNo <= COUNTER_MAX_VALUE);
        assert(partitionId <= PARTITIONID_MAX_VALUE);

        // put this time value in the right offset
        uniqueId = uniqueId << (COUNTER_BITS + PARTITIONID_BITS);
        // add the counter value at the right offset
        uniqueId |= seqNo << PARTITIONID_BITS;
        // finally add the siteid at the end
        uniqueId |= partitionId;

        return uniqueId;
    }

    static int64_t pid(UniqueId uid) {
        return uid & PARTITION_ID_MASK;
    }

    static bool isMpUniqueId(UniqueId uid) {
        return pid(uid) == MP_INIT_PID;
    }

    static int64_t timestamp(UniqueId uid) {
        return (uid & TIMESTAMP_MAX_VALUE) >> (COUNTER_BITS + PARTITIONID_BITS);
    }

    const int64_t uid;

    UniqueId(int64_t uid) : uid(uid) {}

    operator int64_t() const {
        return uid;
    }
};
}
#endif /* UNIQUEID_H_ */
