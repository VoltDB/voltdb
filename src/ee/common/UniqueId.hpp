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

#pragma once

#include "common/types.h"
#include "common/debuglog.h"
#include <sstream>

namespace voltdb
{
const static int64_t VOLT_EPOCH =           1199145600000000L;
const static int64_t VOLT_EPOCH_IN_MILLIS = 1199145600000L;

class UniqueId {
public:
    const static int64_t TIMESTAMP_BITS = 40;
    const static int64_t COUNTER_BITS = 9;
    const static int64_t PARTITIONID_BITS = 14;
    const static int64_t TIMESTAMP_MAX_VALUE = (1L << TIMESTAMP_BITS) - 1L;
    const static int64_t COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    const static int64_t TIMESTAMP_PLUS_COUNTER_MAX_VALUE = (1LL << (TIMESTAMP_BITS + COUNTER_BITS)) - 1LL;
    const static int64_t PARTITIONID_MAX_VALUE = (1L << PARTITIONID_BITS) - 1L;
    const static int64_t PARTITION_ID_MASK = PARTITIONID_MAX_VALUE;
    const static int64_t MP_INIT_PID = PARTITIONID_MAX_VALUE;

    static UniqueId makeIdFromComponents(int64_t ts, int64_t seqNo, int64_t partitionId) {
        // compute the time in millis since VOLT_EPOCH_IN_MILLIS
        int64_t uniqueId = ts - VOLT_EPOCH_IN_MILLIS;
        // verify all fields are the right size
        vassert(uniqueId <= TIMESTAMP_MAX_VALUE);
        vassert(seqNo <= COUNTER_MAX_VALUE);
        vassert(partitionId <= PARTITIONID_MAX_VALUE);

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

    static int64_t sequenceNumber(UniqueId uid) {
        int64_t seq = uid >> PARTITIONID_BITS;
        seq = seq & COUNTER_MAX_VALUE;
        return seq;
    }

    // Timestamp excluding the counter
    static int64_t ts(UniqueId uid) {
        int64_t time = uid >> (COUNTER_BITS + PARTITIONID_BITS);
        // Ensure microseconds
        time *= 1000;
        time += VOLT_EPOCH;
        return time;
    }

    // Timestamp excluding the counter
    static int64_t tsInMillis(UniqueId uid) {
        int64_t time = uid >> (COUNTER_BITS + PARTITIONID_BITS);
        time += VOLT_EPOCH_IN_MILLIS;
        return time;
    }

    // Timestamp including the counter
    static int64_t timestampSinceUnixEpoch(UniqueId uid) {
        return tsCounterSinceUnixEpoch((uid >> PARTITIONID_BITS) & TIMESTAMP_PLUS_COUNTER_MAX_VALUE);
    }

    static bool isMpUniqueId(UniqueId uid) {
        return pid(uid) == MP_INIT_PID;
    }


    static std::string toString(UniqueId uid) {
        std::ostringstream oss;
        oss << pid(uid) << ":" << ts(uid) << ":" << sequenceNumber(uid);
        return oss.str();
    }

    // Convert this into a microsecond-resolution timestamp based on Unix epoch;
    // treat the time portion as the time in milliseconds, and the sequence
    // number as if it is a time in microseconds
    static int64_t tsCounterSinceUnixEpoch(int64_t tsCounter) {
        return (tsCounter >> COUNTER_BITS) * 1000 + VOLT_EPOCH + (tsCounter & COUNTER_MAX_VALUE);
    }

    const int64_t uid;

    UniqueId(int64_t uid) : uid(uid) {}

    operator int64_t() const {
        return uid;
    }
};
}

