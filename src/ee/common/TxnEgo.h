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

#include <stdint.h>
#include "common/ids.h"

namespace voltdb {

/**
 * Class to interact with transaction IDs either TxnId or SpHandle. This mirrors the java class of the same name.
 */
class TxnEgo {
public:
    /**
     * @return the ID of the partition which generated id
     */
    static inline int16_t getPartitionId(TransactionId id) {
        return static_cast<int16_t>(id & PARTITIONID_MASK);
    }

    /**
     * @return the sequence number portion of  id
     */
    static inline int64_t getSequenceNumber(TransactionId id) {
        return id >> PARTITIONID_BITS;
    }

    TxnEgo(TransactionId id) : m_id(id) {}

    inline TransactionId getId() const {
        return m_id;
    }

    /**
     * @return the ID of the partition which generated this transaction ID
     */
    inline int16_t getPartitionId() const {
        return getPartitionId(m_id);
    }

    /**
     * @return the sequence number portion of this ID
     */
    inline int64_t getSequenceNumber() const {
        return getSequenceNumber(m_id);
    }

    bool operator==(const TxnEgo& other) const {
        return m_id == other.m_id;
    }

    bool operator!=(const TxnEgo& other) const {
        return m_id != other.m_id;
    }

protected:
    const static int64_t PARTITIONID_BITS = 14;
    const static int64_t PARTITIONID_MASK = (1 << PARTITIONID_BITS) - 1;

private:
    const TransactionId m_id;
};

}
