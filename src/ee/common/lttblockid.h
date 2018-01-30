/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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


#ifndef VOLTDB_LTTBLOCKID_H
#define VOLTDB_LTTBLOCKID_H
#include <inttypes.h>
#include <ostream>
#include "common/iosflagsaver.h"

namespace voltdb {
class LargeTempTableBlockId {
public:
    LargeTempTableBlockId(int32_t siteId, int32_t blockId) : m_siteId(siteId), m_bid(blockId) {}
    // Preincrement.
    LargeTempTableBlockId operator++() {
      m_bid++;
      return *this;
    }

    explicit operator int64_t() const {
      return m_id64;
    }

    bool operator<(const LargeTempTableBlockId &other) const {
      return (m_siteId < other.m_siteId)
              || ((m_siteId == other.m_siteId) && (m_bid < other.m_bid));
    }

    bool operator==(const LargeTempTableBlockId &other) const {
        return m_id64 == other.m_id64;
    }

    int32_t getSiteId() const {
        return m_siteId;
    }
    int32_t getBlockId() const {
        return m_bid;
    }
protected:
    union {
        int64_t       m_id64;
        struct {
            int32_t   m_siteId;
            int32_t   m_bid;
        };
    };
};

inline std::ostream &operator<<(std::ostream &out, LargeTempTableBlockId id) {
    IOSFlagSaver saver(out);
    return out << std::dec << id.getSiteId() << "::" << id.getBlockId();
}
}
#endif // VOLTDB_LTTBLOCKID_H
