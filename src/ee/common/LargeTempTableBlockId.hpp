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
    typedef int64_t siteId_t;
    typedef int64_t blockId_t;
    LargeTempTableBlockId(siteId_t siteId, blockId_t blockId) : m_siteId(siteId), m_blockCounter(blockId) {}
    LargeTempTableBlockId(const LargeTempTableBlockId &other) : m_siteId(other.m_siteId), m_blockCounter(other.m_blockCounter) {}
    // Preincrement.
    LargeTempTableBlockId operator++() {
      m_blockCounter++;
      return *this;
    }

    bool operator<(const LargeTempTableBlockId &other) const {
      return (m_siteId < other.m_siteId)
              || ((m_siteId == other.m_siteId) && (m_blockCounter < other.m_blockCounter));
    }

    bool operator==(const LargeTempTableBlockId &other) const {
        return getSiteId() == other.getSiteId() && getBlockCounter() == other.getBlockCounter();
    }

    LargeTempTableBlockId &operator=(const LargeTempTableBlockId &other) {
        if (this != &other) {
            this->m_siteId = other.getSiteId();
            this->m_blockCounter = other.getBlockCounter();
        }
        return *this;
    }
    siteId_t getSiteId() const {
        return m_siteId;
    }
    blockId_t getBlockCounter() const {
        return m_blockCounter;
    }
protected:
    union {
        // For getting at the raw bits.
        int8_t         m_data[sizeof(siteId_t ) + sizeof(blockId_t)];
        // For getting at the data itself.
        struct {
            siteId_t   m_siteId;
            blockId_t  m_blockCounter;
        };
    };
private:
    /*
     * Private and undefined.  This discourages people from using this.
     */
    LargeTempTableBlockId();
};

inline std::ostream &operator<<(std::ostream &out, LargeTempTableBlockId id) {
    IOSFlagSaver saver(out);
    return out << std::dec << id.getSiteId() << "::" << id.getBlockCounter();
}
}
#endif // VOLTDB_LTTBLOCKID_H
