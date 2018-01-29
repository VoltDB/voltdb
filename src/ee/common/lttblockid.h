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

namespace voltdb {
class LargeTempTableBlockId {
public:
    LargeTempTableBlockId(int64_t id) : m_id(id) {}
    // Preincrement.
    LargeTempTableBlockId operator++() {
      m_id++;
      return *this;
    }

    explicit operator int64_t() const {
      return m_id;
    }

    bool operator<(const LargeTempTableBlockId &other) const {
      return m_id < other.m_id;
    }

    bool operator==(const LargeTempTableBlockId &other) const {
        return m_id == other.m_id;
    }
protected:
    int64_t       m_id;
};

inline std::ostream &operator<<(std::ostream &out, LargeTempTableBlockId id) {
  return out << int64_t(id);
}
}
#endif // VOLTDB_LTTBLOCKID_H
