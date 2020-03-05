/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

#ifndef UNDOQUANTUM_RELEASE_INTEREST_H_
#define UNDOQUANTUM_RELEASE_INTEREST_H_

#include <atomic>

namespace voltdb {
class UndoQuantumReleaseInterest {
public:
    UndoQuantumReleaseInterest() : m_lastSeenUndoToken(-1), m_interestId(s_uniqueTableId++) {}
    virtual void finalizeRelease() = 0;
    virtual ~UndoQuantumReleaseInterest() {}

    inline bool isNewReleaseInterest(int64_t currentUndoToken) {
        if (m_lastSeenUndoToken == currentUndoToken) {
            return false;
        }
        else {
            m_lastSeenUndoToken = currentUndoToken;
            return true;
        }
    }
    inline int64_t getLastSeenUndoToken() const { return m_lastSeenUndoToken; }
    inline int32_t getUniqueInterestId() const { return m_interestId; }
    inline bool operator <(const UndoQuantumReleaseInterest& rhs) { return m_interestId < rhs.m_interestId; }
private:
    static std::atomic<int32_t> s_uniqueTableId;
    int64_t m_lastSeenUndoToken;
    const int32_t m_interestId;
};
}

#endif /* UNDOQUANTUM_H_ */
