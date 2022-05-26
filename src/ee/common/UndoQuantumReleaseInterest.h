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

#ifndef UNDOQUANTUM_RELEASE_INTEREST_H_
#define UNDOQUANTUM_RELEASE_INTEREST_H_

namespace voltdb {
class UndoQuantumReleaseInterest {
public:
    UndoQuantumReleaseInterest() : m_lastSeenUndoToken(-1) {}
    virtual void notifyQuantumRelease() = 0;
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
private:
    int64_t m_lastSeenUndoToken;
};
}

#endif /* UNDOQUANTUM_H_ */
