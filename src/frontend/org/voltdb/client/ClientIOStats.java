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

package org.voltdb.client;

class ClientIOStats {

    long m_connectionId;
    long m_bytesSent;
    long m_bytesReceived;

    ClientIOStats(long connectionId, long bytesSent, long bytesReceived) {
        m_connectionId = connectionId;
        m_bytesSent = bytesSent;
        m_bytesReceived = bytesReceived;
    }

    public static ClientIOStats diff(ClientIOStats newer, ClientIOStats older) {
        if (newer.m_connectionId != older.m_connectionId) {
            throw new IllegalArgumentException("Can't diff these ClientIOStats instances.");
        }

        ClientIOStats retval = new ClientIOStats(older.m_connectionId,
                                                 newer.m_bytesSent - older.m_bytesSent,
                                                 newer.m_bytesReceived - older.m_bytesReceived);
        return retval;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    @Override
    protected Object clone() {
        return new ClientIOStats(m_connectionId, m_bytesSent, m_bytesReceived);
    }
}
