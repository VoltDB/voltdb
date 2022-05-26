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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;

public class HashMismatchMessage extends VoltMessage {

    private boolean m_reschedule = false;
    private boolean m_checkHostMessage = false;

    public HashMismatchMessage() {
        super();
    }

    public HashMismatchMessage(boolean reschedule) {
        super();
        m_reschedule = reschedule;
    }

    public HashMismatchMessage(boolean reschedule, boolean hostLeaderCheck) {
        this(reschedule);
        m_checkHostMessage = hostLeaderCheck;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 1; // m_reschedule
        msgsize += 1; // m_checkHostMessage
        return msgsize;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_reschedule = buf.get() == 1;
        m_checkHostMessage = buf.get() == 1;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.HASH_MISMATCH_MESSAGE_ID);
        buf.put(m_reschedule ? (byte) 1 : (byte) 0);
        buf.put(m_checkHostMessage ? (byte) 1 : (byte) 0);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    public boolean isReschedule() {
        return m_reschedule;
    }

    public boolean isCheckHostMessage() {
        return m_checkHostMessage;
    }
}
