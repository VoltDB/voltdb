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

import org.voltcore.messaging.TransactionInfoBaseMessage;

/**
 * Used between CommandLogReinitiators to informs the MP replayer that the
 * command log for this partition has reached the end, or sent from the MPI to
 * SPIs to indicate the end of MPs.
 */
public class Iv2EndOfLogMessage extends TransactionInfoBaseMessage
{
    // what partition has reached end of log
    private int m_pid;

    public Iv2EndOfLogMessage() {
        super();
    }

    public Iv2EndOfLogMessage(int pid)
    {
        super(0l, 0l, 0l, 0l, false, true);
        m_pid = pid;
    }

    public int getPid() { return m_pid; }

    @Override
    public boolean isSinglePartition() {
        return true;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 4; // m_pid
        return msgsize;
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        super.initFromBuffer(buf);
        m_pid = buf.getInt();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_EOL_ID);
        super.flattenToBuffer(buf);
        buf.putInt(m_pid);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("UNEXPECTED END OF LOG");
    }

    @Override
    public String toString() {
        return "END OF COMMAND LOG FOR PARTITION, PARTITION ID: " + m_pid;
    }
}
