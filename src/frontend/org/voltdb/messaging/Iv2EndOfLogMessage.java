/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltDbMessageFactory;

/**
 * Informs the initiators that the command log for this partition has reached
 * the end. The SPI should release any MP txn for replay immediately when it
 * sees the first fragment.
 */
public class Iv2EndOfLogMessage extends TransactionInfoBaseMessage
{
    // true if this EOL message is from the MPI to the SPIs indicating the
    // end of MP transactions
    private boolean m_isMP = false;

    public Iv2EndOfLogMessage() {
        super();
    }

    public Iv2EndOfLogMessage(boolean isMP)
    {
        super(0l, 0l, 0l, 0l, true, true);
        m_isMP = isMP;
    }

    public boolean isMP()
    {
        return m_isMP;
    }

    @Override
    public boolean isSinglePartition() {
        return true;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 1; // m_isMP
        return msgsize;
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        super.initFromBuffer(buf);
        m_isMP = buf.get() == 1;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_EOL_ID);
        super.flattenToBuffer(buf);
        buf.put(m_isMP ? 1 : (byte) 0);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public String toString() {
        return "END OF COMMAND LOG FOR PARTITION, MP: " + m_isMP;
    }
}
