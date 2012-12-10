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

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

import org.voltdb.messaging.VoltDbMessageFactory;

/**
 * Informs the SPI that the command log for this partition has reached the end.
 * The SPI should release any MP txn for replay immediately when it sees the
 * first fragment.
 */
public class Iv2EndOfLogMessage extends VoltMessage {
    public Iv2EndOfLogMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {}

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_EOL_ID);
    }

    @Override
    public String toString() {
        return "END OF COMMAND LOG FOR PARTITION";
    }
}
