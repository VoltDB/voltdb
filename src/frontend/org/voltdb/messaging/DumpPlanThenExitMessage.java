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
import org.voltcore.utils.CoreUtils;
import org.voltdb.common.Constants;
import org.voltdb.utils.SerializationHelper;

/**
 * Message from an initiator to its replicas, instructing the
 * site to dump the compile plan from specified stored procedure
 * and default CRUD procedures. After dumping the plan, the site
 * calls crashLocalVoltDB() to stop current node.
 *
 */
public class DumpPlanThenExitMessage extends VoltMessage
{
    String m_procName;

    /** Empty constructor for de-serialization */
    public DumpPlanThenExitMessage() {
        super();
    }

    public DumpPlanThenExitMessage(String procName)
    {
        super();
        m_procName = procName;
    }

    public String getProcName() {
        return m_procName;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 4 /*string length */
                + m_procName.getBytes(Constants.UTF8ENCODING).length;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.DUMP_PLAN_ID);
        SerializationHelper.writeString(m_procName, buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        m_procName = SerializationHelper.getString(buf);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DUMP PLAN FROM " + m_procName);
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        return sb.toString();
    }
}
