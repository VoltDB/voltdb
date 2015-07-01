/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.List;

import org.voltcore.messaging.TransactionInfoBaseMessage;

/**
 * Used to reply a partition's portion of the work done as part of a
 * multi-partition procedure. It bundles together all of the state-modifying
 * FragmentTaskMessages into one message. Execution sites know how to run
 * this thing.
 *
 * This currently assumes that SQL-based plan fragments that modify
 * state have no dependencies.
 *
 * Initially, this is used to reply work during a pause-less rejoin. It
 * could also be useful for DR replication.
 *
 * One interesting guarantee this makes, when the list of FragmentTasks are
 * consumed, this class ensures there is one and only one final task, and
 * that it will be the last one in the list. Note, it'll make the last task
 * final if it isn't already.
 *
 */
public class FragmentTaskLogMessage extends TransactionInfoBaseMessage {

    /** Empty constructor for de-serialization */
    FragmentTaskLogMessage() {
        super();
        // all logged frag-tasks are always readwrite.
        m_isReadOnly = false;
    }

    public FragmentTaskLogMessage(long initiatorHSId,
                                  long coordinatorHSId,
                                  long txnId) {
        super(initiatorHSId, coordinatorHSId, txnId, 0, false, false);
    }

    private ArrayList<FragmentTaskMessage> m_fragmentTasks = new ArrayList<FragmentTaskMessage>();

    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize();
        size += 4; // fragment count as int
        for (FragmentTaskMessage ft : m_fragmentTasks) {
            size += ft.getSerializedSize();
        }
        return size;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {

        buf.put(VoltDbMessageFactory.FRAGMENT_TASK_LOG_ID);
        super.flattenToBuffer(buf);

        if (buf.position() != super.getSerializedSize()) {
            int size = super.getSerializedSize();
            assert(buf.position() == size);
        }

        buf.putInt(m_fragmentTasks.size());

        assert(buf.position() == super.getSerializedSize() + 4);

        for (FragmentTaskMessage ft : m_fragmentTasks) {
            int pre = buf.position();
            int expected = pre + ft.getSerializedSize();

            ft.flattenToSubMessageBuffer(buf);

            assert(buf.position() == expected);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);
        int size = buf.getInt();
        for (int i = 0; i < size; i++) {
            byte type = buf.get();
            assert(type == VoltDbMessageFactory.FRAGMENT_TASK_ID);
            FragmentTaskMessage ft = new FragmentTaskMessage();
            ft.initFromSubMessageBuffer(buf);

            m_fragmentTasks.add(ft);
        }
    }

    public void appendFragmentTask(FragmentTaskMessage ft) {
        assert(ft.getFragmentCount() > 0);
        m_fragmentTasks.add(ft);
    }

    public List<FragmentTaskMessage> getFragmentTasks() {
        return m_fragmentTasks;
    }
}
