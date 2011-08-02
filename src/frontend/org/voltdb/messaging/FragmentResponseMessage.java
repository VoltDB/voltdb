/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.util.ArrayList;

import org.voltdb.VoltTable;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.utils.DBBPool;

/**
 * Message from an execution site which is participating in a transaction
 * to the stored procedure coordinator for that transaction. The contents
 * are the tables output by the plan fragments and a status code. In the
 * event of an error, a text message can be embedded in a table attached.
 *
 */
public class FragmentResponseMessage extends VoltMessage {

    public static final byte SUCCESS          = 1;
    public static final byte USER_ERROR       = 2;
    public static final byte UNEXPECTED_ERROR = 3;

    int m_executorSiteId;
    int m_destinationSiteId;
    long m_txnId;
    byte m_status;
    // default dirty to true until proven otherwise
    // Not currently used; leaving it in for now
    boolean m_dirty = true;
    boolean m_recovering = false;
    short m_dependencyCount = 0;
    ArrayList<Integer> m_dependencyIds = new ArrayList<Integer>();
    ArrayList<VoltTable> m_dependencies = new ArrayList<VoltTable>();
    SerializableException m_exception;

    /** Empty constructor for de-serialization */
    FragmentResponseMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public FragmentResponseMessage(FragmentTaskMessage task, int siteId) {
        m_executorSiteId = siteId;
        m_txnId = task.m_txnId;
        m_destinationSiteId = task.m_coordinatorSiteId;
        m_subject = Subject.DEFAULT.getId();
    }

    /**
     * If the status code is failure then an exception may be included.
     * @param status
     * @param e
     */
    public void setStatus(byte status, SerializableException e) {
        m_status = status;
        m_exception = e;
    }

    public boolean isRecovering() {
        return m_recovering;
    }

    public void setRecovering(boolean recovering) {
        m_recovering = recovering;
    }

    public void addDependency(int dependencyId, VoltTable table) {
        m_dependencyIds.add(dependencyId);
        m_dependencies.add(table);
        m_dependencyCount++;
    }

    public int getExecutorSiteId() {
        return m_executorSiteId;
    }

    public int getDestinationSiteId() {
        return m_destinationSiteId;
    }

    public long getTxnId() {
        return m_txnId;
    }

    public byte getStatusCode() {
        return m_status;
    }

    public int getTableCount() {
        return m_dependencyCount;
    }

    public int getTableDependencyIdAtIndex(int index) {
        return m_dependencyIds.get(index);
    }

    public VoltTable getTableAtIndex(int index) {
        return m_dependencies.get(index);
    }

    public RuntimeException getException() {
        return m_exception;
    }

    @Override
    protected void flattenToBuffer(final DBBPool pool) throws IOException {
        int msgsize = 4 + 4 + 8 + 1 + 1 + 1 + 2;
        assert(m_exception == null || m_status != SUCCESS);

        if (m_exception != null) {
            msgsize += m_exception.getSerializedSize();
        } else {
            msgsize += 4;//Still serialize exception length 0
        }

        // stupid lame flattening of the tables
        ByteBuffer tableBytes = null;
        if (m_dependencyCount > 0) {

            FastSerializer fs = new FastSerializer();
            try {
                for (int i = 0; i < m_dependencyCount; i++)
                    fs.writeObject(m_dependencies.get(i));
            } catch (IOException e) {
                e.printStackTrace();
                assert(false);
            }
            tableBytes = fs.getBuffer();
            msgsize += tableBytes.remaining();
            msgsize += 4 * m_dependencyCount;
        }

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(FRAGMENT_RESPONSE_ID);

        m_buffer.putInt(m_executorSiteId);
        m_buffer.putInt(m_destinationSiteId);
        m_buffer.putLong(m_txnId);
        m_buffer.put(m_status);
        m_buffer.put((byte) (m_dirty ? 1 : 0));
        m_buffer.put((byte) (m_recovering ? 1 : 0));
        m_buffer.putShort(m_dependencyCount);
        for (int i = 0; i < m_dependencyCount; i++)
            m_buffer.putInt(m_dependencyIds.get(i));
        if (tableBytes != null)
            m_buffer.put(tableBytes);
        if (m_exception != null) {
            m_exception.serializeToBuffer(m_buffer);
        } else {
            m_buffer.putInt(0);
        }

        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        m_executorSiteId = m_buffer.getInt();
        m_destinationSiteId = m_buffer.getInt();
        m_txnId = m_buffer.getLong();
        m_status = m_buffer.get();
        m_dirty = m_buffer.get() == 0 ? false : true;
        m_recovering = m_buffer.get() == 0 ? false : true;
        m_dependencyCount = m_buffer.getShort();
        //assert(m_dependencyCount <= 50);
        for (int i = 0; i < m_dependencyCount; i++)
            m_dependencyIds.add(m_buffer.getInt());
        for (int i = 0; i < m_dependencyCount; i++) {
            FastDeserializer fds = new FastDeserializer(m_buffer);
            try {
                m_dependencies.add(fds.readObject(VoltTable.class));
            } catch (IOException e) {
                e.printStackTrace();
                assert(false);
            }
        }
        m_exception = SerializableException.deserializeFromBuffer(m_buffer);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FRAGMENT_RESPONSE (FROM ");
        sb.append(m_executorSiteId);
        sb.append(" TO ");
        sb.append(m_destinationSiteId);
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        if (m_status == SUCCESS)
            sb.append("\n  SUCCESS");
        else if (m_status == UNEXPECTED_ERROR)
            sb.append("\n  UNEXPECTED_ERROR");
        else
            sb.append("\n  USER_ERROR");

        if (m_dirty)
            sb.append("\n  DIRTY");
        else
            sb.append("\n  PRISTINE");

        for (int i = 0; i < m_dependencyCount; i++) {
            sb.append("\n  DEP ").append(m_dependencyIds.get(i));
            sb.append(" WITH ").append(m_dependencies.get(i).getRowCount()).append(" ROWS (");
            for (int j = 0; j < m_dependencies.get(i).getColumnCount(); j++) {
                sb.append(m_dependencies.get(i).getColumnName(j)).append(", ");
            }
            sb.setLength(sb.lastIndexOf(", "));
            sb.append(")");
        }

        return sb.toString();
    }
}
