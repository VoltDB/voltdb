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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.SerializableException;

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

    long m_executorHSId;
    long m_destinationHSId;
    long m_txnId;
    private long m_spHandle;
    byte m_status;
    // default dirty to true until proven otherwise
    // Not currently used; leaving it in for now
    boolean m_dirty = true;
    boolean m_recovering = false;
    // WHA?  Why do we have a separate dependency count when
    // the array lists will tell you their lengths?  Doesn't look like
    // we do anything else with this value other than track the length
    short m_dependencyCount = 0;
    ArrayList<Integer> m_dependencyIds = new ArrayList<Integer>();
    ArrayList<VoltTable> m_dependencies = new ArrayList<VoltTable>();
    SerializableException m_exception;

    /** Empty constructor for de-serialization */
    FragmentResponseMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public FragmentResponseMessage(FragmentTaskMessage task, long HSId) {
        m_executorHSId = HSId;
        m_txnId = task.getTxnId();
        m_spHandle = task.getSpHandle();
        m_destinationHSId = task.getCoordinatorHSId();
        m_subject = Subject.DEFAULT.getId();
    }

    // IV2 hacky constructor
    // We need to be able to create a new FragmentResponseMessage
    // with unioned dependency table for sysprocs.  Let us build a
    // FragmentResponse from a prior one.  Don't copy the tables
    // and dependencies because we'll fill those in later.
    public FragmentResponseMessage(FragmentResponseMessage resp)
    {
        m_executorHSId = resp.m_executorHSId;
        m_destinationHSId = resp.m_destinationHSId;
        m_txnId = resp.m_txnId;
        m_spHandle = resp.m_spHandle;
        m_status = resp.m_status;
        m_dirty = resp.m_dirty;
        m_recovering = resp.m_recovering;
        m_exception = resp.m_exception;
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

    // IV2: need to be able to reset this for dep tracking
    // until we change it to count partitions rather than
    // initiator HSIds
    public void setExecutorSiteId(long executorHSId) {
        m_executorHSId = executorHSId;
    }

    public long getExecutorSiteId() {
        return m_executorHSId;
    }

    public long getDestinationSiteId() {
        return m_destinationHSId;
    }

    public long getTxnId() {
        return m_txnId;
    }

    public long getSpHandle() {
        return m_spHandle;
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

    public SerializableException getException() {
        return m_exception;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();

        msgsize += 8 // executorHSId
            + 8 // destinationHSId
            + 8 // txnId
            + 8 // m_spHandle
            + 1 // status byte
            + 1 // dirty flag
            + 1 // node recovering flag
            + 2; // dependency count

        // one int per dependency ID
        msgsize += 4 * m_dependencyCount;

        // one byte to indicate null dependency result table
        msgsize += m_dependencies.size();

        // Add the actual result lengths
        for (VoltTable dep : m_dependencies)
        {
            if (dep != null) {
                msgsize += dep.getSerializedSize();
            }
        }

        if (m_exception != null) {
            msgsize += m_exception.getSerializedSize();
        } else {
            msgsize += 4; //Still serialize exception length 0
        }

        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf)
    {
        assert(m_exception == null || m_status != SUCCESS);
        buf.put(VoltDbMessageFactory.FRAGMENT_RESPONSE_ID);

        buf.putLong(m_executorHSId);
        buf.putLong(m_destinationHSId);
        buf.putLong(m_txnId);
        buf.putLong(m_spHandle);
        buf.put(m_status);
        buf.put((byte) (m_dirty ? 1 : 0));
        buf.put((byte) (m_recovering ? 1 : 0));
        buf.putShort(m_dependencyCount);
        for (int i = 0; i < m_dependencyCount; i++)
            buf.putInt(m_dependencyIds.get(i));

        for (int i = 0; i < m_dependencyCount; i++)
        {
            VoltTable dep = m_dependencies.get(i);
            if (dep == null) {
                buf.put((byte) 0);
            } else {
                buf.put((byte) 1);
                dep.flattenToBuffer(buf);
            }
        }

        if (m_exception != null) {
            m_exception.serializeToBuffer(buf);
        } else {
            buf.putInt(0);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        m_executorHSId = buf.getLong();
        m_destinationHSId = buf.getLong();
        m_txnId = buf.getLong();
        m_spHandle = buf.getLong();
        m_status = buf.get();
        m_dirty = buf.get() == 0 ? false : true;
        m_recovering = buf.get() == 0 ? false : true;
        m_dependencyCount = buf.getShort();
        for (int i = 0; i < m_dependencyCount; i++)
            m_dependencyIds.add(buf.getInt());
        for (int i = 0; i < m_dependencyCount; i++) {
            boolean isNull = buf.get() == 0 ? true : false;
            if (isNull) {
                m_dependencies.add(null);
            } else {
                m_dependencies.add(PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf));
            }
        }
        m_exception = SerializableException.deserializeFromBuffer(buf);
        assert(buf.capacity() == buf.position());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FRAGMENT_RESPONSE (FROM ");
        sb.append(CoreUtils.hsIdToString(m_executorHSId));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(m_destinationHSId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append(", SP HANDLE: ");
        sb.append(m_spHandle);

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
