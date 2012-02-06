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
import java.util.ArrayList;

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.MiscUtils;
import org.voltdb.ParameterSet;

/**
 * Message from a stored procedure coordinator to an execution site
 * which is participating in the transaction. This message specifies
 * which planfragment to run and with which parameters.
 *
 */
public class FragmentTaskMessage extends TransactionInfoBaseMessage
{
    public static final byte USER_PROC = 0;
    public static final byte SYS_PROC_PER_PARTITION = 1;
    public static final byte SYS_PROC_PER_SITE = 2;

    long[] m_fragmentIds = null;
    ByteBuffer[] m_parameterSets = null;
    int[] m_outputDepIds = null;
    ArrayList<?>[] m_inputDepIds = null;
    boolean m_isFinal = false;
    byte m_taskType = 0;
    // Unused, should get removed from this message
    boolean m_shouldUndo = false;
    int m_inputDepCount = 0;

    /** Empty constructor for de-serialization */
    FragmentTaskMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    /**
     *
     * @param initiatorHSId
     * @param coordinatorHSId
     * @param txnId
     * @param isReadOnly
     * @param fragmentIds
     * @param outputDepIds
     * @param parameterSets
     * @param isFinal
     */
    public FragmentTaskMessage(long initiatorHSId,
                        long coordinatorHSId,
                        long txnId,
                        boolean isReadOnly,
                        long[] fragmentIds,
                        int[] outputDepIds,
                        ByteBuffer[] parameterSets,
                        boolean isFinal) {
        super(initiatorHSId, coordinatorHSId, txnId, isReadOnly);

        assert(fragmentIds != null);
        assert(parameterSets != null);
        assert(parameterSets.length == fragmentIds.length);

        m_fragmentIds = fragmentIds;
        m_outputDepIds = outputDepIds;
        m_parameterSets = parameterSets;
        m_isFinal = isFinal;
        m_subject = Subject.DEFAULT.getId();
        assert(selfCheck());
    }

    private boolean selfCheck() {
        for (ByteBuffer paramSet : m_parameterSets)
            if (paramSet == null)
                return false;
        return true;
    }

    public void addInputDepId(int index, int depId) {
        if (m_inputDepIds == null)
            m_inputDepIds = new ArrayList<?>[m_fragmentIds.length];
        assert(index < m_fragmentIds.length);
        if (m_inputDepIds[index] == null)
            m_inputDepIds[index] = new ArrayList<Integer>();
        @SuppressWarnings("unchecked")
        ArrayList<Integer> l = (ArrayList<Integer>) m_inputDepIds[index];
        l.add(depId);
        m_inputDepCount++;
    }

    @SuppressWarnings("unchecked")
    public ArrayList<Integer> getInputDepIds(int index) {
        if (m_inputDepIds == null)
            return null;
        return (ArrayList<Integer>) m_inputDepIds[index];
    }

    public int getOnlyInputDepId(int index) {
        if (m_inputDepIds == null)
            return -1;
        @SuppressWarnings("unchecked")
        ArrayList<Integer> l = (ArrayList<Integer>) m_inputDepIds[index];
        if (l == null)
            return -1;
        assert(l.size() == 1);
        return l.get(0);
    }

    public int[] getAllUnorderedInputDepIds() {
        int[] retval = new int[m_inputDepCount];
        int i = 0;
        if (m_inputDepIds != null) {
            for (ArrayList<?> l : m_inputDepIds) {
                @SuppressWarnings("unchecked")
                ArrayList<Integer> l2 = (ArrayList<Integer>) l;
                if (l2 != null)
                {
                    for (int depId : l2) {
                        retval[i++] = depId;
                    }
                }
            }
        }
        assert(i == m_inputDepCount);
        return retval;
    }

    public void setFragmentTaskType(byte value) {
        m_taskType = value;
    }

    public boolean isFinalTask() {
        return m_isFinal;
    }

    public boolean isSysProcTask() {
        return (m_taskType != USER_PROC);
    }

    public byte getFragmentTaskType() {
        return m_taskType;
    }

    public int getFragmentCount() {
        return (m_fragmentIds != null) ?
            m_fragmentIds.length : 0;
    }

    public long getFragmentId(int index) {
        return m_fragmentIds[index];
    }

    public int getOutputDepId(int index) {
        return m_outputDepIds[index];
    }

    public ByteBuffer getParameterDataForFragment(int index) {
        return m_parameterSets[index].asReadOnlyBuffer();
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();

        // Fixed length components:
        // m_fragmentIds count (2)
        // m_outputDepIds count (2)
        // m_inputDepIds count (2)
        // m_isFinal (1)
        // m_taskType (1)
        // m_shouldUndo (1)

        // Plus a long (8) per fragment ID
        //    plus an int for the length of the params for a fragment ID
        //    plus the actual length of the params
        // Plus an int (4) per input and output dependency ID

        msgsize += 2 + 2 + 2 + 1 + 1 + 1;
        if (m_fragmentIds != null) {
            msgsize += 8 * m_fragmentIds.length;
            // each frag has one parameter set
            for (int i = 0; i < m_fragmentIds.length; i++)
                msgsize += 4 + m_parameterSets[i].remaining();
        }

        if (m_inputDepIds != null) {
            msgsize += 4 * m_inputDepIds.length;
        }
        if (m_outputDepIds != null) {
            msgsize += 4 * m_outputDepIds.length;
        }

        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.FRAGMENT_TASK_ID);
        super.flattenToBuffer(buf);

        if (m_fragmentIds == null) {
            buf.putShort((short) 0);
        }
        else {
            buf.putShort((short) m_fragmentIds.length);
            for (int i = 0; i < m_fragmentIds.length; i++) {
                buf.putLong(m_fragmentIds[i]);
            }
            for (int i = 0; i < m_fragmentIds.length; i++) {
                buf.putInt(m_parameterSets[i].remaining());
                //Duplicate because the parameter set might be used locally also
                buf.put(m_parameterSets[i].duplicate());
            }
        }

        if (m_outputDepIds == null) {
            buf.putShort((short) 0);
        }
        else {
            buf.putShort((short) m_outputDepIds.length);
            for (int i = 0; i < m_outputDepIds.length; i++) {
                buf.putInt(m_outputDepIds[i]);
            }
        }

        if (m_inputDepIds == null) {
            buf.putShort((short) 0);
        }
        else {
            buf.putShort((short) m_inputDepIds.length);
            for (int i = 0; i < m_inputDepIds.length; i++) {
                @SuppressWarnings("unchecked")
                ArrayList<Integer> l = (ArrayList<Integer>) m_inputDepIds[i];
                buf.putShort((short) l.size());
                for (int depId : l)
                    buf.putInt(depId);
            }
        }

        buf.put(m_isFinal ? (byte) 1 : (byte) 0);
        buf.put(m_taskType);
        buf.put(m_shouldUndo ? (byte) 1 : (byte) 0);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        super.initFromBuffer(buf);

        short fragCount = buf.getShort();
        if (fragCount > 0) {
            m_fragmentIds = new long[fragCount];
            for (int i = 0; i < fragCount; i++)
                m_fragmentIds[i] = buf.getLong();
            m_parameterSets = new ByteBuffer[fragCount];
            for (int i = 0; i < fragCount; i++) {
                int paramsbytecount = buf.getInt();
                m_parameterSets[i] = ByteBuffer.allocate(paramsbytecount);
                int cachedLimit = buf.limit();
                buf.limit(buf.position() + m_parameterSets[i].remaining());
                m_parameterSets[i].put(buf);
                m_parameterSets[i].flip();
                buf.limit(cachedLimit);
            }
        }
        short expectedDepCount = buf.getShort();
        if (expectedDepCount > 0) {
            m_outputDepIds = new int[expectedDepCount];
            for (int i = 0; i < expectedDepCount; i++)
                m_outputDepIds[i] = buf.getInt();
        }

        short inputDepCount = buf.getShort();
        if (inputDepCount > 0) {
            m_inputDepIds = new ArrayList<?>[inputDepCount];
            for (int i = 0; i < inputDepCount; i++) {
                short count = buf.getShort();
                if (count > 0) {
                    ArrayList<Integer> l = new ArrayList<Integer>();
                    for (int j = 0; j < count; j++) {
                        l.add(buf.getInt());
                        m_inputDepCount++;
                    }
                    m_inputDepIds[i] = l;
                }
            }
        }

        m_isFinal = buf.get() == 1;
        m_taskType = buf.get();
        m_shouldUndo = buf.get() == 1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FRAGMENT_TASK (FROM ");
        sb.append(MiscUtils.hsIdToString(m_coordinatorHSId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        sb.append("\n");
        if (m_isReadOnly)
            sb.append("  READ, COORD ");
        else
            sb.append("  WRITE, COORD ");
        sb.append(MiscUtils.hsIdToString(m_coordinatorHSId));

        if ((m_fragmentIds != null) && (m_fragmentIds.length > 0)) {
            sb.append("\n");
            sb.append("  FRAGMENT_IDS ");
            for (long id : m_fragmentIds)
                sb.append(id).append(", ");
            sb.setLength(sb.lastIndexOf(", "));
        }

        if ((m_inputDepIds != null) && (m_inputDepIds.length > 0)) {
            sb.append("\n");
            sb.append("  DEPENDENCY_IDS ");
            for (long id : m_fragmentIds)
                sb.append(id).append(", ");
            sb.setLength(sb.lastIndexOf(", "));
        }

        if (m_isFinal)
            sb.append("\n  THIS IS THE FINAL TASK");

        if (m_taskType == USER_PROC)
        {
            sb.append("\n  THIS IS A SYSPROC TASK");
        }
        else if (m_taskType == SYS_PROC_PER_PARTITION)
        {
            sb.append("\n  THIS IS A SYSPROC RUNNING PER PARTITION");
        }
        else if (m_taskType == SYS_PROC_PER_SITE)
        {
            sb.append("\n  THIS IS A SYSPROC TASK RUNNING PER EXECUTION SITE");
        }
        else
        {
            sb.append("\n  UNKNOWN FRAGMENT TASK TYPE");
        }

        if (m_shouldUndo)
            sb.append("\n  THIS IS AN UNDO REQUEST");

        if ((m_parameterSets != null) && (m_parameterSets.length > 0)) {
            for (ByteBuffer paramSetBytes : m_parameterSets) {
                FastDeserializer fds = new FastDeserializer(paramSetBytes.asReadOnlyBuffer());
                ParameterSet pset = null;
                try {
                    pset = fds.readObject(ParameterSet.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                assert(pset != null);
                sb.append("\n  ").append(pset.toString());

            }
        }

        return sb.toString();
    }
}
