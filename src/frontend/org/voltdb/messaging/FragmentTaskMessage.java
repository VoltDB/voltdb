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
import java.util.List;

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
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

    private static class FragmentData {
        long m_fragmentId = 0;
        ByteBuffer m_parameterSet = null;
        Integer m_outputDepId = null;
        ArrayList<Integer> m_inputDepIds = null;
        // For unplanned item
        byte[] m_fragmentPlan = null;

        public FragmentData() {
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("FRAGMENT ID: %d\n", m_fragmentId));
            if (m_parameterSet != null) {
                FastDeserializer fds = new FastDeserializer(m_parameterSet.asReadOnlyBuffer());
                ParameterSet pset = null;
                try {
                    pset = fds.readObject(ParameterSet.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                assert(pset != null);
                sb.append("\n  ").append(pset.toString());
            }
            if (m_outputDepId != null) {
                sb.append("\n");
                sb.append("  OUTPUT_DEPENDENCY_ID ");
                sb.append(m_outputDepId);
            }
            if ((m_inputDepIds != null) && (m_inputDepIds.size() > 0)) {
                sb.append("\n");
                sb.append("  INPUT_DEPENDENCY_IDS ");
                for (long id : m_inputDepIds)
                    sb.append(id).append(", ");
                sb.setLength(sb.lastIndexOf(", "));
            }
            if ((m_fragmentPlan != null) && (m_fragmentPlan.length != 0)) {
                sb.append("\n");
                sb.append("  FRAGMENT_PLAN ");
                sb.append(m_fragmentPlan);
            }
            return sb.toString();
        }
    }

    List<FragmentData> m_items = new ArrayList<FragmentData>();

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
     * @param isFinal
     */
    public FragmentTaskMessage(long initiatorHSId,
                               long coordinatorHSId,
                               long txnId,
                               boolean isReadOnly,
                               boolean isFinal) {
        super(initiatorHSId, coordinatorHSId, txnId, isReadOnly);

        m_isFinal = isFinal;
        m_subject = Subject.DEFAULT.getId();
        assert(selfCheck());
    }

    /**
     * Add a pre-planned fragment.
     *
     * @param fragmentId
     * @param outputDepId
     * @param parameterSet
     */
    public void addFragment(long fragmentId, int outputDepId, ByteBuffer parameterSet) {
        FragmentData item = new FragmentData();
        item.m_fragmentId = fragmentId;
        item.m_outputDepId = outputDepId;
        item.m_parameterSet = parameterSet;
        m_items.add(item);
    }

    /**
     * Add an unplanned fragment.
     *
     * @param fragmentId
     * @param outputDepId
     * @param parameterSet
     * @param fragmentPlan
     */
    public void addCustomFragment(int outputDepId, ByteBuffer parameterSet, byte[] fragmentPlan) {
        FragmentData item = new FragmentData();
        item.m_outputDepId = outputDepId;
        item.m_parameterSet = parameterSet;
        item.m_fragmentPlan = fragmentPlan;
        m_items.add(item);
    }


    /**
     * Convenience factory method to replace constructory that includes arrays of stuff.
     *
     * @param initiatorHSId
     * @param coordinatorHSId
     * @param txnId
     * @param isReadOnly
     * @param fragmentId
     * @param outputDepId
     * @param parameterSet
     * @param isFinal
     *
     * @return new FragmentTaskMessage
     */
    public static FragmentTaskMessage createWithOneFragment(long initiatorHSId,
                                                            long coordinatorHSId,
                                                            long txnId,
                                                            boolean isReadOnly,
                                                            long fragmentId,
                                                            int outputDepId,
                                                            ByteBuffer parameterSet,
                                                            boolean isFinal) {
        FragmentTaskMessage ret = new FragmentTaskMessage(initiatorHSId, coordinatorHSId,
                                                          txnId, isReadOnly, isFinal);
        ret.addFragment(fragmentId, outputDepId, parameterSet);
        return ret;
    }

    private boolean selfCheck() {
        for (FragmentData item : m_items) {
            if (item == null) {
                return false;
            }
            if (item.m_parameterSet == null) {
                return false;
            }
        }
        return true;
    }

    public void addInputDepId(int index, int depId) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        if (item.m_inputDepIds == null)
            item.m_inputDepIds = new ArrayList<Integer>();
        item.m_inputDepIds.add(depId);
        m_inputDepCount++;
    }

    public ArrayList<Integer> getInputDepIds(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.m_inputDepIds;
    }

    public int getOnlyInputDepId(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        if (item.m_inputDepIds == null)
            return -1;
        assert(item.m_inputDepIds.size() == 1);
        return item.m_inputDepIds.get(0);
    }

    public int[] getAllUnorderedInputDepIds() {
        int[] retval = new int[m_inputDepCount];
        int i = 0;
        for (FragmentData item : m_items) {
            if (item.m_inputDepIds != null) {
                for (int depId : item.m_inputDepIds) {
                    retval[i++] = depId;
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
        return m_items.size();
    }

    public long getFragmentId(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.m_fragmentId;
    }

    public int getOutputDepId(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.m_outputDepId;
    }

    public ByteBuffer getParameterDataForFragment(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.m_parameterSet.asReadOnlyBuffer();
    }

    public byte[] getFragmentPlan(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.m_fragmentPlan;
    }

    /*
     * Serialization Format [description: type: byte count]
     *
     * Fixed header:
     *     item count (nitems): short: 2
     *     unplanned item count (nunplanned): short: 2
     *     final flag: byte: 1
     *     task type: byte: 1
     *     should undo flag: byte: 1
     *     output dependencies flag (outdep): byte: 1
     *     input dependencies flag (indep): byte: 1
     *
     * Fragment ID block (1 per item):
     *     fragment ID: long: 8 * nitems
     *
     * Parameter set block (1 per item):
     *     parameter buffer size: int: 4 * nitems
     *     parameter buffer: bytes: ? * nitems
     *
     * Output dependency block (1 per item if outdep == 1):
     *     output dependency ID: int: 4 * nitems
     *
     * Input dependency block (1 per item if indep == 1):
     *     ID count: short: 2 * nitems
     *     input dependency ID sub-block (1 per ID):
     *         input dependency ID: int: 4 * ? * nitems
     *
     * Unplanned block (1 of each per unplanned item):
     *    item index: short: 2 * nunplanned
     *    fragment plan string length: int: 4 * nunplanned
     *    fragment plan string: bytes: ? * nunplanned
     */

    @Override
    public int getSerializedSize()
    {
        assert(m_items != null);
        assert(!m_items.isEmpty());

        int msgsize = super.getSerializedSize();

        // Fixed header
        msgsize += 2 + 2 + 1 + 1 + 1 + 1 + 1;

        // Fragment ID block
        msgsize += 8 * m_items.size();

        // Make a pass through the fragment data items to account for the
        // optional output and input dependency blocks, plus the unplanned block.
        boolean foundOutputDepId = false;
        boolean foundInputDepIds = false;
        for (FragmentData item : m_items) {
            // Account for parameter sets
            msgsize += 4 + item.m_parameterSet.remaining();

            // Account for the optional output dependency block, if needed.
            if (!foundOutputDepId && item.m_outputDepId != null) {
                msgsize += 4 * m_items.size();
                foundOutputDepId = true;
            }

            // Account for the optional input dependency block, if needed.
            if (item.m_inputDepIds != null) {
                if (!foundInputDepIds) {
                    // Account for the size short for each item now that we know
                    // that the optional block is needed.
                    msgsize += 2 * m_items.size();
                    foundInputDepIds = true;
                }
                // Account for the input dependency IDs themselves, if any.
                msgsize += 4 * item.m_inputDepIds.size();
            }

            // Each unplanned item gets an index (2) and a size (4) and buffer for
            // the fragment plan string.
            if (item.m_fragmentPlan != null) {
                msgsize += 2 + 4 + item.m_fragmentPlan.length;
            }
        }

        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        flattenToSubMessageBuffer(buf);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    /**
     * Used directly by {@link FragmentTaskLogMessage} to embed FTMs
     */
    void flattenToSubMessageBuffer(ByteBuffer buf) throws IOException
    {
        // See Serialization Format comment above getSerializedSize().

        assert(m_items != null);
        assert(!m_items.isEmpty());

        buf.put(VoltDbMessageFactory.FRAGMENT_TASK_ID);
        super.flattenToBuffer(buf);

        // Get useful statistics for the header and optional blocks.
        short nInputDepIds = 0;
        short nOutputDepIds = 0;
        short nUnplanned = 0;
        for (FragmentData item : m_items) {
            if (item.m_inputDepIds != null) {
                // Supporting only one input dep id for now.
                nInputDepIds++;
            }
            if (item.m_outputDepId != null) {
                nOutputDepIds++;
            }
            if (item.m_fragmentPlan != null) {
                nUnplanned++;
            }
        }

        // Header block
        buf.putShort((short) m_items.size());
        buf.putShort(nUnplanned);
        buf.put(m_isFinal ? (byte) 1 : (byte) 0);
        buf.put(m_taskType);
        buf.put(m_shouldUndo ? (byte) 1 : (byte) 0);
        buf.put(nOutputDepIds > 0 ? (byte) 1 : (byte) 0);
        buf.put(nInputDepIds  > 0 ? (byte) 1 : (byte) 0);

        // Fragment ID block
        for (FragmentData item : m_items) {
            buf.putLong(item.m_fragmentId);
        }

        // Parameter set block
        for (FragmentData item : m_items) {
            buf.putInt(item.m_parameterSet.remaining());
            buf.put(item.m_parameterSet.asReadOnlyBuffer());
        }

        // Optional output dependency ID block
        if (nOutputDepIds > 0) {
            for (FragmentData item : m_items) {
                buf.putInt(item.m_outputDepId);
            }
        }

        // Optional input dependency ID block
        if (nInputDepIds > 0) {
            for (FragmentData item : m_items) {
                if (item.m_inputDepIds == null || item.m_inputDepIds.size() == 0) {
                    buf.putShort((short) 0);
                } else {
                    buf.putShort((short) item.m_inputDepIds.size());
                    for (Integer inputDepId : item.m_inputDepIds) {
                        buf.putInt(inputDepId);
                    }
                }
            }
        }

        // Unplanned item block
        for (short index = 0; index < m_items.size(); index++) {
            // Each unplanned item gets an index (2) and a size (4) and buffer for
            // the fragment plan string.
            FragmentData item = m_items.get(index);
            if (item.m_fragmentPlan != null) {
                buf.putShort(index);
                buf.putInt(item.m_fragmentPlan.length);
                buf.put(item.m_fragmentPlan);
            }
        }
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        initFromSubMessageBuffer(buf);
        assert(buf.capacity() == buf.position());
    }

    /**
     * Used directly by {@link FragmentTaskLogMessage} to embed FTMs
     */
    void initFromSubMessageBuffer(ByteBuffer buf) throws IOException
    {
        // See Serialization Format comment above getSerializedSize().

        super.initFromBuffer(buf);

        // Header block
        short fragCount = buf.getShort();
        assert(fragCount > 0);
        short unplannedCount = buf.getShort();
        assert(unplannedCount >= 0 && unplannedCount <= fragCount);
        m_isFinal = buf.get() != 0;
        m_taskType = buf.get();
        m_shouldUndo = buf.get() != 0;
        boolean haveOutputDependencies = buf.get() != 0;
        boolean haveInputDependencies = buf.get() != 0;

        m_items = new ArrayList<FragmentData>(fragCount);

        // Fragment ID block (creates the FragmentData objects)
        for (int i = 0; i < fragCount; i++) {
            FragmentData item = new FragmentData();
            item.m_fragmentId = buf.getLong();
            m_items.add(item);
        }

        // Parameter set block
        for (FragmentData item : m_items) {
            int paramsbytecount = buf.getInt();
            item.m_parameterSet = ByteBuffer.allocate(paramsbytecount);
            int cachedLimit = buf.limit();
            buf.limit(buf.position() + item.m_parameterSet.remaining());
            item.m_parameterSet.put(buf);
            item.m_parameterSet.flip();
            buf.limit(cachedLimit);
        }

        // Optional output dependency block
        if (haveOutputDependencies) {
            for (FragmentData item : m_items) {
                item.m_outputDepId = buf.getInt();
            }
        }

        // Optional input dependency block
        if (haveInputDependencies) {
            for (FragmentData item : m_items) {
                short count = buf.getShort();
                if (count > 0) {
                    item.m_inputDepIds = new ArrayList<Integer>(count);
                    for (int j = 0; j < count; j++) {
                        item.m_inputDepIds.add(buf.getInt());
                        m_inputDepCount++;
                    }
                }
            }
        }

        // Unplanned block
        for (int iUnplanned = 0; iUnplanned < unplannedCount; iUnplanned++) {
            short index = buf.getShort();
            assert(index >= 0 && index < m_items.size());
            FragmentData item = m_items.get(index);
            int fragmentPlanLength = buf.getInt();
            if (fragmentPlanLength > 0) {
                item.m_fragmentPlan = new byte[fragmentPlanLength];
                buf.get(item.m_fragmentPlan);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FRAGMENT_TASK (FROM ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        sb.append("\n");
        if (m_isReadOnly)
            sb.append("  READ, COORD ");
        else
            sb.append("  WRITE, COORD ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));

        for (FragmentData item : m_items) {
            sb.append("\n");
            sb.append("=====");
            sb.append("\n");
            sb.append(item.toString());
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

        return sb.toString();
    }

    public boolean isEmpty() {
        return m_items.isEmpty();
    }
}
