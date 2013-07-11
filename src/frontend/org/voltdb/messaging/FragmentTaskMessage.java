/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

/**
 * Message from a stored procedure coordinator to an execution site
 * which is participating in the transaction. This message specifies
 * which planfragment to run and with which parameters.
 *
 */
public class FragmentTaskMessage extends TransactionInfoBaseMessage
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final byte USER_PROC = 0;
    public static final byte SYS_PROC_PER_PARTITION = 1;
    public static final byte SYS_PROC_PER_SITE = 2;

    public static final byte[] EMPTY_HASH;
    static {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(-1); // this means the jvm is broke
        }
        md.update("".getBytes(Constants.UTF8ENCODING));
        EMPTY_HASH = md.digest();
    }

    private static class FragmentData {
        byte[] m_planHash = null;
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
            sb.append(String.format("FRAGMENT PLAN HASH: %s\n", Encoder.hexEncode(m_planHash)));
            if (m_parameterSet != null) {
                FastDeserializer fds = new FastDeserializer(m_parameterSet.asReadOnlyBuffer());
                ParameterSet pset = null;
                try {
                    pset = ParameterSet.fromFastDeserializer(fds);
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
    // We use this flag to generate a null FragmentTaskMessage that returns a
    // response for a dependency but doesn't try to execute anything on the EE.
    // This is a hack to serialize CompleteTransactionMessages and
    // BorrowTaskMessages when the first fragment of the restarting transaction
    // is a short-circuited replicated table read.
    // If this flag is set, the message should contain a single fragment with the
    // desired output dep ID, but no real work to do.
    boolean m_emptyForRestart = false;
    int m_inputDepCount = 0;
    Iv2InitiateTaskMessage m_initiateTask;
    ByteBuffer m_initiateTaskBuffer;

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
                               long uniqueId,
                               boolean isReadOnly,
                               boolean isFinal,
                               boolean isForReplay) {
        super(initiatorHSId, coordinatorHSId, txnId, uniqueId, isReadOnly, isForReplay);
        m_isFinal = isFinal;
        m_subject = Subject.DEFAULT.getId();
        assert(selfCheck());
    }

    // The parameter sets are .duplicate()'d in flattenToBuffer,
    // so we can make a shallow copy here and still be thread-safe
    // when we serialize the copy.
    public FragmentTaskMessage(long initiatorHSId,
            long coordinatorHSId,
            FragmentTaskMessage ftask)
    {
        super(initiatorHSId, coordinatorHSId, ftask);

        setSpHandle(ftask.getSpHandle());
        m_taskType = ftask.m_taskType;
        m_isFinal = ftask.m_isFinal;
        m_subject = ftask.m_subject;
        m_inputDepCount = ftask.m_inputDepCount;
        m_items = ftask.m_items;
        m_initiateTask = ftask.m_initiateTask;
        m_emptyForRestart = ftask.m_emptyForRestart;
        if (ftask.m_initiateTaskBuffer != null) {
            m_initiateTaskBuffer = ftask.m_initiateTaskBuffer.duplicate();
        }
        assert(selfCheck());
    }

    /**
     * Add a pre-planned fragment.
     *
     * @param fragmentId
     * @param outputDepId
     * @param parameterSet
     */
    public void addFragment(byte[] planHash, int outputDepId, ByteBuffer parameterSet) {
        FragmentData item = new FragmentData();
        item.m_planHash = planHash;
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
    public void addCustomFragment(byte[] planHash, int outputDepId, ByteBuffer parameterSet, byte[] fragmentPlan) {
        FragmentData item = new FragmentData();
        item.m_planHash = planHash;
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
                                                            long uniqueId,
                                                            boolean isReadOnly,
                                                            byte[] planHash,
                                                            int outputDepId,
                                                            ByteBuffer parameterSet,
                                                            boolean isFinal,
                                                            boolean isForReplay) {
        FragmentTaskMessage ret = new FragmentTaskMessage(initiatorHSId, coordinatorHSId,
                                                          txnId, uniqueId, isReadOnly, isFinal, isForReplay);
        ret.addFragment(planHash, outputDepId, parameterSet);
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

    // We're going to use this fragment task to generate a null distributed
    // fragment to serialize Completion and Borrow messages.  Create an empty
    // fragment with the provided outputDepId
    public void setEmptyForRestart(int outputDepId) {
        m_emptyForRestart = true;
        addFragment(EMPTY_HASH, outputDepId, ByteBuffer.allocate(0));
    }

    public boolean isEmptyForRestart() {
        return m_emptyForRestart;
    }

    /*
     * The first fragment contains the initiate task for a multi-part txn for command logging
     */
    public void setInitiateTask(Iv2InitiateTaskMessage initiateTask) {
        m_initiateTask = initiateTask;
        m_initiateTaskBuffer = ByteBuffer.allocate(initiateTask.getSerializedSize());
        try {
            initiateTask.flattenToBuffer(m_initiateTaskBuffer);
            m_initiateTaskBuffer.flip();
        } catch (IOException e) {
            //Executive decision, don't throw a checked exception. Let it burn.
            throw new RuntimeException(e);
        }
    }

    public Iv2InitiateTaskMessage getInitiateTask() {
        return m_initiateTask;
    }

    public byte[] getPlanHash(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.m_planHash;
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

    public ParameterSet getParameterSetForFragment(int index) {
        ParameterSet params = null;
        final ByteBuffer paramData = m_items.get(index).m_parameterSet.asReadOnlyBuffer();
        if (paramData != null) {
            final FastDeserializer fds = new FastDeserializer(paramData);
            try {
                params = ParameterSet.fromFastDeserializer(fds);
            }
            catch (final IOException e) {
                hostLog.l7dlog(Level.FATAL,
                        LogKeys.host_ExecutionSite_FailedDeserializingParamsForFragmentTask.name(), e);
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }
        }
        else {
            params = ParameterSet.emptyParameterSet();
        }
        return params;
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

        // Fragment ID block (20 bytes per sha1-hash)
        msgsize += 20 * m_items.size();

        //nested initiate task message length prefix
        msgsize += 4;
        if (m_initiateTaskBuffer != null) {
            msgsize += m_initiateTaskBuffer.remaining();
        }

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
        buf.put(m_emptyForRestart ? (byte) 1 : (byte) 0);
        buf.put(nOutputDepIds > 0 ? (byte) 1 : (byte) 0);
        buf.put(nInputDepIds  > 0 ? (byte) 1 : (byte) 0);

        // Plan Hash block
        for (FragmentData item : m_items) {
            buf.put(item.m_planHash);
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

        if (m_initiateTaskBuffer != null) {
            ByteBuffer dup = m_initiateTaskBuffer.duplicate();
            buf.putInt(dup.remaining());
            buf.put(dup);
        } else {
            buf.putInt(0);
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
        m_emptyForRestart = buf.get() != 0;
        boolean haveOutputDependencies = buf.get() != 0;
        boolean haveInputDependencies = buf.get() != 0;

        m_items = new ArrayList<FragmentData>(fragCount);

        // Fragment ID block (creates the FragmentData objects)
        for (int i = 0; i < fragCount; i++) {
            FragmentData item = new FragmentData();
            item.m_planHash = new byte[20]; // sha1 is 20b
            buf.get(item.m_planHash);
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

        int initiateTaskMessageLength = buf.getInt();
        if (initiateTaskMessageLength > 0) {
            int startPosition = buf.position();
            Iv2InitiateTaskMessage message = new Iv2InitiateTaskMessage();
            // EHGAWD: init task was serialized with flatten which added
            // the message type byte. deserialization expects the message
            // factory to have stripped that byte. but ... that's not the
            // way we do it here. So read the message type byte...
            byte messageType = buf.get();
            assert(messageType == VoltDbMessageFactory.IV2_INITIATE_TASK_ID);
            message.initFromBuffer(buf);
            m_initiateTask = message;
            if (m_initiateTask != null && m_initiateTaskBuffer == null) {
                m_initiateTaskBuffer = ByteBuffer.allocate(m_initiateTask.getSerializedSize());
                try {
                    m_initiateTask.flattenToBuffer(m_initiateTaskBuffer);
                    m_initiateTaskBuffer.flip();
                } catch (IOException e) {
                    //Executive decision, don't throw a checked exception. Let it burn.
                    throw new RuntimeException(e);
                }
            }

            /*
             * There is an assertion that all bytes of the message are consumed.
             * Initiate task lazily deserializes the parameter buffer and doesn't consume
             * all the bytes so do it here so the assertion doesn't trip
             */
            buf.position(startPosition + initiateTaskMessageLength);
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
        sb.append(" FOR REPLAY ").append(isForReplay());
        sb.append(", SP HANDLE: ").append(getSpHandle());
        sb.append("\n");
        if (m_isReadOnly)
            sb.append("  READ, COORD ");
        else
            sb.append("  WRITE, COORD ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));

        for (FragmentData item : m_items) {
            sb.append("\n=====\n");
            sb.append(item.toString());
        }

        if (m_isFinal)
            sb.append("\n  THIS IS THE FINAL TASK");

        if (m_taskType == USER_PROC)
        {
            sb.append("\n  THIS IS A USER TASK");
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

        if (m_emptyForRestart)
            sb.append("\n  THIS IS A NULL FRAGMENT TASK USED FOR RESTART");

        return sb.toString();
    }

    public boolean isEmpty() {
        return m_items.isEmpty();
    }
}
