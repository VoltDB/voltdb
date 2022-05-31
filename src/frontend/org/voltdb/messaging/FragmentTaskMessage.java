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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.common.Constants;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableSet;

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
        int m_inputDepId = -1;
        byte[] m_stmtName = null;
        // For unplanned item
        byte[] m_fragmentPlan = null;
        byte[] m_stmtText = null;

        public FragmentData() {
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("FRAGMENT PLAN HASH: %s\n", Encoder.hexEncode(m_planHash)));
            if (m_stmtName != null) {
                sb.append("\n  STATEMENT NAME: ");
                sb.append(getStmtName());
            }
            if (m_parameterSet != null) {
                ParameterSet pset = null;
                try {
                    pset = ParameterSet.fromByteBuffer(m_parameterSet.asReadOnlyBuffer());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                assert(pset != null);
                sb.append("\n  ").append(pset.toString());
            }
            if (m_outputDepId != null) {
                sb.append("\n  OUTPUT_DEPENDENCY_ID ");
                sb.append(m_outputDepId);
            }
            if (m_inputDepId != -1) {
                sb.append("\n  INPUT_DEPENDENCY_ID ");
                sb.append(m_inputDepId);
            }
            if (m_fragmentPlan != null && m_fragmentPlan.length != 0) {
                sb.append("\n  FRAGMENT_PLAN ");
                sb.append(new String(m_fragmentPlan, Charsets.UTF_8));
            }
            if (m_stmtText != null && m_stmtText.length != 0) {
                sb.append("\n  STATEMENT_TEXT ");
                sb.append(new String(m_stmtText, Charsets.UTF_8));
            }
            return sb.toString();
        }

        public String getStmtName() {
            if (m_stmtName != null) {
                return new String(m_stmtName, Charsets.UTF_8);
            }
            else {
                return null;
            }
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

    // Used to flag an N-Part transaction
    boolean m_nPartTxn;

    // If this flag = true, it means the current execution is being sampled.
    boolean m_perFragmentStatsRecording = false;
    boolean m_coordinatorTask = false;

    Iv2InitiateTaskMessage m_initiateTask;
    ByteBuffer m_initiateTaskBuffer;
    // Partitions involved in this multipart, set in the first fragment
    Set<Integer> m_involvedPartitions = ImmutableSet.of();

    // The name of a procedure to load at places about to run FragmentTasks
    // generated by this procedure in an MP txn. Currently used for
    // default procs that are auto-generated and whose plans might not be
    // loaded everywhere. Also used for @LoadMultipartitionTable because it
    // leverages the default procs' plan.
    byte[] m_procNameToLoad = null;

    // context for long running fragment status log messages
    byte[] m_procedureName = null;
    int m_currentBatchIndex = 0;

    int m_batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;

    // indicate that the fragment should be handled via original partition leader
    // before MigratePartitionLeader if the first batch or fragment has been processed in a batched or
    // multiple fragment transaction. m_currentBatchIndex > 0
    boolean m_executedOnPreviousLeader = false;

    // Use to differentiate fragments and completions from different rounds of restart
    // (same transaction can be restarted multiple times due to multiple leader promotions)
    long m_restartTimestamp = -1;

    // The last Unique ID generated by the partition on which this fragment is being executed
    // Only used by SpScheduler.updateReplicas during a SnapshotSave
    long m_lastSpUniqueId;

    int m_maxResponseSize = Integer.MAX_VALUE;

    public void setPerFragmentStatsRecording(boolean value) {
        m_perFragmentStatsRecording = value;
    }

    public boolean isPerFragmentStatsRecording() {
        return m_perFragmentStatsRecording;
    }

    public void setCoordinatorTask(boolean value) {
        m_coordinatorTask = value;
    }

    public boolean isCoordinatorTask() {
        return m_coordinatorTask;
    }

    public int getCurrentBatchIndex() {
        return m_currentBatchIndex;
    }

    /** Empty constructor for de-serialization */
    public FragmentTaskMessage() {
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
                               boolean isForReplay,
                               boolean nPartTxn,
                               long timestamp) {
        super(initiatorHSId, coordinatorHSId, txnId, uniqueId, isReadOnly, isForReplay);
        m_isFinal = isFinal;
        m_subject = Subject.DEFAULT.getId();
        m_nPartTxn = nPartTxn;
        m_restartTimestamp = timestamp;
        assert(selfCheck());
    }

    // If you add a new field to the message and you don't want to lose information at all point,
    // remember to add it to the constructor below, Because this constructor is used to copy a message at some place.
    // for example, in SpScheduler.handleFragmentTaskMessage()
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
        m_nPartTxn = ftask.m_nPartTxn;
        m_items = ftask.m_items;
        m_initiateTask = ftask.m_initiateTask;
        m_emptyForRestart = ftask.m_emptyForRestart;
        m_procedureName = ftask.m_procedureName;
        m_currentBatchIndex = ftask.m_currentBatchIndex;
        m_involvedPartitions = ftask.m_involvedPartitions;
        m_procNameToLoad = ftask.m_procNameToLoad;
        m_batchTimeout = ftask.m_batchTimeout;
        m_perFragmentStatsRecording = ftask.m_perFragmentStatsRecording;
        m_coordinatorTask = ftask.m_coordinatorTask;
        m_restartTimestamp = ftask.m_restartTimestamp;
        if (ftask.m_initiateTaskBuffer != null) {
            m_initiateTaskBuffer = ftask.m_initiateTaskBuffer.duplicate();
        }
        m_lastSpUniqueId = ftask.m_lastSpUniqueId;
        m_maxResponseSize = ftask.m_maxResponseSize;
        assert(selfCheck());
    }

    public void setProcedureName(String procedureName) {
        Iv2InitiateTaskMessage it = getInitiateTask();
        if (it != null) {
            assert(it.getStoredProcedureName().equals(procedureName));
        }
        else {
            m_procedureName = procedureName.getBytes(Charsets.UTF_8);
        }
    }

    public void setBatch(int batchIndex) {
        m_currentBatchIndex = batchIndex;
    }

    /**
     * Add a pre-planned fragment.
     *
     * @param fragmentId
     * @param outputDepId
     * @param parameterSet
     */
    public void addFragment(byte[] planHash, int outputDepId, ByteBuffer parameterSet) {
        addFragment(planHash, null, outputDepId, parameterSet);
    }

    public void addFragment(byte[] planHash, String stmtName, int outputDepId, ByteBuffer parameterSet) {
        FragmentData item = new FragmentData();
        item.m_planHash = planHash;
        if (stmtName != null) {
            item.m_stmtName = stmtName.getBytes(Charsets.UTF_8);
        }
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
    public void addCustomFragment(byte[] planHash, int outputDepId, ByteBuffer parameterSet, byte[] fragmentPlan, String stmtText) {
        FragmentData item = new FragmentData();
        item.m_planHash = planHash;
        item.m_outputDepId = outputDepId;
        item.m_parameterSet = parameterSet;
        item.m_fragmentPlan = fragmentPlan;
        item.m_stmtText = stmtText.getBytes();
        m_items.add(item);
    }


    /**
     * Convenience factory method to replace constructor that includes arrays of stuff.
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
                                                            ParameterSet params,
                                                            boolean isFinal,
                                                            boolean isForReplay,
                                                            boolean isNPartTxn,
                                                            long timestamp) {
        ByteBuffer parambytes = null;
        if (params != null) {
            parambytes = ByteBuffer.allocate(params.getSerializedSize());
            try {
                params.flattenToBuffer(parambytes);
                parambytes.flip();
            }
            catch (IOException e) {
                VoltDB.crashLocalVoltDB("Failed to serialize parameter for fragment: " + params.toString(), true, e);
            }
        }

        FragmentTaskMessage ret = new FragmentTaskMessage(initiatorHSId, coordinatorHSId,
                                                          txnId, uniqueId, isReadOnly, isFinal,
                                                          isForReplay, isNPartTxn, timestamp);
        ret.addFragment(planHash, outputDepId, parambytes);
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

    public void setInputDepId(int index, int depId) {
        assert (index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert (item != null);
        assert item.m_inputDepId == -1;

        item.m_inputDepId = depId;
    }

    public int getOnlyInputDepId(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.m_inputDepId;
    }

    public void setFragmentTaskType(byte value) {
        m_taskType = value;
    }

    public void setProcNameToLoad(String procNameToLoad) {
        if (procNameToLoad != null) {
            m_procNameToLoad = procNameToLoad.getBytes(Charsets.UTF_8);
        }
        else {
            m_procNameToLoad = null;
        }
    }

    public void setProcNameToLoad(byte[] procNameToLoad) {
        m_procNameToLoad = procNameToLoad;
    }

    public String getProcNameToLoad() {
        if (m_procNameToLoad != null) {
            return new String(m_procNameToLoad, Charsets.UTF_8);
        }
        else {
            return null;
        }
    }

    public int getBatchTimeout() {
        return m_batchTimeout;
    }

    public void setBatchTimeout(int batchTimeout) {
        m_batchTimeout = batchTimeout;
    }

    @Override
    public long getLastSpUniqueId() {
        return m_lastSpUniqueId;
    }

    public void setLastSpUniqueId(long lastSpUniqueId) {
        this.m_lastSpUniqueId = lastSpUniqueId;
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
        ParameterSet blank = ParameterSet.emptyParameterSet();
        ByteBuffer mt = ByteBuffer.allocate(blank.getSerializedSize());
        try {
            blank.flattenToBuffer(mt);
        }
        catch (IOException ioe) {
            // Shouldn't ever happen, just bail out to not-obviously equivalent behavior
            mt = ByteBuffer.allocate(2);
            mt.putShort((short)0);
        }
        addFragment(EMPTY_HASH, outputDepId, mt);
    }

    public boolean isEmptyForRestart() {
        return m_emptyForRestart;
    }

    public String getProcedureName() {
        Iv2InitiateTaskMessage initMsg = getInitiateTask();
        if (initMsg != null) {
            return initMsg.m_invocation.getProcName();
        }
        else if (m_procedureName != null) {
            return new String(m_procedureName, Charsets.UTF_8);
        }
        else {
            return null;
        }
    }

    /*
     * The first fragment contains the initiate task and the involved partitions set
     * for a multi-part txn for command logging.
     *
     * Involved partitions set is a set of partition IDs that are involved in this
     * multi-part txn.
     */
    public void setStateForDurability(Iv2InitiateTaskMessage initiateTask,
                                      Collection<Integer> involvedPartitions) {
        m_initiateTask = initiateTask;
        m_involvedPartitions = ImmutableSet.copyOf(involvedPartitions);
        assert(!m_nPartTxn || m_involvedPartitions.size() > 0);
        // this function may be called for the same instance twice, with slightly different
        // but same size initiateTask. The second call is intended to update the spHandle in
        // the initiateTask to a new value and update the corresponding buffer, therefore it
        // only change the spHandle which takes a fixed amount of bytes, the only component
        // that can change size, which is the StoredProcedureInvocation, isn't changed at all.
        // Because of these, the serialized size will be the same for the two calls and the
        // buffer only needs to be allocated once. When this function is called the second
        // time, just reset the position and limit and reserialize the updated content to the
        // existing ByteBuffer so we can save an allocation.
        if (m_initiateTaskBuffer == null) {
            m_initiateTaskBuffer = ByteBuffer.allocate(initiateTask.getSerializedSize());
        }
        else {
            m_initiateTaskBuffer.position(0);
            m_initiateTaskBuffer.limit(initiateTask.getSerializedSize());
        }
        try {
            initiateTask.flattenToBuffer(m_initiateTaskBuffer);
            m_initiateTaskBuffer.flip();
        } catch (IOException e) {
            //Executive decision, don't throw a checked exception. Let it burn.
            throw new RuntimeException(e);
        }
    }

    // Can be null for non-first fragment task message or read-only transaction
    // or in replay
    public Iv2InitiateTaskMessage getInitiateTask() {
        return m_initiateTask;
    }

    public Set<Integer> getInvolvedPartitions() { return m_involvedPartitions; }

    public boolean isNPartTxn() {
        return m_nPartTxn;
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
            try {
                params = ParameterSet.fromByteBuffer(paramData);
            }
            catch (final IOException e) {
                hostLog.fatal("Failure while deserializing a parameter set for a fragment task", e);
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

    public String getStmtName(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return item.getStmtName();
    }

    public String getStmtText(int index) {
        assert(index >= 0 && index < m_items.size());
        FragmentData item = m_items.get(index);
        assert(item != null);
        return new String(item.m_stmtText, Constants.UTF8ENCODING);
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
     *     NPart Partition Count: byte : 1
     *     FragmentRestart Sequence: long : 8
     *     Last SP Unique ID : 8
     *
     * Procedure name to load string (if any).
     *
     * perFragmentStatsRecording and coordinatorTask flag: byte: 2
     *
     * Fragment ID block (1 per item):
     *     fragment ID: long: 8 * nitems
     *
     * Procedure name: byte: length of the name string.
     *
     * voltExecuteIndex: short: 2
     *
     * batchIndexBase: short: 2
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

    public int getFixedHeaderSize() {
        int msgsize = super.getSerializedSize();

        // Fixed header
        msgsize += 2 + 2 + 1 + 1 + 1 + 1 + 1 + 2 + 1 + 8 + 8 + 4;

        // procname to load str if any
        if (m_procNameToLoad != null) {
            msgsize += m_procNameToLoad.length;
        }

        // perFragmentStatsRecording and coordinatorTask.
        // TODO: We could use only one byte and bitmasks to represent all the
        // boolean values used in this class, it can save a little bit space.
        msgsize += 2;

        // Fragment ID block (20 bytes per sha1-hash)
        msgsize += 20 * m_items.size();

        // short + str for proc name
        msgsize += 2;
        if (m_procedureName != null) {
            msgsize += m_procedureName.length;
        }

        // int for which batch (4)
        msgsize += 4;

        // 1 byte for the timeout flag
        msgsize += 1;

        msgsize += 1; //m_handleByOriginalLeader
        msgsize += m_batchTimeout == BatchTimeoutOverrideType.NO_TIMEOUT ? 0 : 4;

        // Involved partitions
        msgsize += 2 + m_involvedPartitions.size() * 4;

        //nested initiate task message length prefix
        msgsize += 4;

        return msgsize;
    }

    @Override
    public int getSerializedSize()
    {
        assert(m_items != null);
        assert(!m_items.isEmpty());

        int msgsize = getFixedHeaderSize();

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

            // short + str for stmt name
            msgsize += 2;
            if (item.m_stmtName != null) {
                msgsize += item.m_stmtName.length;
            }

            // Account for the optional output dependency block, if needed.
            if (!foundOutputDepId && item.m_outputDepId != null) {
                msgsize += 4 * m_items.size();
                foundOutputDepId = true;
            }

            // Account for the optional input dependency block, if needed.
            if (item.m_inputDepId != -1) {
                if (!foundInputDepIds) {
                    // Account for the size short for each item now that we know
                    // that the optional block is needed.
                    msgsize += 2 * m_items.size();
                    foundInputDepIds = true;
                }
                // Account for the input dependency IDs themselves, if any.
                msgsize += 4;
            }

            // Each unplanned item gets:
            //  - an index (2)
            //  - a size (4)
            //  - buffer for fragment plan string
            //  - a size (4)
            //  - a buffer for statement text
            if (item.m_fragmentPlan != null) {
                msgsize += 2 + 4 + item.m_fragmentPlan.length;

                assert(item.m_stmtText != null);
                msgsize += 4 + item.m_stmtText.length;
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
            if (item.m_inputDepId != -1) {
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
        buf.put(m_executedOnPreviousLeader ? (byte) 1 : (byte) 0);
        buf.put(nOutputDepIds > 0 ? (byte) 1 : (byte) 0);
        buf.put(nInputDepIds  > 0 ? (byte) 1 : (byte) 0);
        if (m_procNameToLoad != null) {
            buf.putShort((short) m_procNameToLoad.length);
            buf.put(m_procNameToLoad);
        }
        else {
            buf.putShort((short) -1);
        }
        buf.put(m_perFragmentStatsRecording ? (byte) 1 : (byte) 0);
        buf.put(m_coordinatorTask ? (byte) 1 : (byte) 0);
        // N Partition Transaction PartitionCount
        buf.put(m_nPartTxn ? (byte) 1 : (byte) 0);
        // timestamp for restarted transaction
        buf.putLong(m_restartTimestamp);

        buf.putLong(m_lastSpUniqueId);
        buf.putInt(m_maxResponseSize);

        // Plan Hash block
        for (FragmentData item : m_items) {
            buf.put(item.m_planHash);
        }

        for (FragmentData item : m_items) {
            // write statement name
            if (item.m_stmtName == null) {
                buf.putShort((short) -1);
            }
            else {
                assert(item.m_stmtName.length <= Short.MAX_VALUE);
                buf.putShort((short) item.m_stmtName.length);
                buf.put(item.m_stmtName);
            }
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
                if (item.m_inputDepId == -1) {
                    buf.putShort((short) 0);
                } else {
                    buf.putShort((short) 1);
                    buf.putInt(item.m_inputDepId);
                }
            }
        }

        // write procedure name
        if (m_procedureName == null) {
            buf.putShort((short) -1);
        }
        else {
            assert(m_procedureName.length <= Short.MAX_VALUE);
            buf.putShort((short) m_procedureName.length);
            buf.put(m_procedureName);
        }

        // ints for batch context
        buf.putInt(m_currentBatchIndex);

        // put byte flag for timeout value and 4 bytes integer value if specified
        if (m_batchTimeout == BatchTimeoutOverrideType.NO_TIMEOUT) {
            buf.put(BatchTimeoutOverrideType.NO_OVERRIDE_FOR_BATCH_TIMEOUT.getValue());
        } else {
            buf.put(BatchTimeoutOverrideType.HAS_OVERRIDE_FOR_BATCH_TIMEOUT.getValue());
            buf.putInt(m_batchTimeout);
        }

        buf.putShort((short) m_involvedPartitions.size());
        for (int pid : m_involvedPartitions) {
            buf.putInt(pid);
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
            // Each unplanned item gets:
            //  - an index (2)
            //  - a size (4)
            //  - buffer for the fragment plan string
            //  - a size(4)
            //  - buffer for the statement text
            FragmentData item = m_items.get(index);
            if (item.m_fragmentPlan != null) {
                buf.putShort(index);
                buf.putInt(item.m_fragmentPlan.length);
                buf.put(item.m_fragmentPlan);

                assert(item.m_stmtText != null);
                buf.putInt(item.m_stmtText.length);
                buf.put(item.m_stmtText);
            }
        }

    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        initFromSubMessageBuffer(buf);
        assert(buf.capacity() == buf.position());
    }

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
        m_executedOnPreviousLeader = buf.get() == 1;
        boolean haveOutputDependencies = buf.get() != 0;
        boolean haveInputDependencies = buf.get() != 0;
        short procNameToLoadBytesLen = buf.getShort();
        if (procNameToLoadBytesLen >= 0) {
            m_procNameToLoad = new byte[procNameToLoadBytesLen];
            buf.get(m_procNameToLoad);
        }
        m_perFragmentStatsRecording = buf.get() != 0;
        m_coordinatorTask = buf.get() != 0;
        // N Partition Transaction PartitionCount
        m_nPartTxn = buf.get() != 0;
        // timestamp for restarted transaction
        m_restartTimestamp = buf.getLong();
        m_lastSpUniqueId = buf.getLong();
        m_maxResponseSize = buf.getInt();

        m_items = new ArrayList<FragmentData>(fragCount);

        // Fragment ID block (creates the FragmentData objects)
        for (int i = 0; i < fragCount; i++) {
            FragmentData item = new FragmentData();
            item.m_planHash = new byte[20]; // sha1 is 20b
            buf.get(item.m_planHash);
            m_items.add(item);
        }

        // Statement names block
        for (FragmentData item : m_items) {
            short stmtNameLen = buf.getShort();
            if (stmtNameLen >= 0) {
                item.m_stmtName = new byte[stmtNameLen];
                buf.get(item.m_stmtName);
            }
            else {
                item.m_stmtName = null;
            }
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
                    assert count == 1;
                    item.m_inputDepId = buf.getInt();
                }
            }
        }

        // read procedure name if there
        short procNameLen = buf.getShort();
        if (procNameLen >= 0) {
            m_procedureName = new byte[procNameLen];
            buf.get(m_procedureName);
        }
        else {
            m_procedureName = null;
        }

        // ints for batch context
        m_currentBatchIndex = buf.getInt();

        BatchTimeoutOverrideType batchTimeoutType = BatchTimeoutOverrideType.typeFromByte(buf.get());
        if (batchTimeoutType == BatchTimeoutOverrideType.NO_OVERRIDE_FOR_BATCH_TIMEOUT) {
            m_batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
        } else {
            m_batchTimeout = buf.getInt();
        }

        // Involved partition
        short involvedPartitionCount = buf.getShort();
        ImmutableSet.Builder<Integer> involvedPartitionsBuilder = ImmutableSet.builder();
        for (int i = 0; i < involvedPartitionCount; i++) {
            involvedPartitionsBuilder.add(buf.getInt());
        }
        m_involvedPartitions = involvedPartitionsBuilder.build();

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

                int stmtTextLength = buf.getInt();
                item.m_stmtText = new byte[stmtTextLength];
                buf.get(item.m_stmtText);
            }
        }
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("FRAGMENT TASK: FragmentIndex: ").append(m_currentBatchIndex);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FRAGMENT_TASK (FROM ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));
        sb.append(") FOR TXN ").append(TxnEgo.txnIdToString(m_txnId)).append("(" + m_txnId + ")");
        sb.append(" FOR REPLAY ").append(isForReplay());
        sb.append(", SP HANDLE: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append(", TRUNCATION HANDLE:" + getTruncationHandle());
        sb.append("\n");
        sb.append("THIS IS A ");
        sb.append(m_coordinatorTask ? "COORDINATOR" : "WORKER");
        sb.append(" TASK.\n");
        if (m_perFragmentStatsRecording) {
            sb.append("PER FRAGMENT STATS RECORDING\n");
        }
        if (m_isReadOnly) {
            sb.append("  READ, COORD ");
        } else
        if (m_nPartTxn) {
            sb.append("  ").append(" N part WRITE, COORD ");
        } else {
            sb.append("  WRITE, COORD ");
        }
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));

        if (!m_emptyForRestart) {
            for (FragmentData item : m_items) {
                sb.append("\n=====\n");
                sb.append(item.toString());
            }
        }
        else {
            sb.append("\n=====\n");
            sb.append("  FRAGMENT EMPTY FOR RESTART SERIALIZATION");
        }

        if (m_isFinal) {
            sb.append("\n  THIS IS THE FINAL TASK");
        }
        if (m_isForReplica) {
            sb.append("\n  THIS IS SENT TO REPLICA");
        }

        if (m_executedOnPreviousLeader) {
            sb.append("\n  EXECUTE ON ORIGNAL LEADER");
        }
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
        sb.append("\nBatch index:").append(m_currentBatchIndex).append(" Dep count: ")
                .append(m_items.stream().mapToInt(i -> i.m_inputDepId == -1 ? 0 : 1).sum());

        if (m_emptyForRestart) {
            sb.append("\n  THIS IS A NULL FRAGMENT TASK USED FOR RESTART");
        }

        String procName = getProcedureName();
        if (procName != null) {
            sb.append("\n  PROC NAME:" + procName);
        }
        return sb.toString();
    }

    public boolean isEmpty() {
        return m_items.isEmpty();
    }

    public void setExecutedOnPreviousLeader(boolean forOldLeader) {
        m_executedOnPreviousLeader = forOldLeader;
    }

    public boolean isExecutedOnPreviousLeader() {
        return m_executedOnPreviousLeader;
    }

    public void setTimestamp(long timestamp) {
        m_restartTimestamp = timestamp;
    }

    public long getTimestamp() {
        return m_restartTimestamp;
    }

    public void setMaxResponseSize(int maxResponseSize) {
        assert maxResponseSize > 0;
        m_maxResponseSize = maxResponseSize;
    }

    public int getMaxResponseSize() {
        return m_maxResponseSize;
    }
}
