/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.iv2;

import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements the N-partition procedure ProcedureTask.
 * Runs n-partition transaction, causing work to be distributed
 * to only the involved partitions.
 *
 * It works by trimming the partition master HSIDs passed in to
 * only include the interesting partitions. Whenever the partition
 * master HSID map is updated, it only updates the partitions it's
 * interested in.
 *
 * An n-partition transaction should be checked for mispartitioning
 * before it runs on the site thread. Partitioning could change
 * between queuing of the transaction and the execution of the
 * transaction due to elastic join.
 *
 * Note that DR cannot handle n-partition transactions at the moment
 * as the DRAgent will wait for sentinels from all partitions.
 */
public class NpProcedureTask extends MpProcedureTask {
    /**
     * @param partitionMasters    Assuming that the partition masters map only includes partitions
     *                            that the txn cares about
     */
    public NpProcedureTask(Mailbox mailbox, String procName, TransactionTaskQueue queue, Iv2InitiateTaskMessage msg,
                           Map<Integer, Long> partitionMasters, long buddyHSId, boolean isRestart, int leaderId)
    {
        super(mailbox, procName, queue, msg, Lists.newArrayList(partitionMasters.values()), partitionMasters,
                buddyHSId, isRestart, leaderId, true);

        if (execLog.isTraceEnabled()) {
            execLog.trace("Created N-partition txn " + m_procName + " with partition masters " +
                    CoreUtils.hsIdValueMapToString(partitionMasters));
        }
    }

    @Override
    public void updateMasters(List<Long> masters, Map<Integer, Long> partitionMasters)
    {
        HashMap<Integer, Long> partitionMastersCopy = trimPartitionMasters(partitionMasters);
        super.updateMasters(Lists.newArrayList(partitionMastersCopy.values()), partitionMastersCopy);

        if (execLog.isTraceEnabled()) {
            execLog.trace("Updating N-partition txn " + m_procName + " with new partition masters " +
                    CoreUtils.hsIdValueMapToString(partitionMastersCopy));
        }
    }

    @Override
    public void doRestart(List<Long> masters, Map<Integer, Long> partitionMasters)
    {
        HashMap<Integer, Long> partitionMastersCopy = trimPartitionMasters(partitionMasters);
        super.doRestart(Lists.newArrayList(partitionMastersCopy.values()), partitionMastersCopy);

        if (execLog.isTraceEnabled()) {
            execLog.trace("Restarting N-partition txn " + m_procName + " with new partition masters " +
                    CoreUtils.hsIdValueMapToString(partitionMastersCopy));
        }
    }

    private HashMap<Integer, Long> trimPartitionMasters(Map<Integer, Long> partitionMasters)
    {
        HashMap<Integer,Long> partitionMastersCopy = Maps.newHashMap(partitionMasters);

        // For n-partition transaction, only care about the partitions involved in the transaction.
        partitionMastersCopy.keySet().retainAll(((MpTransactionState) getTransactionState()).m_masterHSIds.keySet());
        return partitionMastersCopy;
    }
}
