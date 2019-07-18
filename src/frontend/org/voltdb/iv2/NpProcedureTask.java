/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
