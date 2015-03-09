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

package org.voltdb;

import java.util.Map;

import org.voltcore.utils.InstanceId;
import org.voltdb.dtxn.TransactionCreator;

/**
 * The default command log reinitiator for community edition VoltDB.
 */
public class DefaultCommandLogReinitiator implements CommandLogReinitiator
{
    private Callback m_callback;

    @Override
    public void setCallback(Callback callback) {
        m_callback = callback;
    }

    @Override
    public void replay() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (m_callback != null) {
                    m_callback.onReplayCompletion();
                }
            }
        }).start();
    }

    @Override
    public boolean hasReplayedSegments() {
        return false;
    }

    @Override
    public Long getMaxLastSeenTxn() {
        return null;
    }

    @Override
    public Map<Integer, Long> getMaxLastSeenTxnByPartition() {
        return null;
    }

    @Override
    public void setSnapshotTxnId(RestoreAgent.SnapshotInfo info) {
    }

    @Override
    public void returnAllSegments() {
    }

    @Override
    public boolean checkAndBalancePartitions()
    {
        return true;
    }

    @Override
    public boolean hasReplayedTxns() {
        return false;
    }

    @Override
    public void generateReplayPlan(int newPartitionCount, boolean isMpiNode) {
    }

    @Override
    public void setInitiator(TransactionCreator initiator) {
    }

    @Override
    public InstanceId getInstanceId() {
        // When we don't have a command log, return null to cause the instance ID
        // check to get skipped entirely in RestoreAgent.generatePlans()
        return null;
    }

    @Override
    public void initPartitionTracking() {}
}
