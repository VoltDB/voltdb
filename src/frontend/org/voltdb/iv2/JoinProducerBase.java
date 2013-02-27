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

package org.voltdb.iv2;

import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.TaskLog;

public abstract class JoinProducerBase extends SiteTasker {
    final int m_partitionId;
    final SiteTaskerQueue m_taskQueue;
    InitiatorMailbox m_mailbox = null;

    public static abstract class JoinCompletionAction implements Runnable
    {
        final long m_snapshotTxnId;

        JoinCompletionAction(long snapshotTxnId)
        {
            m_snapshotTxnId = snapshotTxnId;
        }

        long getSnapshotTxnId()
        {
            return m_snapshotTxnId;
        }
    }

    JoinProducerBase(int partitionId, SiteTaskerQueue taskQueue)
    {
        m_partitionId = partitionId;
        m_taskQueue = taskQueue;
    }

    public void setMailbox(InitiatorMailbox mailbox)
    {
        m_mailbox = mailbox;
    }

    public abstract boolean acceptPromotion();

    public abstract void deliver(RejoinMessage message);

    public abstract TaskLog getTaskLog();
}
