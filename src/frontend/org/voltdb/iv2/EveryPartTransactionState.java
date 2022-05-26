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

package org.voltdb.iv2;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

/**
 * Every transaction needs a TransactionState.  This provides a minimal one with the proper
 * default settings for every partition transactions (currently only used by system procedures).
 * This used to just use SpTransactionState until it became necessary to not have these
 * claim to be isSinglePartition().
 */
public class EveryPartTransactionState extends TransactionState
{
    final Iv2InitiateTaskMessage m_initiationMsg;
    protected EveryPartTransactionState(TransactionInfoBaseMessage notice)
    {
        super(null, notice);
        if (notice instanceof Iv2InitiateTaskMessage) {
            m_initiationMsg = (Iv2InitiateTaskMessage)notice;
        } else {
            m_initiationMsg = null;
        }
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public StoredProcedureInvocation getInvocation()
    {
        return m_initiationMsg.getStoredProcedureInvocation();
    }
}
