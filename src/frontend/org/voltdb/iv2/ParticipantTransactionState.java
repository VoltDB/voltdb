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
import org.voltdb.messaging.FragmentTaskMessage;

public class ParticipantTransactionState extends TransactionState
{
    final boolean m_npTransaction;

    ParticipantTransactionState(long txnId, FragmentTaskMessage notice)
    {
        super(null, notice);
        m_npTransaction = notice.isNPartTxn();
    }

    /**
     * This constructor is only reserved for BorrowTransactionState, which is read only now.
     * @param txnId
     * @param notice
     * @param readOnly
     */
    ParticipantTransactionState(long txnId, TransactionInfoBaseMessage notice, boolean readOnly, boolean npTransaction)
    {
        super(null, notice, readOnly);
        m_npTransaction = npTransaction;
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public StoredProcedureInvocation getInvocation()
    {
        return null;
    }
}
