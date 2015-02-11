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

package org.voltdb.iv2;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.dtxn.TransactionState;

public class ParticipantTransactionState extends TransactionState
{
    ParticipantTransactionState(long txnId, TransactionInfoBaseMessage notice)
    {
        super(null, notice);
    }

    /**
     * This constructor is only reserved for BorrowTransactionState, which is read only now.
     * @param txnId
     * @param notice
     * @param readOnly
     */
    ParticipantTransactionState(long txnId, TransactionInfoBaseMessage notice, boolean readOnly)
    {
        super(null, notice, readOnly);
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
