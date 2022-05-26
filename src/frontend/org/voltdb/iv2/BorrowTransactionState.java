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

import org.voltdb.messaging.BorrowTaskMessage;

/**
 * BorrowTransactionState represents the execution of a borrowed
 * read fragment that must be executed before the leader SP has
 * created the multi-partition transaction. This fragment must not
 * block the head of the pendingQueue -- only work arriving from
 * the SP leader can block the queue or the SP leader's ordering
 * is violated at the borrowed site.
 */
public class BorrowTransactionState extends ParticipantTransactionState
{
    BorrowTransactionState(long txnId, BorrowTaskMessage notice)
    {
        super(txnId, notice, true, false);
    }

    @Override
    public boolean isSinglePartition()
    {
        return true;
    }


}
