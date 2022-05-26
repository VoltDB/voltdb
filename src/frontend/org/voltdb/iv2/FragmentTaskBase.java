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

public abstract class FragmentTaskBase extends TransactionTask {
    final protected boolean m_isNPartition;

    public FragmentTaskBase(ParticipantTransactionState txnState, TransactionTaskQueue queue) {
        super(txnState, queue);
        m_isNPartition = txnState.m_npTransaction;
    }

    // Used for FragmentTask and SysprocFragmentTask to match restarted messages
    abstract public long getTimestamp();
}
