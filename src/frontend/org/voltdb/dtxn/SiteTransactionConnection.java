/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.dtxn;

import java.util.HashMap;
import java.util.List;

import org.voltdb.VoltTable;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.VoltMessage;

/**
 *  TransactionState invokes SiteTransactionConnection methods to manipulate
 *  or request services from an ExecutionSite.
 */
public interface SiteTransactionConnection {


    public FragmentResponseMessage processFragmentTask(
            TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies,
            final VoltMessage task);

    public InitiateResponseMessage processInitiateTask(
            TransactionState txnState,
            final VoltMessage task);

    public void beginNewTxn(TransactionState txnState);

    // Workunits need topology to duplicate suppress replica responses.
    // Feels like another bad side-effect of the "site invokes txnState"
    // and "txnState invokes Site" relationship.
    public SiteTracker getSiteTracker();
}