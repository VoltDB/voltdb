/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.Serializable;

import org.voltdb.StoredProcedureInvocation;

/**
 * Information the Initiator stores about each transaction in the system
 */
public class InFlightTxnState implements Serializable {
    private static final long serialVersionUID = 4039988810252965910L;

    public InFlightTxnState(
            long txnId,
            int coordinatorId,
            int otherSiteIds[],
            boolean isReadOnly,
            boolean isSinglePartition,
            StoredProcedureInvocation invocation,
            Object clientData,
            int messageSize,
            long initiateTime,
            long connectionId,
            String connectionHostname) {
        this.txnId = txnId;
        this.invocation = invocation;
        this.isReadOnly = isReadOnly;
        this.isSinglePartition = isSinglePartition;
        this.clientData = clientData;
        this.coordinatorId = coordinatorId;
        this.otherSiteIds = otherSiteIds;
        this.messageSize = messageSize;
        this.initiateTime = initiateTime;
        this.connectionId = connectionId;
        this.connectionHostname = connectionHostname;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("IN_FLIGHT_TXN_STATE");
        sb.append("\n  TXN_ID: " + txnId);
        sb.append("\n  COORDINATOR_ID: " + coordinatorId);
        sb.append("\n  OTHER_SITE_IDS: ");
        if (otherSiteIds != null)
        {
            sb.append("(length: " + otherSiteIds.length + ") ");
            for (int i = 0; i < otherSiteIds.length; i++)
            {
                sb.append("" + otherSiteIds[i] + ", ");
            }
        }
        else
        {
            sb.append("NULL");
        }
        sb.append("\n  READ_ONLY: " + isReadOnly);
        sb.append("\n  SINGLE_PARTITION: " + isSinglePartition);
        sb.append("\n");
        sb.append("\n  STORED_PROCEDURE_INVOCATION: " + invocation.toString());
        sb.append("\n");
        return sb.toString();
    }

    public final long txnId;
    public final int coordinatorId;
    public final int otherSiteIds[];
    public final boolean isReadOnly;
    public final boolean isSinglePartition;
    transient public final StoredProcedureInvocation invocation;
    transient public final Object clientData;
    public final int messageSize;
    public final long initiateTime;
    public final long connectionId;
    public final String connectionHostname;
}
