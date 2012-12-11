/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;

/**
 * Information the Initiator stores about each transaction in the system
 */
public class InFlightTxnState implements Serializable {
    private static final long serialVersionUID = 4039988810252965910L;

    public InFlightTxnState(
            long txnId,
            long firstCoordinatorId,
            ArrayList<Long> coordinatorReplicas,
            long otherSiteIds[],
            boolean isReadOnly,
            boolean isSinglePartition,
            StoredProcedureInvocation invocation,
            Object clientData,
            int messageSize,
            long initiateTime,
            long connectionId,
            String connectionHostname,
            boolean isAdmin,
            boolean allowMismatchedResults)
    {
        this.txnId = txnId;
        this.firstCoordinatorId = firstCoordinatorId;
        this.coordinatorReplicas = coordinatorReplicas;
        this.invocation = invocation;
        this.isReadOnly = isReadOnly;
        this.isSinglePartition = isSinglePartition;
        this.clientData = clientData;
        this.otherSiteIds = otherSiteIds;
        this.messageSize = messageSize;
        this.initiateTime = initiateTime;
        this.connectionId = connectionId;
        this.connectionHostname = connectionHostname;
        this.isAdmin = isAdmin;
        this.allowMismatchedResults = allowMismatchedResults;

        m_outstandingResponses = 1;

        if (isSinglePartition) {
            m_outstandingCoordinators = new HashSet<Long>();
            m_outstandingCoordinators.add(firstCoordinatorId);
        }
    }

    public void addCoordinator(long coordinatorId) {
        assert(isSinglePartition);
        if (m_outstandingCoordinators.add(coordinatorId))
            m_outstandingResponses++;
    }

    public int countOutstandingResponses() {
        return m_outstandingResponses;
    }

    public ClientResponseImpl addResponse(long coordinatorHSId, ClientResponseImpl r) {
        Integer sqlHash = r.getHash(); // get this and then reset it
        r.setHash(null); // this makes sure the hash doesn't get sent to clients

        // ensure response to send isn't null
        if (m_responseToSend == null) m_responseToSend = r;

        // remove this coordinator from the outstanding list
        if (m_outstandingCoordinators != null)
            m_outstandingCoordinators.remove(coordinatorHSId);

        m_outstandingResponses--;

        VoltTable[] currResults = r.getResults();

        // Check that the replicated procedure ran the same SQL with the same
        // parameters for each run of the java code (non-deterministic SQL)
        // (Note that this applies for k > 0)
        // If not same, kill entire cluster and hide the bodies.
        // In all seriousness, we have no valid way to recover from a non-deterministic event
        // The safest thing is to make the user aware and stop doing potentially corrupt work.
        // Note that read-only procs and sysprocs just have a null hash value, so they don't
        // trip anything up here.

        // ensure the hash is non-null
        sqlHash = sqlHash == null ? 0 : sqlHash;
        if (m_sqlHash != null) {
             if (m_sqlHash.equals(sqlHash) == false) {
                 {
                     String msg = "Mismatched hash of SQL run for transaction ID: " + txnId;
                     msg += "\n  while executing stored procedure: " + invocation.getProcName();
                     msg += "\n  from execution site: " + CoreUtils.hsIdToString(coordinatorHSId);
                     msg += "\n  Expected hash value: " + m_sqlHash;
                     msg += "\n  Mismatched hash value: " + sqlHash;
                     msg += "\n  Read-only: " + new Boolean(isReadOnly).toString();
                     // die die die
                     VoltDB.crashGlobalVoltDB(msg, false, null); // kills process
                     throw new RuntimeException(msg); // gets called only by test code
                 }
             }
        }
        else if (m_outstandingResponses > 0) {
            m_sqlHash = sqlHash;
        }

        // Check that the replica results are the same (non-deterministic SQL)
        // (Note that this applies for k > 0)
        // If not same, kill entire cluster and hide the bodies.
        // As stated above, we have no valid way to recover from a non-deterministic event
        // The safest thing is to make the user aware and stop doing potentially corrupt work.
        // ENG-3288 - Allow non-deterministic read-only transactions to have mismatched results
        // so that LIMIT queries without ORDER BY clauses work.
        if (m_resultsForComparison != null) {
            if (!allowMismatchedResults) {
                VoltTable[] curr_results = r.getResults();
                if (m_resultsForComparison.length != curr_results.length)
                {
                    String msg = "Mismatched result count received for transaction ID: " + txnId;
                    msg += "\n  while executing stored procedure: " + invocation.getProcName();
                    msg += "\n  from execution site: " + CoreUtils.hsIdToString(coordinatorHSId);
                    msg += "\n  Expected number of results: " + m_resultsForComparison.length;
                    msg += "\n  Mismatched number of results: " + curr_results.length;
                    msg += "\n  Read-only: " + new Boolean(isReadOnly).toString();
                    // die die die
                    VoltDB.crashGlobalVoltDB(msg, false, null); // kills process
                    throw new RuntimeException(msg); // gets called only by test code
                }
                for (int i = 0; i < m_resultsForComparison.length; ++i)
                {
                    if (!curr_results[i].hasSameContents(m_resultsForComparison[i]))
                    {
                        String msg = "Mismatched results received for transaction ID: " + txnId;
                        msg += "\n  while executing stored procedure: " + invocation.getProcName();
                        msg += "\n  from execution site: " + CoreUtils.hsIdToString(coordinatorHSId);
                        msg += "\n  Expected results: " + m_resultsForComparison[i].toString();
                        msg += "\n  Mismatched results: " + curr_results[i].toString();
                        msg += "\n  Read-only: " + new Boolean(isReadOnly).toString();
                        // die die die
                        VoltDB.crashGlobalVoltDB(msg, false, null); // kills process
                        throw new RuntimeException(msg); // gets called only by test code
                    }
                }
            }
        }
        // store these results for any future results to compare to
        else if (m_outstandingResponses > 0) {
            m_resultsForComparison = new VoltTable[currResults.length];
            // Create shallow copies of all the VoltTables to avoid
            // race conditions with the ByteBuffer metadata
            for (int i = 0; i < currResults.length; ++i)
            {
                if (currResults[i] == null) {
                    m_resultsForComparison[i] = null;
                }
                else {
                    m_resultsForComparison[i] = PrivateVoltTableFactory.createVoltTableFromBuffer(
                            currResults[i].getTableDataReference(), true);
                }
            }
        }

        // decide if it's safe to send a response to the client
        if (isReadOnly && (!m_hasSentResponse)) {
            m_hasSentResponse = true;
            return m_responseToSend;
        }
        else if ((!isReadOnly) && (m_outstandingResponses == 0)) {
            m_hasSentResponse = true;
            return m_responseToSend;
        }

        // if this is a post-send read or a pre-send write
        return null;
    }

    public ClientResponseImpl addFailedOrRecoveringResponse(long coordinatorId) {
        // verify this transaction has the right coordinator
        if (m_outstandingCoordinators != null) {
            boolean success = m_outstandingCoordinators.remove(coordinatorId);
            assert(success);
        }
        else assert(coordinatorId == firstCoordinatorId);

        // if you're out of coordinators and haven't sent a response
        if (((--m_outstandingResponses) == 0) && (!m_hasSentResponse)) {
            // this might be null...
            // if it is, you're totally hosed because that means
            // the transaction might have committed but you don't have
            // a message from the failed to coordinator to tell you if so
            m_hasSentResponse = m_responseToSend != null;
            return m_responseToSend;
        }

        // this failure hasn't messed anything up royally...
        return null;
    }

    public boolean hasSentResponse() {
        return m_hasSentResponse;
    }

    public boolean hasAllResponses() {
        return m_outstandingResponses == 0;
    }

    public boolean siteIsCoordinator(long coordinatorId) {
        // for single-partition txns
        if (m_outstandingCoordinators != null)
            return m_outstandingCoordinators.contains(coordinatorId);
        // for multi-partition txns
        return firstCoordinatorId == coordinatorId;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("IN_FLIGHT_TXN_STATE");
        sb.append("\n  TXN_ID: " + txnId);
        sb.append("\n  OUTSTANDING_COORDINATOR_IDS: ");
        for (long id : m_outstandingCoordinators)
            sb.append(id).append(" ");

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
    public final long otherSiteIds[];
    public final boolean isReadOnly;
    public final boolean isSinglePartition;
    transient public final StoredProcedureInvocation invocation;
    transient public final Object clientData;
    public final int messageSize;
    public final long initiateTime;
    public final long connectionId;
    public final String connectionHostname;
    public final boolean isAdmin;
    public final boolean allowMismatchedResults;

    // in multipartition txns, the coord id goes here
    // in single partition txns:
    //    one coord goes here
    //    if k > 0: the complete set of coords is stored
    //        in the outstandingCoordinators set
    public final long firstCoordinatorId;

    public final ArrayList<Long> coordinatorReplicas;

    protected int m_outstandingResponses = 1;

    // set to true once an answer is sent to the client
    protected boolean m_hasSentResponse = false;

    //////////////////////////////////////////////////
    // BELOW HERE USED IF single partition
    //////////////////////////////////////////////////

    // list of coordinators that have not responded
    Set<Long> m_outstandingCoordinators = null;

    // the response queued to be sent to the client
    // note, this is only needed for write transactions
    protected ClientResponseImpl m_responseToSend = null;

    //////////////////////////////////////////////////
    // BELOW HERE USED IF single partition AND k > 0
    //////////////////////////////////////////////////

    // the definitive answer to the query, used to ensure
    //  all non-recovering answers match
    protected VoltTable[] m_resultsForComparison = null;
    // the definitive hash of SQL+Params run by the proc
    protected Integer m_sqlHash = null;
}
