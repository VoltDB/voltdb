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

package org.voltdb.debugstate;

import java.io.Serializable;

public class ExecutorContext extends VoltThreadContext implements Serializable, Comparable<ExecutorContext> {
    private static final long serialVersionUID = 1287066609853828682L;

    public static class InitiatorContactHistory implements Serializable {
        private static final long serialVersionUID = 3858969909071061196L;

        public int initiatorSiteId;
        public long transactionId;
    }

    public static class ExecutorTxnState implements Serializable, Comparable<ExecutorTxnState> {
        private static final long serialVersionUID = -7004231267404046307L;

        public static class WorkUnitState implements Serializable {
            private static final long serialVersionUID = 7914755056489393739L;

            public static class DependencyState implements Serializable {
                private static final long serialVersionUID = 6057772683580065116L;

                public int dependencyId;
                public int count;
            }

            public String payload;
            public boolean shouldResume = false;
            public int outstandingDependencyCount;
            public DependencyState[] dependencies = null;
        }

        public long txnId;
        public boolean ready;
        public int initiatorSiteId;
        public int coordinatorSiteId;
        public boolean isReadOnly;
        public boolean hasWaitingResponse = false;
        public boolean hasUndoWorkUnit = false;
        public WorkUnitState[] workUnits;
        public int[] nonCoordinatingSites;
        public boolean procedureIsAborting = false;

        @Override
        public int compareTo(ExecutorTxnState o) {
            if (o == null)
                return -1;
            return (new Long(txnId)).compareTo(new Long(o.txnId));
        }
    }

    public int siteId;
    public MailboxHistory mailboxHistory;
    public ExecutorTxnState activeTransaction;
    public ExecutorTxnState[] queuedTransactions;
    public InitiatorContactHistory[] contactHistory;
    public long transactionsStarted;

    @Override
    public int compareTo(ExecutorContext o) {
        if (o == null) return -1;
        return siteId - o.siteId;
    }
}
