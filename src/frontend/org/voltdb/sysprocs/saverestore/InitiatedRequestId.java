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

package org.voltdb.sysprocs.saverestore;

import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.SnapshotDaemon;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Optional;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.FluentIterable;

public class InitiatedRequestId implements Comparable<InitiatedRequestId>{
    public final static Pattern INITIATED_REQUEST_ID_RE =
            Pattern.compile(
                    SnapshotDaemon.RequestId.REQUEST_ID_PATTERN_PREFIX
                  + "_TXNID_(?<txnid>-?\\d+)"
                  );

    final SnapshotDaemon.RequestId m_reqId;
    final long m_txnId;

    public InitiatedRequestId(SnapshotDaemon.RequestId reqId, long txnId) {
        Preconditions.checkArgument(reqId != null, "null request id");
        m_reqId = reqId;
        m_txnId = txnId;
    }

    public SnapshotDaemon.RequestId getRequestId() {
        return m_reqId;
    }

    public long getTxnId() {
        return m_txnId;
    }

    public static InitiatedRequestId forInitiated(String reqId, long txnId) {
        Preconditions.checkArgument(
                reqId != null && !reqId.trim().isEmpty(),
                "request id is null or empty"
                );
        SnapshotDaemon.RequestId id = SnapshotDaemon.RequestId.valueOf(reqId);
        Preconditions.checkArgument(
                id != null,
                "%s is not a valid request id", reqId
                );
        Preconditions.checkArgument(
                TxnEgo.getPartitionId(txnId) == MpInitiator.MP_INIT_PID,
                "%s is not a valid multipartion transaction id", txnId
                );

        return new InitiatedRequestId(id, txnId);
    }

    public static InitiatedRequestId valueOf(String initiatedId) {
        Preconditions.checkArgument(
                initiatedId != null && !initiatedId.trim().isEmpty(),
                "initiated is null or empty"
                );
        Matcher mtc = INITIATED_REQUEST_ID_RE.matcher(initiatedId);
        Preconditions.checkArgument(
                mtc.find(),
                "%s is not a recognizable initiated request id"
                );
        SnapshotDaemon.SNAPSHOT_TYPE snapType =
                SnapshotDaemon.SNAPSHOT_TYPE.valueOf(mtc.group("type"));
        int id = Integer.parseInt(mtc.group("id"));
        long txnId = Long.parseLong(mtc.group("txnid"));

        SnapshotDaemon.RequestId reqId =
                new SnapshotDaemon.RequestId(snapType,id);

        return new InitiatedRequestId(reqId, txnId);
    }

    public static Optional<InitiatedRequestId> optionalValueOf(
            String initiatedId) {
        if (initiatedId == null || initiatedId.trim().isEmpty()) {
            return Optional.absent();
        }
        Matcher mtc = INITIATED_REQUEST_ID_RE.matcher(initiatedId);
        if (!mtc.find()) {
            return Optional.absent();
        }
        SnapshotDaemon.SNAPSHOT_TYPE snapType =
                SnapshotDaemon.SNAPSHOT_TYPE.valueOf(mtc.group("type"));
        int id = Integer.parseInt(mtc.group("id"));
        long txnId = Long.parseLong(mtc.group("txnid"));

        SnapshotDaemon.RequestId reqId =
                new SnapshotDaemon.RequestId(snapType,id);
        return Optional.of(new InitiatedRequestId(reqId, txnId));
    }

    public static Optional<InitiatedRequestId> childOf(
            String reqId, List<String> children) {
        Preconditions.checkArgument(
                reqId != null && !reqId.trim().isEmpty(),
                "request id is null or empty"
                );
        Preconditions.checkArgument(
                children != null,
                "children is null"
                );

        NavigableSet<InitiatedRequestId> found = FluentIterable.from(children)
            .filter(new StartsWithPredicate(reqId))
            .transform(toInitiatedRequestId)
            .filter(isPresent)
            .transform(peel)
            .toSortedSet(comparator);

        Optional<InitiatedRequestId> present = Optional.absent();
        if (!found.isEmpty()) {
            present = Optional.of(found.first());
        }
        return present;
    }

    static class StartsWithPredicate implements Predicate<String> {
        private final String m_prefix;
        StartsWithPredicate(String prefix) {
            m_prefix = prefix + "_TXNID_";
        }
        @Override
        public boolean apply(String node) {
            return node.startsWith(m_prefix);
        }
    }

    final static Comparator<InitiatedRequestId> comparator =
            new Comparator<InitiatedRequestId>() {
                @Override
                public int compare(InitiatedRequestId o1, InitiatedRequestId o2) {
                    return o1.compareTo(o2);
                }
    };

    final static Predicate<Optional<InitiatedRequestId>> isPresent =
            new Predicate<Optional<InitiatedRequestId>>() {
                @Override
                public boolean apply(Optional<InitiatedRequestId> input) {
                    return input.isPresent();
                }
    };

    final static Function<String, Optional<InitiatedRequestId>> toInitiatedRequestId =
            new Function<String, Optional<InitiatedRequestId>>() {
                @Override
                public Optional<InitiatedRequestId> apply(String initiatedId) {
                    return InitiatedRequestId.optionalValueOf(initiatedId);
                }
    };

    final static Function<Optional<InitiatedRequestId>,InitiatedRequestId> peel =
            new Function<Optional<InitiatedRequestId>,InitiatedRequestId>() {
                @Override
                public InitiatedRequestId apply(Optional<InitiatedRequestId> input) {
                    return input.get();
                }
    };

    @Override
    public int compareTo(InitiatedRequestId o) {
        int cmp = Long.valueOf(m_txnId).compareTo(o.m_txnId);
        if (cmp == 0) {
            cmp = o.m_reqId.compareTo(m_reqId);
        }
        return cmp;
    }

    @Override
    public String toString() {
        return String.format("%s_TXNID_%d", m_reqId.toString(), m_txnId);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_reqId == null) ? 0 : m_reqId.hashCode());
        result = prime * result + (int) (m_txnId ^ (m_txnId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InitiatedRequestId other = (InitiatedRequestId) obj;
        if (m_reqId == null) {
            if (other.m_reqId != null)
                return false;
        } else if (!m_reqId.equals(other.m_reqId))
            return false;
        if (m_txnId != other.m_txnId)
            return false;
        return true;
    }
}
