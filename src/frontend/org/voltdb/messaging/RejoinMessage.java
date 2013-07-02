/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableSet;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool;
import org.voltdb.utils.FixedDBBPool;

/**
 * Rejoin message used to drive the whole rejoin process. It is only sent between
 * the rejoin coordinator and local sites.
 */
public class RejoinMessage extends VoltMessage {
    public static enum Type {
        INITIATION,           // start live rejoin or join.
        INITIATION_COMMUNITY, // start community rejoin.
        INITIATION_RESPONSE,  // For IV2, the site must return the HSId of
                              // the SnapshotDataSink mailbox

        // The following are response types
        REQUEST_RESPONSE, // sent from the rejoining site to itself
        SNAPSHOT_FINISHED, // sent from a local site to the coordinator
        REPLAY_FINISHED, // sent from a local site to the coordinator

        // Elastic join specific message types
        FIRST_FRAGMENT_RECEIVED, // sent from a local site to the coordinator
    }

    private Type m_type;
    private long m_snapshotTxnId = -1; // snapshot txnId
    private long m_masterHSId = -1;
    private String m_snapshotNonce = null;
    private FixedDBBPool m_bufferPool = null;
    // number of sinks to create on the site, default is 1, elastic join may use more
    private int m_snapshotSinkCount = 1;
    private Set<Long> m_snapshotSinkHSIds = null;

    /** Empty constructor for de-serialization */
    public RejoinMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RejoinMessage(long snapshotTxnId) {
        m_subject = Subject.DEFAULT.getId();
        m_type = Type.REQUEST_RESPONSE;
        m_snapshotTxnId = snapshotTxnId;
    }

    public RejoinMessage(long sourceHSId, Type type) {
        m_sourceHSId = sourceHSId;
        m_subject = Subject.DEFAULT.getId();
        m_type = type;
    }

    /**
     * INITIATION, INITIATION_COMMUNITY pass the nonce used by the coordinator to the site.
     */
    public RejoinMessage(long sourceHSId, Type type, String snapshotNonce, FixedDBBPool bufferPool)
    {
        this(sourceHSId, type);
        assert(type == Type.INITIATION || type == Type.INITIATION_COMMUNITY);
        m_snapshotNonce = snapshotNonce;
        m_bufferPool = bufferPool;
    }

    /**
     * For elastic join, the coordinator tells the producer how many snapshot sinks to create.
     */
    public RejoinMessage(long sourceHSId, Type type, String snapshotNonce, int sinkCount,
                         FixedDBBPool bufferPool)
    {
        this(sourceHSId, type, snapshotNonce, bufferPool);
        m_snapshotSinkCount = sinkCount;
    }

    /**
     * For IV2, INITIATION_RESPONSE is used by the local site to inform the
     * Iv2RejoinCoordinator of the HSId of the SnapshotDataSink is has created
     */
    public RejoinMessage(long sourceHSId, long masterHSId, long sinkHSId)
    {
        this(sourceHSId, Type.INITIATION_RESPONSE);
        m_masterHSId = masterHSId;
        m_snapshotSinkHSIds = ImmutableSet.copyOf(Arrays.asList(sinkHSId));
    }

    /**
     * For elastic join, the producer may have multiple sinks.
     */
    public RejoinMessage(long sourceHSId, Set<Long> sinkHSIds)
    {
        this(sourceHSId, Type.INITIATION_RESPONSE);
        m_snapshotSinkHSIds = ImmutableSet.copyOf(sinkHSIds);
    }

    public Type getType() {
        return m_type;
    }

    public long getSnapshotTxnId() {
        return m_snapshotTxnId;
    }

    public String getSnapshotNonce() {
        return m_snapshotNonce;
    }

    public long getMasterHSId() {
        return m_masterHSId;
    }

    public int getSnapshotSinkCount() {
        return m_snapshotSinkCount;
    }

    public FixedDBBPool getSnapshotBufferPool()
    {
        return m_bufferPool;
    }

    /**
     * Get the only snapshot sink HSID specified in the message. This can only be called if
     * there is only one sink HSID. Rejoin uses one snapshot sink for a single site, elastic
     * join could use multiple sinks for a site. For elastic join, call
     * {@link #getSnapshotSinkHSIds()}.
     */
    public long getSnapshotSinkHSId() {
        assert m_snapshotSinkHSIds.size() == 1;
        return m_snapshotSinkHSIds.iterator().next();
    }

    public Set<Long> getSnapshotSinkHSIds() {
        return m_snapshotSinkHSIds;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize +=
            8 + // m_sourceHSId
            1 + // m_type
            8; // m_snapshotTxnId
        return msgsize;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        throw new RuntimeException("RejoinMessage: Attempted to deserialize a message which should never need it.");
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        throw new RuntimeException("RejoinMessage: Attempted to serialize a message which should never need it.");
    }
}
