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

package org.voltdb;

import groovy.json.JsonException;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.EventType;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.SiteMailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.utils.RateLimitedLogger;
import org.voltcore.zk.SynchronizedStatesManager;
import org.voltcore.zk.SynchronizedStatesManager.StateMachineInstance;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZKUtil.StringCallback;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.messaging.SnapshotCheckRequestMessage;
import org.voltdb.messaging.SnapshotCheckResponseMessage;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.SusceptibleRunnable;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.util.concurrent.Callables;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningScheduledExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

/**
 * A scheduler of automated snapshots and manager of archived and retained snapshots.
 * The new functionality for handling truncation snapshots operates separately from
 * the old automated snapshots. They just share the same event processing threads. Future work
 * should merge them.
 *
 * Note: comments that start with TRAIL [TruncSnap] TruncationSnapshot legend
 *       are way to document code trails which may be perused via the code trails
 *       eclipse plugin https://marketplace.eclipse.org/content/code-trails
 */
public class SnapshotDaemon implements SnapshotCompletionInterest {

    private class TruncationSnapshotAttempt {
        private String path;
        private String nonce;
        private boolean finished;
    }

    static enum SNAPSHOT_TYPE {

        STREAM(SnapshotFormat.STREAM),
        INDEX(SnapshotFormat.INDEX),
        FILE(SnapshotFormat.NATIVE),
        CSV(SnapshotFormat.CSV),
        LOG(SnapshotFormat.NATIVE),            // Truncation Request from Command log (occupies the same slot as FILE)
        EMPTY(null);

        final static private Map<SnapshotFormat,SNAPSHOT_TYPE> formatMap;
        final static private EnumSet<SNAPSHOT_TYPE> defaultTypes =
                EnumSet.<SNAPSHOT_TYPE>of(FILE,LOG);

        static {
            formatMap = new EnumMap<>(SnapshotFormat.class);
            formatMap.put(SnapshotFormat.STREAM, STREAM);
            formatMap.put(SnapshotFormat.INDEX, INDEX);
            formatMap.put(SnapshotFormat.NATIVE, FILE);
            formatMap.put(SnapshotFormat.CSV, CSV);
        }

        final SnapshotFormat m_format;

        SNAPSHOT_TYPE(SnapshotFormat format) {
            m_format = format;
        }

        public RequestId requestId(int id) {
            return new RequestId(this,id);
        }

        public SnapshotFormat format() {
            return m_format;
        }

        public boolean isDefaultType() {
            return defaultTypes.contains(this);
        }

        public static SNAPSHOT_TYPE valueOf(SnapshotFormat format) {
            return formatMap.get(format);
        }
    }

    public final static class RequestId implements Comparable<RequestId>{
        final int m_id;
        final SNAPSHOT_TYPE m_type;

        final static Pattern REQUEST_ID_RE = Pattern
                .compile("(?<type>STREAM|INDEX|FILE|CSV|LOG)_SR_(?<id>\\d+)");

        public RequestId(SNAPSHOT_TYPE type, int id) {
            Preconditions.checkArgument(type != null, "type is null");
            m_type = type;
            m_id = id;
        }

        public static RequestId valueOf(String reqId) {
            if (reqId == null || reqId.trim().isEmpty()) {
                return null;
            }
            Matcher mtc = REQUEST_ID_RE.matcher(reqId);
            if (!mtc.find()) return null;

            return new RequestId(
                    SNAPSHOT_TYPE.valueOf(mtc.group("type")),
                    Integer.parseInt(mtc.group("id"))
                    );
        }

        public int getId() {
            return m_id;
        }

        public SNAPSHOT_TYPE getType() {
            return m_type;
        }

        public String name() {
            return String.format("%s_SR_%010d", m_type.name(), m_id);
        }

        @Override
        public String toString() {
            return name();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + m_id;
            result = prime * result
                    + ((m_type == null) ? 0 : m_type.hashCode());
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
            RequestId other = (RequestId) obj;
            if (m_id != other.m_id)
                return false;
            if (m_type != other.m_type)
                return false;
            return true;
        }

        @Override
        public int compareTo(RequestId o) {
            int cmp = this.m_type.ordinal() - o.m_type.ordinal();
            if (cmp == 0) {
                cmp = this.m_id - o.m_id;
            }
            return cmp;
        }
    }

    private static final int m_queueSlots = SNAPSHOT_TYPE.values().length - 2;

    private class SnapshotQueue extends StateMachineInstance {
        // Unused required abstracts
        @Override
        protected void taskRequested(ByteBuffer proposedTask) {}
        @Override
        protected void staleTaskRequestNotification(ByteBuffer proposedTask) {}
        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results) {}
        @Override
        protected void membershipChanged(Set<String> addedMembers, Set<String> removedMembers) {}

        /*
         *  Layout of queue:
         *        1 byte of currently active snapshot category
         *        4 byte ordering of pending snapshot categories for round robin processing
         *        n bytes of active snapshot data
         */
        private final String m_streamSnapshotPrefix;
        private final String m_indexSnapshotPrefix;
        private final String m_fileSnapshotPrefix;
        private final String m_csvSnapshotPrefix;
        private Runnable m_processQueueTask = null;
        private Runnable m_processSnapshotCompleteTask = null;
        private Runnable m_leaderQueueProcessor = null;
        private SNAPSHOT_TYPE m_activeSnapshot = SNAPSHOT_TYPE.EMPTY;
        // Can contain either FILE or LOG but not both; FILE can be promoted to LOG
        private final LinkedHashSet<SNAPSHOT_TYPE> m_pendingSnapshotsQueue = new LinkedHashSet<SNAPSHOT_TYPE>();
        private final LinkedHashSet<SNAPSHOT_TYPE> m_localPendingSnapshots = new LinkedHashSet<SNAPSHOT_TYPE>();
        private final byte[] m_startQueue = new byte[] {(byte)SNAPSHOT_TYPE.EMPTY.ordinal(),
            (byte)SNAPSHOT_TYPE.EMPTY.ordinal(), (byte)SNAPSHOT_TYPE.EMPTY.ordinal(),
            (byte)SNAPSHOT_TYPE.EMPTY.ordinal(), (byte)SNAPSHOT_TYPE.EMPTY.ordinal(), 0};


        public SnapshotQueue(SynchronizedStatesManager ssm) throws KeeperException, InterruptedException {
            ssm.super("snapshot_queue", SNAP_LOG);
            m_leaderQueueProcessor = null;

            // Start with an empty queue
            registerStateMachineWithManager(ByteBuffer.wrap(m_startQueue));
            m_streamSnapshotPrefix = ZKUtil.joinZKPath(m_statePath, "stream_snapshots");
            ssm.addIfMissing(m_streamSnapshotPrefix, CreateMode.PERSISTENT, null);
            m_indexSnapshotPrefix = ZKUtil.joinZKPath(m_statePath, "index_snapshots");
            ssm.addIfMissing(m_indexSnapshotPrefix, CreateMode.PERSISTENT, null);
            m_fileSnapshotPrefix = ZKUtil.joinZKPath(m_statePath, "file_snapshots");
            ssm.addIfMissing(m_fileSnapshotPrefix, CreateMode.PERSISTENT, null);
            m_csvSnapshotPrefix = ZKUtil.joinZKPath(m_statePath, "csv_snapshots");
            ssm.addIfMissing(m_csvSnapshotPrefix, CreateMode.PERSISTENT, null);
        }

        @Override
        public String stateToString(ByteBuffer state) {
            if (state == null || state.limit() < m_queueSlots+1) {
                return "state is FUBAR";
            }
            ByteBuffer bb = state.asReadOnlyBuffer();

            final SNAPSHOT_TYPE [] types = SNAPSHOT_TYPE.values();
            StringBuilder sb = new StringBuilder(512);

            byte slot = bb.get();
            sb.append("SnapshotQueue: { Active: ").append(types[slot]);

            sb.append(", Queued: [ ");
            for (int i = 0; i < m_queueSlots; ++i) {
                slot = bb.get();
                if (i>0) sb.append(", ");
                sb.append(types[slot]);
            }
            sb.append("]");
            if (bb.hasRemaining() && bb.remaining() > 1) {
                sb.append(", activeNode: ");
                byte [] nb = new byte[bb.remaining()];
                bb.get(nb);
                String activeNode = new String(nb, StandardCharsets.UTF_8);
                sb.append(activeNode);
            }
            return sb.append(" }").toString();
        }

        private String getPathFromType(SNAPSHOT_TYPE type) {
            String path = null;
            switch (type) {
            case STREAM:
                path = m_streamSnapshotPrefix;
                break;
            case INDEX:
                path = m_indexSnapshotPrefix;
                break;
            case FILE:
            case LOG:
                path = m_fileSnapshotPrefix;
                break;
            case CSV:
                path = m_csvSnapshotPrefix;
                break;
            default:
                break;
            }
            return path;
        }

        private int buildSnapshotNode(String snapshotDirPrefix, byte[] nodeData)
                throws KeeperException, InterruptedException {
            String nodePath;
            nodePath = m_zk.create(snapshotDirPrefix + "/", nodeData,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            String nodeNum = nodePath.substring(snapshotDirPrefix.length()+1);
            return Integer.valueOf(nodeNum);
        }

        public void updateActiveNodeData(int nodeId, byte [] nodeData)
                throws KeeperException, InterruptedException {
            String activePath = getPathFromType(m_activeSnapshot);
            m_zk.setData(getPathFromNodeId(activePath, nodeId), nodeData, -1);
            ByteBuffer newQueue = buildProposalFromUpdatedQueue(ByteBuffer.wrap(nodeData));
            proposeStateChange(newQueue);
        }

        public void coalesceNodesWith(
                SNAPSHOT_TYPE stype,
                List<Integer> removed,
                byte[] withNodeData, int intoNodeId
                )
                throws KeeperException, InterruptedException
        {
            String activePath = getPathFromType(stype);
            ZKUtil.StatCallback datacb = new ZKUtil.StatCallback();
            m_zk.setData(getPathFromNodeId(activePath, intoNodeId), withNodeData, -1, datacb, null);
            if (removed == null || removed.isEmpty()) {
                datacb.get();
            } else {
                ZKUtil.VoidCallback deletecb = null;
                for (int nodeId: removed) {
                    deletecb = new ZKUtil.VoidCallback();
                    m_zk.delete(getPathFromNodeId(activePath, nodeId), -1, deletecb, null);
                }
                deletecb.get();
            }
        }

        private void deleteSnapshotNode(String snapshotDirPrefix, int nodeId) {
            final String path = getPathFromNodeId(snapshotDirPrefix, nodeId);
            try {
                m_zk.delete(path, -1);
            }
            catch (InterruptedException|KeeperException e) {
               SNAP_LOG.error("could not delete zk node " + path, e);
            }
        }

        private String getPathFromNodeId(String snapshotDirPrefix, int nodeId) {
            return ZKUtil.joinZKPath(snapshotDirPrefix, String.format("%010d", nodeId));
        }

        private byte[] getDetailsFromSnapshotNode(String snapshotDirPrefix, int nodeId)
                throws KeeperException, InterruptedException {
            return m_zk.getData(getPathFromNodeId(snapshotDirPrefix, nodeId), false, null);
        }

        private ByteBuffer buildProposalFromUpdatedQueue(ByteBuffer activeSnapshotdata) {
            ByteBuffer newQueue = null;
            newQueue = ByteBuffer.allocate(1 + m_queueSlots + activeSnapshotdata.remaining());
            newQueue.put((byte) m_activeSnapshot.ordinal());   // snapshot in progress
            for (SNAPSHOT_TYPE slot : m_pendingSnapshotsQueue) {
                newQueue.put((byte) slot.ordinal());
            }
            for (int ii=0; ii<m_queueSlots-m_pendingSnapshotsQueue.size(); ii++) {
                newQueue.put((byte) SNAPSHOT_TYPE.EMPTY.ordinal());
            }
            // append nodes list in process with current snapshot
            newQueue.put(activeSnapshotdata);
            newQueue.flip();
            return newQueue;
        }

        private void processCurrentQueueState(ByteBuffer currentAgreedState) {
            m_activeSnapshot = SNAPSHOT_TYPE.values()[currentAgreedState.get()];
            for (int ii=0; ii<m_queueSlots; ii++) {
                m_pendingSnapshotsQueue.add(SNAPSHOT_TYPE.values()[currentAgreedState.get()]);
            }
            m_pendingSnapshotsQueue.remove(SNAPSHOT_TYPE.EMPTY);
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState)
        {
            processCurrentQueueState(currentAgreedState);
        }

        @Override
        protected void lockRequestCompleted() {
            ByteBuffer newQueue;
            ByteBuffer activeNodeData;
            boolean queueChanged = false;
            for (SNAPSHOT_TYPE snapshotReq : m_localPendingSnapshots) {
                if (snapshotReq == SNAPSHOT_TYPE.FILE && m_pendingSnapshotsQueue.contains(SNAPSHOT_TYPE.LOG)) {
                    // We already have a CommandLog request in the queue
                    break;
                }
                if (!m_pendingSnapshotsQueue.contains(snapshotReq)) {
                    if (snapshotReq == SNAPSHOT_TYPE.LOG) {
                        // Promote FILE entry to LOG entry (if it is there; remove could fail)
                        m_pendingSnapshotsQueue.remove(SNAPSHOT_TYPE.FILE);
                    }
                    m_pendingSnapshotsQueue.add(snapshotReq);
                    queueChanged = true;
                }
            }
            m_localPendingSnapshots.clear();
            if (m_bIsSnapshotDaemonLeader) {
                if (m_processSnapshotCompleteTask != null) {
                    // We are the leader and we finished the last snapshot so start the next one in the queue
                    try {
                        m_processSnapshotCompleteTask.run();
                        m_processSnapshotCompleteTask = null;
                        m_processQueueTask = null;
                        if (isProposalInProgress()) {
                            // The task updated the queue for us
                            queueChanged = false;
                        }
                    }
                    catch (Exception e) {
                        if (isProposalInProgress()) {
                            // Proposal was started so don't do
                            queueChanged = false;
                        }
                    }
                }
                else
                if (m_processQueueTask != null) {
                    // We are the leader and we finished the last snapshot so start the next one in the queue
                    try {
                        m_processQueueTask.run();
                        m_processQueueTask = null;
                        if (isProposalInProgress()) {
                            // The task updated the queue for us
                            queueChanged = false;
                        }
                    }
                    catch (Exception e) {
                        if (isProposalInProgress()) {
                            // Proposal was started so don't do
                            queueChanged = false;
                        }
                    }
                }
            }

            if (queueChanged) {
                activeNodeData = getCurrentState();
                activeNodeData.position(activeNodeData.position()+m_queueSlots+1);
                newQueue = buildProposalFromUpdatedQueue(activeNodeData);
                newQueue.flip();
                proposeStateChange(newQueue);
            }
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState) {
            // Accept all queue change requests
            requestedStateChangeAcceptable(true);
        }

        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
            assert(success);
            if (!ourProposal) {
                // Since all changes are always successful, they were applied when we proposed the change
                processCurrentQueueState(proposedState);
            }

            if (m_bIsSnapshotDaemonLeader && m_activeSnapshot == SNAPSHOT_TYPE.EMPTY && !m_pendingSnapshotsQueue.isEmpty()) {
                try {
                    lockDistributedLockAndProcessQueue();
                }
                catch (Exception e) {
                    //  Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        public int requestSnapshot(SNAPSHOT_TYPE reqType, byte[] snapshotDetails) throws KeeperException, InterruptedException {
            int nodeId = buildSnapshotNode(getPathFromType(reqType), snapshotDetails);

            if (m_localPendingSnapshots.size() == 0 && requestLock()) {
                if (reqType == SNAPSHOT_TYPE.FILE && m_pendingSnapshotsQueue.contains(SNAPSHOT_TYPE.LOG)) {
                    // We already have a CommandLog request in the queue
                    return nodeId;
                }

                if (!m_pendingSnapshotsQueue.contains(reqType)) {
                    if (reqType == SNAPSHOT_TYPE.LOG) {
                        // Promote FILE entry to LOG entry (if it is there; remove could fail)
                        m_pendingSnapshotsQueue.remove(SNAPSHOT_TYPE.FILE);
                    }
                    m_pendingSnapshotsQueue.add(reqType);
                    ByteBuffer activeNodeData = getCurrentState();
                    activeNodeData.position(activeNodeData.position()+m_queueSlots+1);
                    ByteBuffer newQueue = buildProposalFromUpdatedQueue(activeNodeData);
                    proposeStateChange(newQueue);
                }
            }
            else {
                m_localPendingSnapshots.add(reqType);
            }
            return nodeId;
        }

        public SNAPSHOT_TYPE getPendingSnapshotType() {
            SNAPSHOT_TYPE pending = SNAPSHOT_TYPE.EMPTY;
            if (!m_pendingSnapshotsQueue.isEmpty()) {
                pending = m_pendingSnapshotsQueue.iterator().next();
            }
            return pending;
        }

        public void lockDistributedLockAndProcessQueue() throws Exception {
            if (requestLock()) {
                m_processQueueChangeUnderDistributedLock.run();
            }
            else {
                m_processQueueTask = m_processQueueChangeUnderDistributedLock;
            }
        }

        public void lockDistributedLockAndProcessCompletedSnapshot() throws Exception {
            if (requestLock()) {
                m_processSnapshotCompleteUnderDistributedLock.run();
            }
            else {
                m_processSnapshotCompleteTask = m_processSnapshotCompleteUnderDistributedLock;
            }
        }

        public void cancelLockedTask() {
            cancelLockRequest();
        }

        public ArrayList<Integer> getNodeListFor(SNAPSHOT_TYPE type) {
            String path = getPathFromType(type);
            assert(path != null);
            ArrayList<Integer> nodeList = new ArrayList<Integer>();
            List<String> nodeNames;
            try {
                nodeNames = m_zk.getChildren(path, false);
            }
            catch (KeeperException | InterruptedException e) {
                nodeNames = new ArrayList<String>();
            }
            for (String nodeName : nodeNames) {
                nodeList.add(Integer.valueOf(nodeName));
            }
            return nodeList;
        }

        public NavigableMap<Integer, ByteBuffer> getDetailsFromSnapshotNodeByType(SNAPSHOT_TYPE type) {
            byte[] nodeData = null;
            String path = getPathFromType(type);
            assert(path != null);
            Map<Integer, ByteBuffer> results = new HashMap<Integer, ByteBuffer>();
            try {
                List<String> nodeNames = m_zk.getChildren(path, false);
                for (String nodeName : nodeNames) {
                    Integer nodeId = Integer.valueOf(nodeName);
                    nodeData = getDetailsFromSnapshotNode(path, nodeId);
                    results.put(nodeId, ByteBuffer.wrap(nodeData));
                }
            }
            catch (KeeperException | InterruptedException e) {
               SNAP_LOG.error("failed to get request details for " + type, e);
            }
            return ImmutableSortedMap.copyOf(results);
        }

        public void removeRequestIdNode(String strRequestId) {
            RequestId reqId = RequestId.valueOf(strRequestId);
            deleteSnapshotNode(getPathFromType(reqId.getType()), reqId.getId());
        }

        public void removeActiveSnapshotNodeList(List<Integer> satisfyingNodes) {
            String activePath = getPathFromType(m_activeSnapshot);
            for (Integer nodeId : satisfyingNodes) {
                deleteSnapshotNode(activePath, nodeId);
            }
        }

        public void lastSnapshotProcessed() {
            assert(m_pendingSnapshotsQueue.isEmpty());
            m_activeSnapshot = SNAPSHOT_TYPE.EMPTY;
            ByteBuffer newQueue = buildProposalFromUpdatedQueue(ByteBuffer.wrap(new byte[0]));
            proposeStateChange(newQueue);
        }

        public void initiateNewSnapshot(SNAPSHOT_TYPE type, boolean satisfiesAllNodes, ByteBuffer activeSnapshotData) {
            assert(holdingDistributedLock());
            m_pendingSnapshotsQueue.remove(type);
            if (!satisfiesAllNodes) {
                // re-queue at the tail
                m_pendingSnapshotsQueue.add(type);
            }
            m_activeSnapshot = type;
            ByteBuffer newQueue = buildProposalFromUpdatedQueue(activeSnapshotData);
            proposeStateChange(newQueue);
        }

        public boolean snapshotPending(SNAPSHOT_TYPE type) {
            return m_pendingSnapshotsQueue.contains(type);
        }

        public SNAPSHOT_TYPE getActiveSnapshotType() {
            return m_activeSnapshot;
        }

        public byte [] getActiveNode() {
            if (m_activeSnapshot == SNAPSHOT_TYPE.EMPTY) {
                return new byte[0];
            }

            ByteBuffer activeState = getCurrentState();
            activeState.position(1+m_queueSlots);
            byte [] activeNode = new byte[activeState.remaining()];
            activeState.get(activeNode);
            return activeNode;
        }
     }

    static int m_periodicWorkInterval = 2000;
    public static volatile int m_userSnapshotRetryInterval = 30;

    private final Runnable m_processQueueChangeUnderDistributedLock = new SusceptibleRunnable() {
        @Override
        public void susceptibleRun() throws Exception {
            assert(m_snapshotQueue.getActiveSnapshotType() == SNAPSHOT_TYPE.EMPTY);
            SNAPSHOT_TYPE atHeadType = m_snapshotQueue.getPendingSnapshotType();
            if (atHeadType == SNAPSHOT_TYPE.EMPTY) {
                m_snapshotQueue.lastSnapshotProcessed();
                return;
            }
            NavigableMap<Integer,ByteBuffer> requestNodes =
                    m_snapshotQueue.getDetailsFromSnapshotNodeByType(atHeadType);

            int nodesCount = requestNodes.size();

            ByteBuffer first = requestNodes.firstEntry().getValue();
            if (first == null || first.limit() == 0) {
                VoltDB.crashLocalVoltDB("snapshot request has no content", true, null);
            }

            byte [] candidate = first.array();
            if (atHeadType.isDefaultType()) {
                candidate = coalesceDefaultSnapshot(requestNodes);
                nodesCount = 1;
            }

            JSONObject joRequest = new JSONObject(new String(candidate, StandardCharsets.UTF_8));
            String requestId = joRequest.optString(
                    "coalescedToRequestId",
                    atHeadType.requestId(requestNodes.firstKey()).name()
                    );
            joRequest.put("requestIdAtHead", requestId);
            candidate = joRequest.toString(4).getBytes(StandardCharsets.UTF_8);

            m_snapshotQueue.initiateNewSnapshot(atHeadType, nodesCount == 1, ByteBuffer.wrap(candidate));

            if (joRequest.optBoolean("isTruncation", false)) {
                truncationSnapshotPrelude(joRequest);
            } else {
                processUserSnapshotPrelude(joRequest);
            }
        }
    };

    private final Runnable m_processSnapshotCompleteUnderDistributedLock = new SusceptibleRunnable() {
        @Override
        public void susceptibleRun() throws Exception {
            assert(m_snapshotQueue.getActiveSnapshotType() != SNAPSHOT_TYPE.EMPTY);
            m_snapshotQueue.m_activeSnapshot = SNAPSHOT_TYPE.EMPTY;
            m_processQueueChangeUnderDistributedLock.run();
        }
    };

    public boolean isTruncationSnapshotQueuedOrActive() {
        return m_snapshotQueue.snapshotPending(SNAPSHOT_TYPE.LOG)
            || m_snapshotQueue.getActiveSnapshotType() == SNAPSHOT_TYPE.LOG;
    }

    public JSONObject getActiveSnapshot() throws JSONException {
        if (m_snapshotQueue.getActiveSnapshotType() == SNAPSHOT_TYPE.EMPTY) {
            return null;
        }
        byte [] activeNode = m_snapshotQueue.getActiveNode();
        String jsonString = new String(activeNode,StandardCharsets.UTF_8);
        return new JSONObject(jsonString);
    }

    /*
     * Something that initiates procedures for the snapshot daemon.
     */
    public interface DaemonInitiator {
        public void initiateSnapshotDaemonWork(final String procedureName, long clientData, Object params[]);
    }

    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");
    private static final VoltLogger loggingLog = new VoltLogger("LOGGING");
    private final ScheduledThreadPoolExecutor m_esBase =
            new ScheduledThreadPoolExecutor(1,
                    CoreUtils.getThreadFactory(null, "SnapshotDaemon", CoreUtils.SMALL_STACK_SIZE, false, null),
                                            new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());
    private final ListeningScheduledExecutorService m_es = MoreExecutors.listeningDecorator(m_esBase);

    private ZooKeeper m_zk;
    private DaemonInitiator m_initiator;
    private long m_nextCallbackHandle;
    private String m_truncationSnapshotPath;
    private boolean m_bIsSnapshotDaemonLeader = false;

    /*
     * Before doing truncation snapshot operations, wait a few seconds
     * to give a few nodes a chance to get into the same state WRT to truncation
     * so that a truncation snapshot will can service multiple truncation requests
     * that arrive at the same time.
     */
    int m_truncationGatheringPeriod = 10;

    private final TreeMap<Long, TruncationSnapshotAttempt> m_truncationSnapshotAttempts =
        new TreeMap<Long, TruncationSnapshotAttempt>();
    private Future<?> m_truncationSnapshotScanTask;

    private TimeUnit m_frequencyUnit;
    private long m_frequencyInMillis;
    private int m_frequency;
    private int m_retain;
    private String m_path;
    private String m_prefix;
    private String m_prefixAndSeparator;

    private SnapshotSchedule m_lastKnownSchedule = null;

    private final HashMap<Long, ProcedureCallback> m_procedureCallbacks = new HashMap<Long, ProcedureCallback>();

    private final SimpleDateFormat m_dateFormat = new SimpleDateFormat("'_'yyyy.MM.dd.HH.mm.ss");

    // true if this SnapshotDaemon is the one responsible for generating
    // snapshots
    private boolean m_isAutoSnapshotLeader = false;
    private Future<?> m_autoSnapshotTask = null;
    private long m_nextSnapshotTime;

    /**
     * Don't invoke sysprocs too close together.
     * Keep track of the last call and only do it after
     * enough time has passed.
     */
    private long m_lastSysprocInvocation = System.currentTimeMillis();
    static long m_minTimeBetweenSysprocs = 3000;
    private SynchronizedStatesManager m_perHostStateManager;
    private SnapshotQueue m_snapshotQueue;

    /**
     * List of snapshots on disk sorted by creation time
     */
    final LinkedList<Snapshot> m_snapshots = new LinkedList<Snapshot>();

    /**
     * States the daemon can be in
     *
     */
    enum State {
        /*
         * Initial state
         */
        STARTUP,
        /*
         * Invoked @SnapshotScan, waiting for results.
         * Done once on startup to find number of snapshots on disk
         * at path with prefix
         */
        SCANNING,
        /*
         * Waiting in between snapshots
         */
        WAITING,
        /*
         * Deleting snapshots that are no longer going to be retained.
         */
        DELETING,
        /*
         * Initiated a snapshot. Will call snapshot scan occasionally to find out
         * when it completes.
         */
        SNAPSHOTTING,
    }

    private State m_state = State.STARTUP;

    SnapshotDaemon() {
        m_esBase.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        m_esBase.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        m_frequencyUnit = null;
        m_retain = 0;
        m_frequency = 0;
        m_frequencyInMillis = 0;
        m_prefix = null;
        m_path = null;
        m_prefixAndSeparator = null;

        // Register the snapshot status to the StatsAgent
        SnapshotStatus snapshotStatus = new SnapshotStatus();
        VoltDB.instance().getStatsAgent().registerStatsSource(StatsSelector.SNAPSHOTSTATUS,
                                                              0,
                                                              snapshotStatus);
        VoltDB.instance().getSnapshotCompletionMonitor().addInterest(this);
    }

    public void init(DaemonInitiator initiator, HostMessenger messenger, Runnable threadLocalInit, GlobalServiceElector gse) {
        m_initiator = initiator;
        m_zk = messenger.getZK();
        m_mb = new SiteMailbox(messenger, messenger.getHSIdForLocalSite(HostMessenger.SNAPSHOT_DAEMON_ID));
        messenger.createMailbox(m_mb.getHSId(), m_mb);

        try {
            m_zk.create(VoltZK.nodes_currently_snapshotting, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {}
        try {
            m_zk.create(VoltZK.completed_snapshots, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {}

        String hostId = "HOST_" + Integer.toString(messenger.getHostId());
        try {
            m_perHostStateManager = new SynchronizedStatesManager(m_zk, "PER_HOST_MANAGER", hostId, 1);
        }
        catch (Exception e) {}
        try {
            m_snapshotQueue = new SnapshotQueue(m_perHostStateManager);
        }
        catch (Exception e) {}

        if (threadLocalInit != null) {
            m_es.execute(threadLocalInit);
        }

        /*
         *  Really shouldn't leak this from a constructor, and twice to boot
         *  If IV2 is enabled leader election for the snapshot daemon is always tied to
         *  leader election for the MP coordinator so that they can't be partitioned
         *  from each other.
         */
        if (gse == null) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    leaderElection();
                }
            });
        } else {
            gse.registerService(new Promotable() {
                @Override
                public void acceptPromotion() throws InterruptedException,
                        ExecutionException, KeeperException {
                    m_es.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                m_isAutoSnapshotLeader = true;
                                if (m_lastKnownSchedule != null) {
                                    makeActivePrivate(m_lastKnownSchedule);
                                }
                                electedTruncationLeader();
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB("Exception in snapshot daemon electing master via ZK", true, e);
                            }
                        }
                    });
                }

            });
        }
    }

    private static final long INITIATION_RESPONSE_TIMEOUT_MS = 20 * 60 * 1000;
    // Tracks when the last @SnapshotSave call was issued.
    // Prevents two @SnapshotSave calls being issued back to back.
    // This is reset when a response is received for the initiation.
    private Pair<Long, Boolean> m_lastInitiationTs = null;
    private Mailbox m_mb;
    private void initiateSnapshotSave(final long handle, final Object params[], boolean blocking)
    {
        boolean success = true;
        VoltTable checkResult = SnapshotUtil.constructNodeResultsTable();
        VoltTable hardlinkCheckResults = SnapshotUtil.constructHardlinkResultsTable();
        final String jsString = String.class.cast(params[0]);

        if (m_lastInitiationTs != null) {
            final long elapsedMs = System.currentTimeMillis() - m_lastInitiationTs.getFirst();
            // Blocking snapshot may take a long time to finish, don't time it out if it's blocking
            if (!m_lastInitiationTs.getSecond() && elapsedMs > INITIATION_RESPONSE_TIMEOUT_MS) {
                SNAP_LOG.warn(String.format("A snapshot was initiated %d minutes ago and hasn't received a response yet.",
                        TimeUnit.MILLISECONDS.toMinutes(elapsedMs)));
                m_lastInitiationTs = null;
            } else {
                checkResult.addRow(CoreUtils.getHostIdFromHSId(m_mb.getHSId()), CoreUtils.getHostnameOrAddress(), null,
                        "FAILURE", "SNAPSHOT IN PROGRESS");
                success = false;
            }
        }

        if (success) {
            try {
                final JSONObject jsObj = new JSONObject(jsString);
                boolean initiateSnapshot;

                // Do scan work on all known live hosts
                VoltMessage msg = new SnapshotCheckRequestMessage(jsString);
                List<Integer> liveHosts = VoltDB.instance().getHostMessenger().getLiveHostIds();
                for (int hostId : liveHosts) {
                    m_mb.send(CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.SNAPSHOT_IO_AGENT_ID), msg);
                }

                // Wait for responses from all hosts for a certain amount of time
                Map<Integer, VoltTable> responses = Maps.newHashMap();
                Map<Integer, VoltTable> hardlinkChecks = Maps.newHashMap();
                final long timeoutMs = 10 * 1000; // 10s timeout
                final long endTime = System.currentTimeMillis() + timeoutMs;
                SnapshotCheckResponseMessage response;
                // TRAIL [ImplementationHints:3] need to do feasibility tests for hard links
                while ((response = (SnapshotCheckResponseMessage) m_mb.recvBlocking(timeoutMs)) != null) {
                    // ignore responses to previous requests
                    if (jsObj.getString("path").equals(response.getPath()) &&
                        jsObj.getString("nonce").equals(response.getNonce())) {

                        int hostid = CoreUtils.getHostIdFromHSId(response.m_sourceHSId);
                        responses.put(hostid, response.getResponse());
                        VoltTable hardlinkCheck = response.getHardLinkResults();
                        if (hardlinkCheck != null) {
                            hardlinkChecks.put(hostid, hardlinkCheck);
                        }
                    }

                    if (responses.size() == liveHosts.size() || System.currentTimeMillis() > endTime) {
                        break;
                    }
                }

                if (responses.size() != liveHosts.size()) {
                    checkResult.addRow(CoreUtils.getHostIdFromHSId(m_mb.getHSId()), CoreUtils.getHostnameOrAddress(), null,
                            "FAILURE", "TIMED OUT CHECKING SNAPSHOT FEASIBILITY");
                    success = false;
                }

                if (success) {
                    // TRAIL [TruncSnap:12] all participating nodes have initiated successfully
                    // Call @SnapshotSave if check passed, return the failure otherwise
                    checkResult = VoltTableUtil.unionTables(responses.values());
                    if (hardlinkChecks.size() > 0) {
                        hardlinkCheckResults = VoltTableUtil.unionTables(hardlinkChecks.values());
                    }
                    initiateSnapshot = SnapshotUtil.didSnapshotRequestSucceed(new VoltTable[]{checkResult});

                    Map<String, ClientResponseImpl> badLinks =
                            SnapshotUtil.checkHardLinkTestResponses(new VoltTable[]{ hardlinkCheckResults });
                    if (badLinks.size() > 0) try {
                        JSONObject hardLinks = jsObj.getJSONObject("hardLinks");
                        String requestid = jsObj.getString("coalescedToRequestId");
                        for (Map.Entry<String, ClientResponseImpl> e: badLinks.entrySet()) {
                            hardLinks.remove(e.getKey());
                            saveResponseToZK(e.getKey(), e.getValue());
                        }
                        m_snapshotQueue.updateActiveNodeData(
                                RequestId.valueOf(requestid).getId(),
                                jsObj.toString(4).getBytes(StandardCharsets.UTF_8)
                                );
                    } catch (InterruptedException|KeeperException ex) {
                        SNAP_LOG.error("unable to save hardlink failure to zk", ex);
                        initiateSnapshot = success = false;
                        checkResult.addRow(
                                CoreUtils.getHostIdFromHSId(m_mb.getHSId()),
                                CoreUtils.getHostnameOrAddress(), null,
                                "FAILURE", "ERROR SAVING HARDLINK FAILURE TO ZK"
                                );
                    }
                    if (initiateSnapshot) {
                        m_lastInitiationTs = Pair.of(System.currentTimeMillis(), blocking);
                        m_initiator.initiateSnapshotDaemonWork("@SnapshotSave", handle, params);
                    } else {
                        success = false;
                    }
                }
            } catch (JSONException e) {
                success = false;
                checkResult.addRow(CoreUtils.getHostIdFromHSId(m_mb.getHSId()), CoreUtils.getHostnameOrAddress(), null, "FAILURE", "ERROR PARSING JSON");
                SNAP_LOG.warn("Error parsing JSON string: " + jsString, e);
            }
        }

        if (!success) {
            final ClientResponseImpl failureResponse =
                    new ClientResponseImpl(ClientResponseImpl.SUCCESS, new VoltTable[]{checkResult}, null);
            failureResponse.setClientHandle(handle);
            processClientResponse(Callables.returning(failureResponse));
        }
    }

    private void saveResponseToZKAndReset(String requestId, ClientResponseImpl response) throws Exception
    {
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize());
        m_zk.create(VoltZK.user_snapshot_response + requestId,
                response.flattenToBuffer(buf).array(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        userSnapshotRequestExistenceCheck(true);
    }

    private void saveResponseToZK(JSONObject jo, ClientResponseImpl response)
               throws JSONException, KeeperException, InterruptedException {
        String requestId = jo.getString("requestIdAtHead");
        JSONObject joHardlinks = jo.optJSONObject("hardLinks");

        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize());
        byte [] flattened = response.flattenToBuffer(buf).array();

        StringCallback lastCallback = new StringCallback();

        m_zk.create(VoltZK.user_snapshot_response + requestId,
                flattened,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                lastCallback, null);
        if (joHardlinks != null) {
            Iterator<String> itr = jsonFieldKeys(joHardlinks);
            while (itr.hasNext()) {
                requestId = itr.next();
                lastCallback = new StringCallback();
                m_zk.create(VoltZK.user_snapshot_response + requestId,
                        flattened,
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT,
                        lastCallback, null);
            }
        }
        lastCallback.get();
    }

    private void saveResponseToZK(String requestid, ClientResponseImpl response)
            throws JSONException, KeeperException, InterruptedException {
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize());
        byte [] flattened = response.flattenToBuffer(buf).array();

        StringCallback lastCallback = new StringCallback();

        m_zk.create(VoltZK.user_snapshot_response + requestid,
                flattened,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                lastCallback, null);

        lastCallback.get();
    }

    /*
     *  Search for truncation snapshots, after a failure there may be
     *  ones we don't know about, there may be ones from a previous instance etc.
     *  Do this every five minutes as an easy hack to make sure we don't leak them.
     *  Next time groom is called it will delete the old ones after a success.
     */
    private void scanTruncationSnapshots() {
        if (m_truncationSnapshotPath == null) {
            try {
                m_truncationSnapshotPath = new String(m_zk.getData(VoltZK.test_scan_path, false, null), "UTF-8");
            } catch (Exception e) {
                return;
            }
        }

        Object params[] = new Object[1];
        params[0] = m_truncationSnapshotPath;
        long handle = m_nextCallbackHandle++;
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(final ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                    SNAP_LOG.error(clientResponse.getStatusString());
                    return;
                }

                final VoltTable results[] = clientResponse.getResults();
                if (results.length == 1) {
                    final VoltTable result = results[0];
                    boolean advanced = result.advanceRow();
                    assert(advanced);
                    assert(result.getColumnCount() == 1);
                    assert(result.getColumnType(0) == VoltType.STRING);
                    loggingLog.error("Snapshot scan failed with failure response: " + result.getString("ERR_MSG"));
                    return;
                }
                assert(results.length == 3);

                final VoltTable snapshots = results[0];
                assert(snapshots.getColumnCount() == 9);

                TreeMap<Long, TruncationSnapshotAttempt> foundSnapshots =
                    new TreeMap<Long, TruncationSnapshotAttempt>();
                while (snapshots.advanceRow()) {
                    final String path = snapshots.getString("PATH");
                    final String nonce = snapshots.getString("NONCE");
                    final Long txnId = snapshots.getLong("TXNID");
                    TruncationSnapshotAttempt snapshotAttempt = new TruncationSnapshotAttempt();
                    snapshotAttempt.path = path;
                    snapshotAttempt.nonce = nonce;
                    foundSnapshots.put(txnId, snapshotAttempt);
                }

                for (Map.Entry<Long, TruncationSnapshotAttempt> entry : foundSnapshots.entrySet()) {
                    if (!m_truncationSnapshotAttempts.containsKey(entry.getKey())) {
                        loggingLog.info("Truncation snapshot scan discovered new snapshot txnid " + entry.getKey() +
                                " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                        m_truncationSnapshotAttempts.put(entry.getKey(), entry.getValue());
                    }
                }
            }

        });
        m_initiator.initiateSnapshotDaemonWork("@SnapshotScan", handle, params);
    }

    /*
     * Delete all snapshots older then the last successful snapshot.
     * This only effects snapshots used for log truncation
     */
    private void groomTruncationSnapshots() {
        ArrayList<TruncationSnapshotAttempt> toDelete = new ArrayList<TruncationSnapshotAttempt>();
        boolean foundMostRecentSuccess = false;
        Iterator<Map.Entry<Long, TruncationSnapshotAttempt>> iter =
            m_truncationSnapshotAttempts.descendingMap().entrySet().iterator();
        loggingLog.info("Snapshot daemon grooming truncation snapshots");
        while (iter.hasNext()) {
            Map.Entry<Long, TruncationSnapshotAttempt> entry = iter.next();
            TruncationSnapshotAttempt snapshotAttempt = entry.getValue();
            if (!foundMostRecentSuccess) {
                if (snapshotAttempt.finished) {
                    loggingLog.info("Found most recent successful snapshot txnid " + entry.getKey()
                            + " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                    foundMostRecentSuccess = true;
                } else {
                    loggingLog.info("Retaining possible partial snapshot txnid " + entry.getKey()
                            + " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                }
            } else {
                loggingLog.info("Deleting old unecessary snapshot txnid " + entry.getKey()
                        + " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                toDelete.add(entry.getValue());
                iter.remove();
            }
        }

        String paths[] = new String[toDelete.size()];
        String nonces[] = new String[toDelete.size()];

        int ii = 0;
        for (TruncationSnapshotAttempt attempt : toDelete) {
            paths[ii] = attempt.path;
            nonces[ii++] = attempt.nonce;
        }

        Object params[] =
            new Object[] {
                paths,
                nonces,
                };
        long handle = m_nextCallbackHandle++;
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    SNAP_LOG.error(clientResponse.getStatusString());
                }
            }

        });
        m_initiator.initiateSnapshotDaemonWork("@SnapshotDelete", handle, params);
    }

    /*
     * If this cluster has per partition transactions ids carried over from
     * previous instances, retrieve them from ZK and pass them to snapshot save so that it can
     * include them in the snapshot
     */
    private JSONArray retrievePerPartitionTransactionIds() {
        JSONArray retval = new JSONArray();
        try {
            ByteBuffer values = ByteBuffer.wrap(m_zk.getData(VoltZK.perPartitionTxnIds, false, null));
            int numKeys = values.getInt();
            for (int ii = 0; ii < numKeys; ii++) {
                retval.put(values.getLong());
            }
        } catch (KeeperException.NoNodeException e) {/*doesn't have to exist*/}
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to retrieve per partition transaction ids for snapshot", false, e);
        }
        return retval;
    }


    /**
     * Leader election for snapshots.
     * Leader will watch for truncation and user snapshot requests
     */
    private void leaderElection() {
        loggingLog.info("Starting leader election for snapshot truncation daemon");
        try {
            while (true) {
                Stat stat = m_zk.exists(VoltZK.snapshot_truncation_master, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        switch(event.getType()) {
                        case NodeDeleted:
                            loggingLog.info("Detected the snapshot truncation leader's ephemeral node deletion");
                            m_es.execute(new Runnable() {
                                @Override
                                public void run() {
                                    leaderElection();
                                }
                            });
                            break;
                            default:
                                break;
                        }
                    }
                });
                if (stat == null) {
                    try {
                        m_zk.create(VoltZK.snapshot_truncation_master, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        m_isAutoSnapshotLeader = true;
                        if (m_lastKnownSchedule != null) {
                            makeActivePrivate(m_lastKnownSchedule);
                        }
                        electedTruncationLeader();
                        return;
                    } catch (NodeExistsException e) {
                    }
                } else {
                    loggingLog.info("Leader election concluded, a leader already exists");
                    break;
                }
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Exception in snapshot daemon electing master via ZK", true, e);
        }
    }

    /*
     * Invoked when this snapshot daemon has been elected as leader
     */
    private void electedTruncationLeader() throws Exception {
        loggingLog.info("This node was selected as the leader for snapshot truncation");
        m_truncationSnapshotScanTask = m_es.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    scanTruncationSnapshots();
                } catch (Exception e) {
                    loggingLog.error("Error during scan and group of truncation snapshots");
                }
            }
        }, 0, 1, TimeUnit.HOURS);
        try {
            // TRAIL [TruncSnap:1] elected as leader
            truncationRequestExistenceCheck();
            userSnapshotRequestExistenceCheck(false);
            m_bIsSnapshotDaemonLeader = true;
            // Need to decide what to do with the existing active entry if we are replacing a Daemon that
            // has gone away.
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error while accepting snapshot daemon leadership", true, e);
        }
    }

    /*
     * Process the event generated when the node for a truncation request
     * is created, reschedules it for a few seconds later
     */
    private void processTruncationRequestEvent(final WatchedEvent event) {
        if (event.getType() == EventType.NodeChildrenChanged) {
            loggingLog.info("Scheduling truncation request processing 10 seconds from now");
            /*
             * Do it 10 seconds later because these requests tend to come in bunches
             * and we want one truncation snapshot to do truncation for all nodes
             * so we don't get them back to back
             *
             * TRAIL [TruncSnap:5] wait 10 secs to process request
             */
            m_es.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        processSnapshotTruncationRequestCreated(event);
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB("Error processing snapshot truncation request creation", true, e);
                    }
                }
            }, m_truncationGatheringPeriod, TimeUnit.SECONDS);
            return;
        } else {
            /*
             * We are very careful to cancel the watch if we find that a truncation requests exists. We are
             * the only thread and daemon that should delete the node or change the data and the watch
             * isn't set when that happens because it is part of processing the request and the watch should
             * either be canceled or have already fired.
             */
            VoltDB.crashLocalVoltDB(
                    "Trunction request watcher fired with event type other then created: " + event.getType(),
                    true,
                    null);
        }
    }

    /*
     * A ZK event occured requestion a truncation snapshot be taken
     */
    private void processSnapshotTruncationRequestCreated(
            final WatchedEvent event) {
        loggingLog.info("Snapshot truncation leader received snapshot truncation request");
        String snapshotPathTemp;
        // TRAIL [TruncSnap:6] Get the snapshot path.
        try {
            snapshotPathTemp = new String(m_zk.getData(VoltZK.truncation_snapshot_path, false, null), "UTF-8");
        } catch (Exception e) {
            loggingLog.error("Unable to retrieve truncation snapshot path from ZK, log can't be truncated");
            return;
        }
        // Get the truncation request ID which is the truncation request node path.
        final String truncReqId;
        try {
            List<String> children = m_zk.getChildren(event.getPath(), false);
            if (children.isEmpty()) {
                loggingLog.error("Unable to retrieve truncation snapshot request id from ZK, log can't be truncated");
                return;
            }
            truncReqId = ZKUtil.joinZKPath(event.getPath(), Collections.max(children));

        } catch (Exception e) {
            loggingLog.error("Unable to retrieve truncation snapshot request ID from ZK, log can't be truncated");
            return;
        }
        m_truncationSnapshotPath = snapshotPathTemp;
        final String snapshotPath = snapshotPathTemp;
        final long now = System.currentTimeMillis();
        final String nonce = Long.toString(now);
        //Allow nodes to check and see if the nonce incoming for a snapshot is
        //for a truncation snapshot. In that case they will mark the completion node
        //to be for a truncation snapshot. SnapshotCompletionMonitor notices the mark.
        // TRAIL [TruncSnap:7] write current ts to request zk node data
        try {
            ByteBuffer payload = ByteBuffer.allocate(8);
            payload.putLong(0, now);
            m_zk.setData(VoltZK.request_truncation_snapshot, payload.array(), -1);
        } catch (Exception e) {
            //Cause a cascading failure?
            VoltDB.crashLocalVoltDB("Setting data on the truncation snapshot request in ZK should never fail", true, e);
        }
        // for the snapshot save invocations
        JSONObject jsObj = new JSONObject();
        try {
            String sData = "";
            if (truncReqId != null) {
                JSONObject jsData = new JSONObject();
                jsData.put("truncReqId", truncReqId);
                sData = jsData.toString();
            }
            jsObj.put("path", snapshotPath );
            jsObj.put("nonce", nonce);
            jsObj.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
            jsObj.put("data", sData);
        } catch (JSONException e) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", true, e);
        }

        // for the snapshot save invocations
        long handle = m_nextCallbackHandle++;

        // for the snapshot save invocation
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                m_lastInitiationTs = null;
                if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                    loggingLog.warn(
                            "Attempt to initiate a truncation snapshot was not successful: " +
                            clientResponse.getStatusString());
                    loggingLog.warn("Retrying log truncation snapshot in 5 minutes");
                    /*
                     * TRAIL [TruncSnap:8] (callback) on failed response try again in a few minute
                     */
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processTruncationRequestEvent(event);
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB("Error processing snapshot truncation request event", true, e);
                            }
                        }
                    }, 5, TimeUnit.MINUTES);
                    return;
                }

                final VoltTable results[] = clientResponse.getResults();
                final VoltTable result = results[0];
                boolean success = true;

                final String err = SnapshotUtil.didSnapshotRequestFailWithErr(results);
                if (err != null) {
                    if (err.trim().equalsIgnoreCase("SNAPSHOT IN PROGRESS")) {
                        loggingLog.info("Snapshot is in progress");
                    } else {
                        loggingLog.warn("Snapshot failed with failure response: " + err);
                    }
                    success = false;
                }

                //assert(result.getColumnName(1).equals("TABLE"));
                if (success) {
                    while (result.advanceRow()) {
                        if (!result.getString("RESULT").equals("SUCCESS")) {
                            success = false;
                            loggingLog.warn("Snapshot save feasibility test failed for host "
                                    + result.getLong("HOST_ID") + " table " + result.getString("TABLE") +
                                    " with error message " + result.getString("ERR_MSG"));
                        }
                    }
                }

                if (success) {
                    loggingLog.info("Snapshot initiation for log truncation was successful");

                    JSONObject obj = new JSONObject(clientResponse.getAppStatusString());
                    final long snapshotTxnId = Long.valueOf(obj.getLong("txnId"));
                    try {
                        boolean found = false;
                        ZKUtil.VoidCallback lastCallback = null;

                        for (String child: m_zk.getChildren(event.getPath(),false)) {
                            String requestId = ZKUtil.joinZKPath(event.getPath(), child);
                            found = found || requestId.equals(truncReqId);

                            lastCallback = new ZKUtil.VoidCallback();
                            m_zk.delete(requestId, -1, lastCallback, null);
                        }

                        if (lastCallback != null) {
                            try {
                                lastCallback.get();
                            } catch (KeeperException.NoNodeException ignoreIt) {
                            }
                        }
                        if (!found) {
                            VoltDB.crashLocalVoltDB(
                                    "Could not match truncations snapshot request id while atepting its removal", true, null);
                        }
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB(
                                "Unexpected error deleting truncation snapshot request", true, e);
                    }

                    try {
                        TruncationSnapshotAttempt snapshotAttempt =
                            m_truncationSnapshotAttempts.get(snapshotTxnId);
                        if (snapshotAttempt == null) {
                            snapshotAttempt = new TruncationSnapshotAttempt();
                            m_truncationSnapshotAttempts.put(snapshotTxnId, snapshotAttempt);
                        }
                        snapshotAttempt.nonce = nonce;
                        snapshotAttempt.path = snapshotPath;
                    } finally {
                        // TRAIL [TruncSnap:9] (callback) restart the whole request check cycle
                        try {
                            truncationRequestExistenceCheck();
                        } catch (Exception e) {
                            VoltDB.crashLocalVoltDB(
                                    "Unexpected error checking for existence of truncation snapshot request"
                                    , true, e);
                        }
                    }
                } else {
                    loggingLog.info("Retrying log truncation snapshot in 60 seconds");
                    /*
                     * TRAIL [TruncSnap:10] (callback) on table reported failure try again in a few minutes
                     */
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processTruncationRequestEvent(event);
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB("Exception processing truncation request event", true, e);
                            }
                        }
                    }, 1, TimeUnit.MINUTES);
                }
            }

        });
        try {
            loggingLog.info("Initiating @SnapshotSave for log truncation");
            initiateSnapshotSave(handle, new Object[]{jsObj.toString(4)}, false);
        } catch (JSONException e) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", true, e);
        }
        return;
    }

    /*
     * A ZK event occured requesting a truncation snapshot be taken
     */
    private void truncationSnapshotPrelude(final JSONObject joRequest) {
        String pathTemp = null;
        String nonceTemp = null;
        try {
            nonceTemp = joRequest.getString("nonce");
            pathTemp = joRequest.getString("path");
        } catch (NumberFormatException | JSONException e1) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", true, e1);
        }
        final String nonce = nonceTemp;
        final String snapshotPath = pathTemp;
        //Allow nodes to check and see if the nonce incoming for a snapshot is
        //for a truncation snapshot. In that case they will mark the completion node
        //to be for a truncation snapshot. SnapshotCompletionMonitor notices the mark.
        try {
            ByteBuffer payload = ByteBuffer.allocate(8);
            payload.putLong(0, Long.parseLong(nonce));
            m_zk.setData(VoltZK.request_truncation_snapshot, payload.array(), -1);
        } catch (Exception e) {
            //Cause a cascading failure?
            VoltDB.crashLocalVoltDB("Setting data on the truncation snapshot request in ZK should never fail", true, e);
        }

        // for the snapshot save invocations
        long handle = m_nextCallbackHandle++;

        // for the snapshot save invocation
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                m_lastInitiationTs = null;
                if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                    loggingLog.warn(
                            "Attempt to initiate a truncation snapshot was not successful: " +
                            clientResponse.getStatusString());
                    loggingLog.warn("Retrying log truncation snapshot in 5 minutes");
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                truncationSnapshotPrelude(joRequest);
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB("Error processing snapshot truncation request event", true, e);
                            }
                        }
                    }, 5, TimeUnit.MINUTES);
                    return;
                }

                final VoltTable results[] = clientResponse.getResults();
                final VoltTable result = results[0];
                boolean success = true;

                final String err = SnapshotUtil.didSnapshotRequestFailWithErr(results);
                if (err != null) {
                    if (err.trim().equalsIgnoreCase("SNAPSHOT IN PROGRESS")) {
                        loggingLog.info("Snapshot is in progress");
                    } else {
                        loggingLog.warn("Snapshot failed with failure response: " + err);
                    }
                    success = false;
                }

                if (success) {
                    while (result.advanceRow()) {
                        if (!result.getString("RESULT").equals("SUCCESS")) {
                            success = false;
                            loggingLog.warn("Snapshot save feasibility test failed for host "
                                    + result.getLong("HOST_ID") + " table " + result.getString("TABLE") +
                                    " with error message " + result.getString("ERR_MSG"));
                        }
                    }
                }

                if (success) {
                    loggingLog.info("Snapshot initiation for log truncation was successful");

                    JSONObject obj = new JSONObject(clientResponse.getAppStatusString());
                    final long snapshotTxnId = Long.valueOf(obj.getLong("txnId"));
                    TruncationSnapshotAttempt snapshotAttempt =
                            m_truncationSnapshotAttempts.get(snapshotTxnId);
                    if (snapshotAttempt == null) {
                        snapshotAttempt = new TruncationSnapshotAttempt();
                        m_truncationSnapshotAttempts.put(snapshotTxnId, snapshotAttempt);
                    }
                    snapshotAttempt.nonce = nonce;
                    snapshotAttempt.path = snapshotPath;
                } else {
                    loggingLog.info("Retrying log truncation snapshot in 60 seconds");
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                truncationSnapshotPrelude(joRequest);
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB("Exception processing truncation request event", true, e);
                            }
                        }
                    }, 1, TimeUnit.MINUTES);
                }
            }

        });
        try {
            loggingLog.info("Initiating @SnapshotSave for log truncation");
            initiateSnapshotSave(handle, new Object[]{joRequest.toString(4)}, false);
        } catch (JSONException e) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", true, e);
        }
        return;
    }

    private TruncationRequestExistenceWatcher m_currentTruncationWatcher = new TruncationRequestExistenceWatcher();
    /*
     * Watcher that handles changes to the ZK node for
     * internal truncation snapshot requests
     */
    private class TruncationRequestExistenceWatcher extends ZKUtil.CancellableWatcher {

        public TruncationRequestExistenceWatcher() {
            super(m_es);
        }

        @Override
        public void pProcess(final WatchedEvent event) {
            if (event.getState() == KeeperState.Disconnected) return;
            try {
                // TRAIL [TruncSnap:4] watch event on zk node fires
                processTruncationRequestEvent(event);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error procesing truncation request event", true, e);
            }
        }
    };

    /*
     * Watcher that handles events to the user snapshot request node
     * in ZK
     */
    private final Watcher m_userSnapshotRequestExistenceWatcher = new Watcher() {

        @Override
        public void process(final WatchedEvent event) {
            if (event.getState() == KeeperState.Disconnected) return;

            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        processUserSnapshotRequestEvent(event);
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB("Error processing user snapshot request event", true, e);
                    }
                }
            });
        }
    };

    /*
     * Process the event generated when the node for a user snapshot request
     * is created.
     */
    private void processUserSnapshotRequestEvent(final WatchedEvent event) throws Exception {
        if (event.getType() == EventType.NodeCreated) {
            byte data[] = m_zk.getData(event.getPath(), false, null);
            String jsonString = new String(data, "UTF-8");
            final JSONObject jsObj = new JSONObject(jsonString);
            final String requestId = jsObj.getString("requestId");
            final boolean blocking = jsObj.getBoolean("block");
            /*
             * Going to reuse the request object, remove the requestId
             * field now that it is consumed
             */
            jsObj.remove("requestId");
            jsObj.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
            final long handle = m_nextCallbackHandle++;
            m_procedureCallbacks.put(handle, new ProcedureCallback() {

                @Override
                public void clientCallback(ClientResponse clientResponse) {
                    m_lastInitiationTs = null;
                    try {
                        /*
                         * If there is an error then we are done.
                         */
                        if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                            ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                            saveResponseToZKAndReset(requestId, rimpl);
                            return;
                        }

                        /*
                         * Now analyze the response. If a snapshot was in progress
                         * we have to reattempt it later, and send a response to the client
                         * saying it was queued. Otherwise, forward the response
                         * failure/success to the client.
                         */
                        if (isSnapshotInProgressResponse(clientResponse)) {
                            scheduleSnapshotForLater(jsObj.toString(4), requestId, true);
                        } else {
                            ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                            saveResponseToZKAndReset(requestId, rimpl);
                            return;
                        }
                    } catch (Exception e) {
                        SNAP_LOG.error("Error processing user snapshot request", e);
                        try {
                            // TRAIL [RequestSnap:8] on error reset watcher
                            userSnapshotRequestExistenceCheck(true);
                        } catch (Exception e2) {
                            VoltDB.crashLocalVoltDB("Error resetting watch for user snapshots", true, e2);
                        }
                    }
                }
            });
            // TRAIL [RequestSnap:7] watcher fires on zk node creation
            initiateSnapshotSave(handle, new Object[]{jsObj.toString(4)}, blocking);
            return;
        }
    }

    /*
     * Process the event generated when the node for a user snapshot request
     * is created.
     */
    private void processUserSnapshotPrelude(final JSONObject jsObj) throws JSONException {
        final boolean blocking = jsObj.getBoolean("block");

        /*
         * Going to reuse the request object, remove the requestId
         * field now that it is consumed
         */
        jsObj.remove("requestId");
        jsObj.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
        final long handle = m_nextCallbackHandle++;
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse) {
                m_lastInitiationTs = null;
                try {
                    /*
                     * If there is an error then we are done.
                     */
                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                        ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                        saveResponseToZK(jsObj, rimpl);
                        return;
                    }

                    if (isSnapshotInProgressResponse(clientResponse)) {
                        saveResponseToZK(jsObj, createSnapshotInProgessResponse());
                    } else {
                        ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                        saveResponseToZK(jsObj, rimpl);
                        return;
                    }
                } catch (Exception e) {
                    SNAP_LOG.error("Error processing user snapshot request:\n" + jsObj.toString(), e);
                }
            }
        });
        initiateSnapshotSave(handle, new Object[]{jsObj.toString(4)}, blocking);
    }

    private ClientResponseImpl createSnapshotInProgessResponse() {
        VoltTable result = SnapshotUtil.constructNodeResultsTable();
        result.addRow(-1,
                CoreUtils.getHostnameOrAddress(),
                "",
                "SUCCESS",
                "SNAPSHOT IN PROGRESS");
        return new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                new VoltTable[] { result },
                "Snapshot request could not be fulfilled because a snapshot " +
                "is in progress. It was queued for execution",
                0);
    }

    private ClientResponseImpl createDuplicateSnapshotResponse() {
        VoltTable result = SnapshotUtil.constructNodeResultsTable();
        result.addRow(-1,
                CoreUtils.getHostnameOrAddress(),
                "",
                "SUCCESS",
                "DUPLICATE SNAPSHOT");
        return new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                new VoltTable[] { result },
                "Snapshot request could not be fulfilled because a snapshot " +
                "for the same nonce and path is already queued",
                0);
    }

    /*
     * Schedule a user snapshot request for later since the database was busy.
     * Continue doing this as long as the error response returned by the DB is snapshot in progress.
     * Since the snapshot is being scheduled for later we will send an immediate response to the client
     * via ZK relay.
     */
    private void scheduleSnapshotForLater(
            final String requestObj,
            final String requestId,
            final boolean isFirstAttempt
            ) throws Exception {
        /*
         * Only need to send the queue response the first time we attempt to schedule the snapshot
         * for later. It may be necessary to reschedule via this function multiple times.
         */
        if (isFirstAttempt) {
            SNAP_LOG.info("A user snapshot request could not be immediately fulfilled and will be reattempted later");
            /*
             * Construct a result to send to the client right now via ZK
             * saying we queued it to run later
             */
            VoltTable result = SnapshotUtil.constructNodeResultsTable();
            result.addRow(-1,
                    CoreUtils.getHostnameOrAddress(),
                    "",
                    "SUCCESS",
                    "SNAPSHOT REQUEST QUEUED");
            final ClientResponseImpl queuedResponse =
                new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                                       new VoltTable[] { result },
                                       "Snapshot request could not be fulfilled because a snapshot " +
                                         "is in progress. It was queued for execution",
                                       0);
            ByteBuffer buf = ByteBuffer.allocate(queuedResponse.getSerializedSize());
            m_zk.create(VoltZK.user_snapshot_response + requestId,
                        queuedResponse.flattenToBuffer(buf).array(),
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
        }

        /*
         * Now queue the request for later
         */
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    /*
                     * Construct a callback to handle the response to the
                     * @SnapshotSave invocation that will reattempt the user snapshot
                     */
                    final long handle = m_nextCallbackHandle++;
                    m_procedureCallbacks.put(handle, new ProcedureCallback() {
                        @Override
                        public void clientCallback(ClientResponse clientResponse) {
                            m_lastInitiationTs = null;
                            try {
                                /*
                                 * If there is an error then we are done
                                 * attempting this user snapshot. The params must be bad
                                 * or things are broken.
                                 */
                                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                    SNAP_LOG.error(clientResponse.getStatusString());
                                    //Reset the watch, in case this is recoverable
                                    userSnapshotRequestExistenceCheck(true);
                                    return;
                                }

                                VoltTable results[] = clientResponse.getResults();
                                //Do this check to avoid an NPE
                                if (results == null || results.length == 0 || results[0].getRowCount() < 1) {
                                    SNAP_LOG.error("Queued user snapshot request reattempt received an unexpected response" +
                                            " and will not be reattempted. The client response is (status: " +
                                            clientResponse.getStatus() + " " + clientResponse.getStatusString() +
                                            " result: " + (results != null && results.length > 0 ? results[0] : "null") +
                                            ")");
                                    /*
                                     * Don't think this should happen, reset the watch to allow later requests
                                     */
                                    userSnapshotRequestExistenceCheck(true);
                                    return;
                                }

                                VoltTable result = results[0];
                                boolean snapshotInProgress = false;
                                boolean haveFailure = false;
                                while (result.advanceRow()) {
                                    if (result.getString("RESULT").equals("FAILURE")) {
                                        if (result.getString("ERR_MSG").equals("SNAPSHOT IN PROGRESS")) {
                                            snapshotInProgress = true;
                                        } else {
                                            haveFailure = true;
                                        }
                                    }
                                }

                                /*
                                 * If a snapshot was in progress, reattempt later, otherwise,
                                 * if there was a failure, abort the attempt and log.
                                 */
                                if (snapshotInProgress) {
                                    SNAP_LOG.info("Queued user snapshot was reattempted, but a snapshot was " +
                                            " still in progress. It will be reattempted.");
                                    //Turtles all the way down
                                    scheduleSnapshotForLater(
                                            requestObj,
                                            requestId,
                                            false);
                                } else if (haveFailure) {
                                    SNAP_LOG.info("Queued user snapshot was attempted, but there was a failure.");
                                    try {
                                        ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                                        saveResponseToZKAndReset(requestId, rimpl);
                                    }
                                    catch (NodeExistsException e) {
                                        // used to pass null as request ID to avoid this check if the request ID
                                        // already existed, this gives us the same behavior with a pre-existing
                                        // request ID
                                    }
                                    //Log the details of the failure, after resetting the watch in case of some odd NPE
                                    result.resetRowPosition();
                                    SNAP_LOG.info(result);
                                } else {
                                    try {
                                        SNAP_LOG.debug("Queued user snapshot was successfully requested, saving to path " +
                                                VoltZK.user_snapshot_response + requestId);
                                        /*
                                         * Snapshot was started no problem, reset the watch for new requests
                                         */
                                        ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                                        saveResponseToZKAndReset(requestId, rimpl);
                                    }
                                    catch (NodeExistsException e) {
                                        // used to pass null as request ID to avoid this check if the request ID
                                        // already existed, this gives us the same behavior with a pre-existing
                                        // request ID
                                    }
                                    return;
                                }
                            } catch (Exception e) {
                                SNAP_LOG.error("Error processing procedure callback for user snapshot", e);
                                try {
                                    userSnapshotRequestExistenceCheck(true);
                                } catch (Exception e1) {
                                    VoltDB.crashLocalVoltDB(
                                            "Error resetting watch for user snapshot requests", true, e1);
                                }
                            }
                        }
                    });

                    initiateSnapshotSave(handle, new Object[]{requestObj}, false);
                } catch (Exception e) {
                    try {
                        userSnapshotRequestExistenceCheck(true);
                    } catch (Exception e1) {
                        VoltDB.crashLocalVoltDB("Error checking for existence of user snapshots", true, e1);
                    }
                }
            }
        };
        m_es.schedule(r, m_userSnapshotRetryInterval, TimeUnit.SECONDS);
    }

    /*
     * Check a client response to and determine if it is a snapshot in progress response
     * to a snapshot request
     */
    private boolean isSnapshotInProgressResponse(
            ClientResponse response) {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            return false;
        }

        if (response.getResults() == null) {
            return false;
        }

        if (response.getResults().length < 1) {
            return false;
        }

        VoltTable results = response.getResults()[0];
        if (results.getRowCount() < 1) {
            return false;
        }

        boolean snapshotInProgress = false;
        while (results.advanceRow()) {
            if (results.getString("RESULT").equals("FAILURE")) {
                if (results.getString("ERR_MSG").equals("SNAPSHOT IN PROGRESS")) {
                    snapshotInProgress = true;
                }
            }
        }
        return snapshotInProgress;
    }

    /*
     * Set the watch in ZK on the node that represents an internal request
     * for a truncation snapshot
     */
    void truncationRequestExistenceCheck() throws KeeperException, InterruptedException {
        loggingLog.info("Checking for existence of snapshot truncation request");
        m_currentTruncationWatcher.cancel();
        m_currentTruncationWatcher = new TruncationRequestExistenceWatcher();
        // TRAIL [TruncSnap:2] checking for zk node existence
        List<String> requests = m_zk.getChildren(VoltZK.request_truncation_snapshot, m_currentTruncationWatcher);
        if (!requests.isEmpty()) {
            loggingLog.info("A truncation request node already existed, processing truncation request event");
            m_currentTruncationWatcher.cancel();
            // TRAIL [TruncSnap:3] fake a node created event (req ZK node already there)
            processTruncationRequestEvent(new WatchedEvent(
                    EventType.NodeChildrenChanged,
                    KeeperState.SyncConnected,
                    VoltZK.request_truncation_snapshot));
        }
    }

    /*
     * Set the watch in ZK on the node that represents a user
     * request for a snapshot
     */
    void userSnapshotRequestExistenceCheck(boolean deleteExistingRequest) throws Exception {
        // TRAIL [RequestSnap:6] clears the request node. Reset so you can watch it later
        if (deleteExistingRequest) {
            m_zk.delete(VoltZK.user_snapshot_request, -1, null, null);
        }
        // TRAIL [RequestSnap:5] sets watcher on on request node, or if present process it
        if (m_zk.exists(VoltZK.user_snapshot_request, m_userSnapshotRequestExistenceWatcher) != null) {
            processUserSnapshotRequestEvent(new WatchedEvent(
                    EventType.NodeCreated,
                    KeeperState.SyncConnected,
                    VoltZK.user_snapshot_request));
        }
    }

    /**
     * Make this SnapshotDaemon responsible for generating snapshots
     */
    public ListenableFuture<Void> mayGoActiveOrInactive(final SnapshotSchedule schedule)
    {
        return m_es.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                makeActivePrivate(schedule);
                return null;
            }
        });
    }

    private void makeActivePrivate(final SnapshotSchedule schedule) {
        m_lastKnownSchedule = schedule;
        if (schedule.getEnabled()) {
            m_frequency = schedule.getFrequencyvalue();
            m_retain = schedule.getRetain();
            m_path = schedule.getPath();
            m_prefix = schedule.getPrefix();
            m_prefixAndSeparator = m_prefix + "_";
            final String frequencyUnitString = schedule.getFrequencyunit().toLowerCase();
            assert(frequencyUnitString.length() == 1);
            final char frequencyUnit = frequencyUnitString.charAt(0);

            switch (frequencyUnit) {
            case 's':
                m_frequencyUnit = TimeUnit.SECONDS;
                break;
            case 'm':
                m_frequencyUnit = TimeUnit.MINUTES;
                break;
            case 'h':
                m_frequencyUnit = TimeUnit.HOURS;
                break;
                default:
                    throw new RuntimeException("Frequency unit " + frequencyUnitString + "" +
                            " in snapshot schedule is not one of d,m,h");
            }
            m_frequencyInMillis = TimeUnit.MILLISECONDS.convert( m_frequency, m_frequencyUnit);
            m_nextSnapshotTime = System.currentTimeMillis() + m_frequencyInMillis;
        }

        if (m_isAutoSnapshotLeader) {
            if (schedule.getEnabled()) {
                if (m_autoSnapshotTask == null) {
                    m_autoSnapshotTask = m_es.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doPeriodicWork(System.currentTimeMillis());
                            } catch (Exception e) {
                                SNAP_LOG.warn("Error doing periodic snapshot management work", e);
                            }
                        }
                    }, 0, m_periodicWorkInterval, TimeUnit.MILLISECONDS);
                }
            } else {
                if (m_autoSnapshotTask != null) {
                    m_autoSnapshotTask.cancel(false);
                    m_autoSnapshotTask = null;
                }
            }
        }
    }

    public void makeInactive() {
        m_es.execute(new Runnable() {
            @Override
            public void run() {

                m_snapshots.clear();
            }
        });
    }

    private class Snapshot implements Comparable<Snapshot> {
        private final String path;
        private final String nonce;
        private final Long txnId;

        private Snapshot (String path, String nonce, Long txnId) {
            this.path = path;
            this.nonce = nonce;
            this.txnId = txnId;
        }

        @Override
        public int compareTo(Snapshot o) {
            return txnId.compareTo(o.txnId);
        }

        @Override
        public String toString() {
            return path + "/" + nonce;
        }
    }

    /**
     * Invoked by the client interface occasionally. Returns null
     * if nothing needs to be done or the name of a sysproc along with procedure parameters
     * if there is work to be done. Responses come back later via invocations
     * of processClientResponse
     * @param now Current time
     * @return null if there is no work to do or a sysproc with parameters if there is work
     */
    private void doPeriodicWork(final long now) {
        if (m_lastKnownSchedule == null)
        {
            setState(State.STARTUP);
            return;
        }

        if (m_frequencyUnit == null) {
            return;
        }

        if (m_state == State.STARTUP) {
            initiateSnapshotScan();
        } else if (m_state == State.SCANNING) {
            RateLimitedLogger.tryLogForMessage(System.nanoTime(), 5, TimeUnit.MINUTES, SNAP_LOG, Level.INFO, "Blocked in scanning");
            return;
        } else if (m_state == State.WAITING){
            processWaitingPeriodicWork(now);
        } else if (m_state == State.SNAPSHOTTING) {
            return;
        } else if (m_state == State.DELETING){
            return;
        }
    }

    /**
     * Do periodic work when the daemon is in the waiting state. The
     * daemon paces out sysproc invocations over time
     * to avoid disrupting regular work. If the time for the next
     * snapshot has passed it attempts to initiate a new snapshot.
     * If there are too many snapshots being retains it attempts to delete
     * the extras. Then it attempts to initiate a new snapshot if
     * one is due
     */
    private void processWaitingPeriodicWork(long now) {
        if (now - m_lastSysprocInvocation < m_minTimeBetweenSysprocs) {
            return;
        }

        if (m_snapshots.size() > m_retain) {
            //Quick hack to make sure we don't delete while the snapshot is running.
            //Deletes work really badly during a snapshot because the FS is occupied
            if (!SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty()) {
                m_lastSysprocInvocation = System.currentTimeMillis() + 3000;
                return;
            }
            deleteExtraSnapshots();
            return;
        }

        if (m_nextSnapshotTime < now) {
            initiateNextSnapshot(now);
            return;
        }
    }

    private void initiateNextSnapshot(long now) {
        setState(State.SNAPSHOTTING);
        m_lastSysprocInvocation = now;
        final Date nowDate = new Date(now);
        final String dateString = m_dateFormat.format(nowDate);
        final String nonce = m_prefix + dateString;
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put("path", m_path);
            jsObj.put("nonce", nonce);
            jsObj.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
            jsObj.put("isAuto", true);
            m_snapshots.offer(new Snapshot(m_path, nonce, now));
            long handle = m_nextCallbackHandle++;
            m_procedureCallbacks.put(handle, new ProcedureCallback() {

                @Override
                public void clientCallback(final ClientResponse clientResponse)
                        throws Exception
                {
                    m_lastInitiationTs = null;
                    processClientResponsePrivate(clientResponse);
                }

            });
            SNAP_LOG.info("Requesting auto snapshot to path " + m_path + " nonce " + nonce);
            initiateSnapshotSave(handle, new Object[]{jsObj.toString(4)}, false);
        } catch (JSONException e) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", false, e);
        }
    }

    /**
     * Invoke the @SnapshotScan system procedure to discover
     * snapshots on disk that are managed by this daemon
     * @return
     */
    private void initiateSnapshotScan() {
        m_lastSysprocInvocation = System.currentTimeMillis();
        Object params[] = new Object[1];
        params[0] = m_path;
        setState(State.SCANNING);
        long handle = m_nextCallbackHandle++;
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(final ClientResponse clientResponse)
                    throws Exception {
                processClientResponsePrivate(clientResponse);
            }

        });
        SNAP_LOG.info("Initiating snapshot scan of " + m_path);
        m_initiator.initiateSnapshotDaemonWork("@SnapshotScan", handle, params);
    }

    /**
     * Process responses to sysproc invocations generated by this daemon
     * via processPeriodicWork
     * @param response
     * @return
     */
    public Future<Void> processClientResponse(final Callable<ClientResponseImpl> response) {
        return m_es.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    ClientResponseImpl resp = response.call();
                    long handle = resp.getClientHandle();
                    m_procedureCallbacks.remove(handle).clientCallback(resp);
                } catch (Exception e) {
                    SNAP_LOG.warn("Error when SnapshotDaemon invoked callback for a procedure invocation", e);
                    /*
                     * Don't think it is productive to propagate any exceptions here, Ideally
                     * they should be handled by the procedure callbacks
                     */
                }
                return null;
            }
        });
    }

    private void processClientResponsePrivate(ClientResponse response) {
        if (m_frequencyUnit == null) {
            throw new RuntimeException("SnapshotDaemon received a response when it has not been configured to run");
        }

        if (m_state == State.STARTUP) {
            throw new RuntimeException("SnapshotDaemon received a response in the startup state");
        } else if (m_state == State.SCANNING) {
            processScanResponse(response);
        } else if (m_state == State.DELETING){
            processDeleteResponse(response);
            return;
        } else if (m_state == State.SNAPSHOTTING){
            processSnapshotResponse(response);
            return;
        }
    }

    /**
     * Confirm and log that the snapshot was a success
     * @param response
     */
    private void processSnapshotResponse(ClientResponse response) {
        setState(State.WAITING);
        final long now = System.currentTimeMillis();
        m_nextSnapshotTime += m_frequencyInMillis;
        if (m_nextSnapshotTime < now) {
            m_nextSnapshotTime = now - 1;
        }

        if (response.getStatus() != ClientResponse.SUCCESS){
            logFailureResponse("Snapshot failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        final VoltTable result = results[0];

        final String err = SnapshotUtil.didSnapshotRequestFailWithErr(results);
        if (err != null) {
            SNAP_LOG.warn("Snapshot failed with failure response: " +  err);
            m_snapshots.removeLast();
            return;
        }

        //assert(result.getColumnName(1).equals("TABLE"));
        boolean success = true;
        while (result.advanceRow()) {
            if (!result.getString("RESULT").equals("SUCCESS")) {
                success = false;
                SNAP_LOG.warn("Snapshot save feasibility test failed for host "
                        + result.getLong("HOST_ID") + " table " + result.getString("TABLE") +
                        " with error message " + result.getString("ERR_MSG"));
            }
        }
        if (!success) {
            m_snapshots.removeLast();
        }
    }

    /**
     * Process a response to a request to delete snapshots.
     * Always transitions to the waiting state even if the delete
     * fails. This ensures the system will continue to snapshot
     * until the disk is full in the event that there is an administration
     * error or a bug.
     * @param response
     */
    private void processDeleteResponse(ClientResponse response) {
        //Continue snapshotting even if a delete fails.
        setState(State.WAITING);
        if (response.getStatus() != ClientResponse.SUCCESS){
            logFailureResponse("Delete of snapshots failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        final String err = SnapshotUtil.didSnapshotRequestFailWithErr(results);
        if (err != null) {
            SNAP_LOG.warn("Snapshot delete failed with failure response: " + err);
            return;
        }
    }

    /**
     * Process the response to a snapshot scan. Find the snapshots
     * that are managed by this daemon by path and nonce
     * and add it the list. Initiate a delete of any that should
     * not be retained
     * @param response
     * @return
     */
    private void processScanResponse(ClientResponse response) {
        setState(State.WAITING);
        if (response.getStatus() != ClientResponse.SUCCESS) {
            logFailureResponse("Initial snapshot scan failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        if (results.length == 1) {
            final VoltTable result = results[0];
            boolean advanced = result.advanceRow();
            assert(advanced);
            assert(result.getColumnCount() == 1);
            assert(result.getColumnType(0) == VoltType.STRING);
            SNAP_LOG.warn("Initial snapshot scan failed with failure response: " + result.getString("ERR_MSG"));
            return;
        }
        assert(results.length == 3);

        final VoltTable snapshots = results[0];
        assert(snapshots.getColumnCount() == 9);

        final File myPath = new File(m_path);
        while (snapshots.advanceRow()) {
            final String path = snapshots.getString("PATH");
            final File pathFile = new File(path);
            if (pathFile.equals(myPath)) {
                final String nonce = snapshots.getString("NONCE");
                if (nonce.startsWith(m_prefixAndSeparator)) {
                    final Long txnId = snapshots.getLong("TXNID");
                    m_snapshots.add(new Snapshot(path, nonce, txnId));
                }
            }
        }

        java.util.Collections.sort(m_snapshots);

        deleteExtraSnapshots();
    }

    /**
     * Check if there are extra snapshots and initiate deletion
     * @return
     */
    private void deleteExtraSnapshots() {
        if (m_snapshots.size() <= m_retain) {
            setState(State.WAITING);
        } else {
            m_lastSysprocInvocation = System.currentTimeMillis();
            setState(State.DELETING);
            final int numberToDelete = m_snapshots.size() - m_retain;
            String pathsToDelete[] = new String[numberToDelete];
            String noncesToDelete[] = new String[numberToDelete];
            for (int ii = 0; ii < numberToDelete; ii++) {
                final Snapshot s = m_snapshots.poll();
                pathsToDelete[ii] = s.path;
                noncesToDelete[ii] = s.nonce;
                SNAP_LOG.info("Snapshot daemon deleting " + s.nonce);
            }
            Object params[] =
                new Object[] {
                    pathsToDelete,
                    noncesToDelete,
                    };
            long handle = m_nextCallbackHandle++;
            m_procedureCallbacks.put(handle, new ProcedureCallback() {

                @Override
                public void clientCallback(final ClientResponse clientResponse)
                        throws Exception {
                    processClientResponsePrivate(clientResponse);
                }

            });
            m_initiator.initiateSnapshotDaemonWork("@SnapshotDelete", handle, params);
        }
    }

    private void logFailureResponse(String message, ClientResponse response) {
        SNAP_LOG.warn(message + "\n" + response.getStatusString());
        if (response.getStatusString() != null) {
            SNAP_LOG.warn(response.getStatusString());
        }
    }

    State getState() {
        return m_state;
    }

    void setState(State state) {
        m_state = state;
    }

    public void shutdown() throws InterruptedException {
        if (m_truncationSnapshotScanTask != null) {
            m_truncationSnapshotScanTask.cancel(false);
        }

        m_es.shutdown();
        m_es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    /*
     * If we are the leader, go ahead an create the procedure invocation to do the work.
     * We aren't going to journal this in ZK. if the leader dies there will be no
     * one to try and complete the work. C'est la vie.
     */
    public void requestUserSnapshot(final StoredProcedureInvocation invocation, final Connection c) {
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    submitUserSnapshotRequest(invocation, c);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Exception submitting user snapshot request", true, e);
                }
            }
        });
    }

    public static final class ForwardClientException extends Exception {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private final VoltTable m_table;
        public ForwardClientException(String msg, VoltTable table) {
            super(msg);
            m_table = table;
        }

    }

    private void submitUserSnapshotRequest(final StoredProcedureInvocation invocation, final Connection c) {
        Object params[] = invocation.getParams().toArray();

        try {
            /*
             * Dang it, have to parse the params here to validate
             */
            SnapshotInitiationInfo snapInfo = new SnapshotInitiationInfo(params);

            createAndWatchRequestNode(invocation.clientHandle, c, snapInfo,
                    false);
        } catch (Exception e) {
            VoltTable tables[] = new VoltTable[0];
            byte status = ClientResponseImpl.GRACEFUL_FAILURE;
            if (e instanceof ForwardClientException && ((ForwardClientException)e).m_table != null) {
                tables = new VoltTable[] { ((ForwardClientException)e).m_table };
                status = ClientResponseImpl.SUCCESS;
            }
            final ClientResponseImpl errorResponse =
                    new ClientResponseImpl(status,
                                           tables,
                                           Throwables.getStackTraceAsString(e),
                                           invocation.clientHandle);
            ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
            buf.putInt(buf.capacity() - 4);
            errorResponse.flattenToBuffer(buf).flip();
            c.writeStream().enqueue(buf);
            return;
        }
    }

    /**
     * Try to create the ZK request node and watch it if created successfully.
     */
    public void createAndWatchRequestNode(final long clientHandle,
                                          final Connection c,
                                          SnapshotInitiationInfo snapInfo,
                                          boolean notifyChanges) throws ForwardClientException {
        boolean requestExists = false;
        // TRAIL [RequestSnap:2] get request id from generated node
        final String requestId = queueRequestNode(snapInfo);
        if (requestId == null) {
            requestExists = true;
        } else {
            if (!snapInfo.isTruncationRequest()) {
                try {
                    registerUserSnapshotResponseWatch(requestId, clientHandle, c, notifyChanges);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Failed to register ZK watch on snapshot response", true, e);
                }
            }
            else {
                // need to construct a success response of some sort here to indicate the truncation attempt
                // was successfully attempted
                VoltTable result = SnapshotUtil.constructNodeResultsTable();
                result.addRow(-1,
                        CoreUtils.getHostnameOrAddress(),
                        "",
                        "SUCCESS",
                        "SNAPSHOT REQUEST QUEUED");
                final ClientResponseImpl resp =
                    new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                            new VoltTable[] {result},
                            "User-requested truncation snapshot successfully queued for execution.",
                            clientHandle);
                ByteBuffer buf = ByteBuffer.allocate(resp.getSerializedSize() + 4);
                buf.putInt(buf.capacity() - 4);
                resp.flattenToBuffer(buf).flip();
                c.writeStream().enqueue(buf);
            }
        }

        if (requestExists) {
            VoltTable result = SnapshotUtil.constructNodeResultsTable();
            result.addRow(-1,
                    CoreUtils.getHostnameOrAddress(),
                    "",
                    "FAILURE",
                    "SNAPSHOT IN PROGRESS");
            throw new ForwardClientException("A request to perform a user snapshot already exists", result);
        }
    }

    private final static Iterator<String> jsonFieldKeys(JSONObject jo) {
        return jo.keys();
    }

    private void makeTruncationRequestStub(JSONObject jo, int id) throws JSONException {
        if (jo.has("isTruncation") && jo.has("path") && jo.has("nonce")) return;

        final String truncReqId = SNAPSHOT_TYPE.FILE.requestId(id).name();

        jo.put("isTruncation", true);

        String snapshotPath;
        try {
            snapshotPath = new String(
                    m_zk.getData(VoltZK.truncation_snapshot_path, false, null),
                    StandardCharsets.UTF_8
                    );
        } catch (KeeperException|InterruptedException e) {
            loggingLog.error("Unable to retrieve truncation snapshot path from ZK, log can't be truncated", e);
            throw new JsonException("Unable to retrieve truncation snapshot path from ZK", e);
        }

        final long now = System.currentTimeMillis();
        final String nonce = Long.toString(now);

        JSONObject jsData = new JSONObject();
        jsData.put("truncReqId", truncReqId);
        String sData = jsData.toString();

        jo.put("path", snapshotPath);
        jo.put("nonce", nonce);
        jo.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
        jo.put("data", sData);
    }

    byte [] coalesceDefaultSnapshot(NavigableMap<Integer, ByteBuffer> map)  {
        if (map == null || map.isEmpty()) return null;

        final SNAPSHOT_TYPE fileType = SNAPSHOT_TYPE.FILE;
        int requestId = map.lastKey();
        JSONObject joRequest = null;
        JSONObject johl = new JSONObject();
        Iterator<String> keys = null;

        Map<Integer, JSONObject> discerners = new LinkedHashMap<>();

        for (Map.Entry<Integer, ByteBuffer> e: map.entrySet()) {
            ByteBuffer baw = e.getValue();
            if (baw == null || baw.limit() == 0) {
                VoltDB.crashLocalVoltDB("zk snapshot request node is null or empty", true, null);;
            }
            try {
                JSONObject jo = new JSONObject(new String(baw.array(), StandardCharsets.UTF_8));
                JSONObject hardLinks = jo.optJSONObject("hardLinks");
                if (hardLinks != null) {
                    keys = jsonFieldKeys(hardLinks);
                    while (keys.hasNext()) {
                        String rqid = keys.next();
                        johl.put(rqid, hardLinks.getJSONObject(rqid));
                    }
                }
                if (jo.optBoolean("isTruncation", false)) {
                    requestId = e.getKey();
                    makeTruncationRequestStub(jo, requestId);
                    joRequest = jo;
                } else {
                    discerners.put(e.getKey(), jo);
                }
            } catch (JSONException exc) {
                VoltDB.crashLocalVoltDB("zk snapshot request has malformed json", true, exc);
            }
        }
        if (joRequest == null) {
            joRequest = discerners.remove(map.lastKey());
        }
        try {
            for (Map.Entry<Integer, JSONObject> e: discerners.entrySet()) {
                JSONObject hardLink = new JSONObject();
                hardLink.put("nonce", e.getValue().get("nonce"));
                hardLink.put("path", e.getValue().get("path"));
                johl.put(fileType.requestId(e.getKey()).name(), hardLink);
            }
            Set<File> deduper = new HashSet<File>();
            keys = jsonFieldKeys(johl);
            while (keys.hasNext()) {
                String rqid = keys.next();
                JSONObject joReq = johl.getJSONObject(rqid);
                File nonceFH = new File(joReq.getString("path"),joReq.getString("nonce"));
                if (deduper.contains(nonceFH)) {
                    joReq.put("requestIdAtHead", rqid);
                    try {
                        saveResponseToZK(joReq, createDuplicateSnapshotResponse());
                    } catch (KeeperException | InterruptedException exc) {
                        VoltDB.crashLocalVoltDB("unable to save duplicate request response to zk", true, exc);
                    }
                    keys.remove();
                } else {
                    deduper.add(nonceFH);
                }
            }
            if (johl.length() > 0) {
                joRequest.put("hardLinks", johl);
            }
            joRequest.put("coalescedToRequestId", fileType.requestId(requestId).name());
        } catch (JSONException exc) {
            VoltDB.crashLocalVoltDB("snapshot request missing required nonce or path", true, exc);
        }
        byte [] request = new byte[0];
        try {
            request = joRequest.toString(4).getBytes(StandardCharsets.UTF_8);
        } catch (JSONException exc) {
            VoltDB.crashLocalVoltDB("unable to coalesce snapshot request", true, exc);
        }
        try {
            List<Integer> shed = ImmutableList.copyOf(discerners.keySet());
            m_snapshotQueue.coalesceNodesWith(fileType, shed, request, requestId);
        } catch (KeeperException | InterruptedException exc) {
            VoltDB.crashLocalVoltDB("unable to save colesced node modifications to zk", true, exc);
        }

        return request;
    }

    private String queueRequestNode(SnapshotInitiationInfo snapInfo) {
        SNAPSHOT_TYPE stype = snapInfo.isTruncationRequest() ?
                SNAPSHOT_TYPE.LOG : SNAPSHOT_TYPE.valueOf(snapInfo.getFormat());

        String jsonStr = null;
        try {
            jsonStr = snapInfo.getJSONObjectForZK().toString(4);
        } catch (JSONException exc) {
            VoltDB.crashLocalVoltDB("unable to serialize snapshot request for zk", true, exc);
        }
        int id = -1;
        try {
            id = m_snapshotQueue.requestSnapshot(stype, jsonStr.getBytes(StandardCharsets.UTF_8));
        } catch (KeeperException | InterruptedException exc) {
            VoltDB.crashLocalVoltDB("unable to queue snapshot request: \n" + jsonStr, true, exc);
        }
        if (stype.isDefaultType()) {
            stype = SNAPSHOT_TYPE.FILE;
        }
        return stype.requestId(id).name();
    }

    private final static byte[] TRUNCATION_REQUEST_STUB =
            "{\"isTruncation\":true}".getBytes(StandardCharsets.UTF_8);

    public String queueTruncationRequest() {
        int id = -1;
        try {
            id = m_snapshotQueue.requestSnapshot(SNAPSHOT_TYPE.LOG, TRUNCATION_REQUEST_STUB);
        } catch (KeeperException | InterruptedException exc) {
            VoltDB.crashLocalVoltDB("unable to queue truncation snapshot request", true, exc);
        }
        return SNAPSHOT_TYPE.FILE.requestId(id).name();
    }

    /**
     * Try to create the ZK node to request the snapshot.
     *
     * @param snapInfo SnapshotInitiationInfo object with the requested snapshot initiation settings
     * @return The request ID if succeeded, otherwise null.
     */
    private String createRequestNode(SnapshotInitiationInfo snapInfo)
    {
        String requestId = null;

        try {
            requestId = java.util.UUID.randomUUID().toString();
            if (!snapInfo.isTruncationRequest()) {
                final JSONObject jsObj = snapInfo.getJSONObjectForZK();
                jsObj.put("requestId", requestId);
                String zkString = jsObj.toString(4);
                byte zkBytes[] = zkString.getBytes("UTF-8");
                // TRAIL [RequestSnap:3] writes  the request JSON to one ZookeperRequest
                m_zk.create(VoltZK.user_snapshot_request, zkBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            else {
                m_zk.create(VoltZK.request_truncation_snapshot_node, null,
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            }
        // TRAIL [RequestSnap:4] it throws if it already exists
        // TRAIL [ImplementationHints:2] queue insertion here???
        } catch (KeeperException.NodeExistsException e) {
            return null;
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Exception while attempting to create user snapshot request in ZK", true, e);
        }

        return requestId;
    }

    private void registerUserSnapshotResponseWatch(
            final String requestId,
            final long clientHandle,
            final Connection c,
            final boolean notifyChanges) throws Exception {
        final String responseNode = VoltZK.user_snapshot_response + requestId;
        Stat exists = m_zk.exists(responseNode, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                if (event.getState() == KeeperState.Disconnected) return;
                switch (event.getType()) {
                case NodeCreated:
                    m_es.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processUserSnapshotRequestResponse(
                                            event,
                                            clientHandle,
                                            c,
                                            notifyChanges);
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB(
                                        "Error retrieving user snapshot request response from ZK",
                                        true,
                                        e);
                            }
                        }
                    });
                    break;
                default:
                }
            }
        });

        if (exists != null) {
            processUserSnapshotRequestResponse(
                    new WatchedEvent(
                        EventType.NodeCreated,
                        KeeperState.SyncConnected,
                        responseNode),
                        clientHandle,
                        c,
                        notifyChanges);
        }
    }

    void processUserSnapshotRequestResponse(
            final WatchedEvent event,
            final long clientHandle,
            final Connection c,
            final boolean notifyChanges) throws Exception {
        byte responseBytes[] = m_zk.getData(event.getPath(), false, null);
        try {
            m_zk.delete(event.getPath(), -1, null, null);
        } catch (Exception e) {
            SNAP_LOG.error("Error cleaning up user snapshot request response in ZK", e);
        }
        ByteBuffer buf = ByteBuffer.wrap(responseBytes);
        ClientResponseImpl response = new ClientResponseImpl();
        response.initFromBuffer(buf);
        response.setClientHandle(clientHandle);

        // Not sure if we need to preserve the original byte buffer here, playing it safe
        ByteBuffer buf2 = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf2.putInt(buf2.capacity() - 4);
        response.flattenToBuffer(buf2).flip();
        c.writeStream().enqueue(buf2);

        /*
         * If the caller wants to be notified of final results for the snapshot
         * request, set up a watcher only if the snapshot is queued.
         */
        if (notifyChanges && (response.getStatus() == ClientResponse.SUCCESS) &&
            SnapshotUtil.isSnapshotQueued(response.getResults())) {
            Watcher watcher = new Watcher() {
                @Override
                public void process(final WatchedEvent event) {
                    if (event.getState() == KeeperState.Disconnected) return;
                    switch (event.getType()) {
                    case NodeCreated:
                        m_es.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    processUserSnapshotRequestResponse(
                                                event,
                                                clientHandle,
                                                c,
                                                false);
                                } catch (Exception e) {
                                    VoltDB.crashLocalVoltDB(
                                                "Error retrieving user snapshot request response from ZK",
                                                true,
                                                e);
                                }
                            }
                        });
                        break;
                    default:
                    }
                }
            };

            // Set the watcher
            if (m_zk.exists(event.getPath(), watcher) != null) {
                processUserSnapshotRequestResponse(event, clientHandle, c, false);
            }
        }
    }

    @Override
    public CountDownLatch snapshotCompleted(final SnapshotCompletionEvent event) {
        JSONObject jo;
        try {
            jo = getActiveSnapshot();
            if (jo != null && event.requestId.equals(jo.getString("requestIdAtHead"))) {
                m_snapshotQueue.removeRequestIdNode(event.requestId);
                m_snapshotQueue.lockDistributedLockAndProcessCompletedSnapshot();
            }
        }
        catch (JSONException e1) {
            //  Auto-generated catch block
            e1.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        if (!event.truncationSnapshot || !event.didSucceed) {
            return new CountDownLatch(0);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    TruncationSnapshotAttempt snapshotAttempt = m_truncationSnapshotAttempts.get(event.multipartTxnId);
                    if (snapshotAttempt == null) {
                        snapshotAttempt = new TruncationSnapshotAttempt();
                        snapshotAttempt.path = event.path;
                        snapshotAttempt.nonce = event.nonce;
                        m_truncationSnapshotAttempts.put(event.multipartTxnId, snapshotAttempt);
                    }
                    snapshotAttempt.finished = true;
                    groomTruncationSnapshots();
                } finally {
                    latch.countDown();
                }
            }
        });
        return latch;
    }

}
