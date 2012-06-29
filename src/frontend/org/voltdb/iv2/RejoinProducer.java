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

package org.voltdb.iv2;

import java.lang.reflect.Constructor;

import java.net.InetAddress;

import java.nio.ByteBuffer;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;

import org.voltcore.utils.Pair;

import org.voltdb.client.ClientResponse;

import org.voltdb.ClientResponseImpl;

import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;

import org.voltdb.PrivateVoltTableFactory;

import org.voltdb.rejoin.RejoinSiteProcessor;

import org.voltdb.SnapshotFormat;

import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SnapshotResponseHandler;

import org.voltdb.utils.MiscUtils;

import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;

/**
 * Manages the lifecycle of snapshot serialization to a site
 * for the purposes of rejoin.
 */
public class RejoinProducer extends SiteTasker
{
    private final SiteTaskerQueue m_taskQueue;
    private final int m_partitionId;
    private InitiatorMailbox m_mailbox;
    private long m_rejoinCoordinatorHsId;
    private RejoinSiteProcessor m_rejoinSiteProcessor;

    /*
     * The handler will be called when a SnapshotUtil.requestSnapshot response comes
     * back. It could potentially take a long time to successfully queue the
     * snapshot request, or it may fail.
     *
     * This runs on the snapshot daemon thread.
     */
    SnapshotResponseHandler m_handler = new SnapshotResponseHandler() {
        @Override
        public void handleResponse(ClientResponse resp) {
            if (resp == null) {
                VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot",
                        false, null);
            } else if (resp.getStatus() != ClientResponseImpl.SUCCESS) {
                VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot: " +
                        resp.getStatusString(), false, null);
            }

            VoltTable[] results = resp.getResults();
            if (SnapshotUtil.didSnapshotRequestSucceed(results)) {
                long txnId = -1;
                String appStatus = resp.getAppStatusString();
                if (appStatus == null) {
                    VoltDB.crashLocalVoltDB("Rejoin snapshot request failed: " +
                            resp.getStatusString(), false, null);
                }

                try {
                    JSONObject jsObj = new JSONObject(appStatus);
                    txnId = jsObj.getLong("txnId");
                } catch (JSONException e) {
                    VoltDB.crashLocalVoltDB("Failed to get the rejoin snapshot txnId",
                            true, e);
                    return;
                }

                // Send a message to self to avoid synchronization
                RejoinMessage msg = new RejoinMessage(txnId);
                m_mailbox.send(m_mailbox.getHSId(), msg);
            } else {
                VoltDB.crashLocalVoltDB("Snapshot request for rejoin failed",
                        false, null);
            }
        }
    };

    // m_handler sends this message when a non-blocking snapshot is
    // completed.
    void doRequestResponse(RejoinMessage message)
    {
        /*
            m_rejoinSnapshotTxnId = rm.getSnapshotTxnId();
            if (m_rejoinTaskLog != null) {
                m_rejoinTaskLog.setEarliestTxnId(m_rejoinSnapshotTxnId);
            }
            VoltDB.instance().getSnapshotCompletionMonitor()
                  .addInterest(m_snapshotCompletionHandler);
        */
        RejoinMessage snap_complete = new RejoinMessage(m_mailbox.getHSId(), Type.SNAPSHOT_FINISHED);
        RejoinMessage replay_complete = new RejoinMessage(m_mailbox.getHSId(), Type.REPLAY_FINISHED);
        m_mailbox.send(m_rejoinCoordinatorHsId, snap_complete);
        m_mailbox.send(m_rejoinCoordinatorHsId, replay_complete);
    }

    public RejoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        m_partitionId = partitionId;
        m_taskQueue = taskQueue;
    }

    public void setMailbox(InitiatorMailbox mailbox)
    {
        m_mailbox = mailbox;
    }

    public void deliver(RejoinMessage message)
    {
        if (message.getType() == RejoinMessage.Type.INITIATION) {
            doInitiation(message);
        }
        else if (message.getType() == RejoinMessage.Type.REQUEST_RESPONSE) {
            doRequestResponse(message);
        }
        else {
            VoltDB.crashLocalVoltDB("Unknown rejoin message type: " +
                    message.getType(), false, null);
        }
    }

    final AtomicReference<Pair<Integer, ByteBuffer>> firstWork =
        new AtomicReference<Pair<Integer, ByteBuffer>>();

    void doInitiation(RejoinMessage message)
    {
        m_rejoinCoordinatorHsId = message.m_sourceHSId;
        m_rejoinSiteProcessor = makeSnapshotProcessor();

        // MUST choose the leader as the source.
        long sourceSite = m_mailbox.getMasterHsId(m_partitionId);
        Pair<List<byte[]>, Integer> endPoints = m_rejoinSiteProcessor.initialize();

        // Initiate a snapshot with stream snapshot target
        String data = makeSnapshotRequest(endPoints, sourceSite);
        String nonce = "Rejoin_" + m_mailbox.getHSId() + "_" + System.currentTimeMillis();

        // request a blocking snapshot.
        SnapshotUtil.requestSnapshot(0l, "", nonce, true,
                                     SnapshotFormat.STREAM, data, m_handler);

        // There are problems here, Chester. First, the site thread needs
        // to stay unblocked until the first data block is available. So,
        // do a messy blocking poll on the first unit of work (by spinning!).
        // Safe that first unit in "firstWork" and then enter the taskQueue.
        // Future: need a blocking peek on rejoinSiteProcessor(); then firstWork
        // can go away and the weird special casing of the first block in
        // run() can also go away.
        Thread firstSnapshotBlock = new Thread() {
            @Override
            public void run()
            {
                try {
                Thread.sleep(30000);
                } catch (Exception e) { }
                Pair<Integer, ByteBuffer> rejoinWork;
                do {
                    rejoinWork = m_rejoinSiteProcessor.poll();
                    if (rejoinWork != null) {
                        firstWork.set(rejoinWork);
                    }
                }
                while (rejoinWork == null);

                // run the rejoin task (ourself) in the Site
                m_taskQueue.offer(RejoinProducer.this);
            }
        };
        firstSnapshotBlock.start();
    }

    private RejoinSiteProcessor makeSnapshotProcessor()
    {
        RejoinSiteProcessor rejoinSiteProcessor = null;
        Class<?> klass =
                MiscUtils.loadProClass("org.voltdb.rejoin.StreamSnapshotSink",
                                       "Rejoin", false);
        Constructor<?> constructor;
        try {
            constructor = klass.getConstructor(long.class);
            rejoinSiteProcessor = (RejoinSiteProcessor) constructor.newInstance(m_mailbox.getHSId());
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to construct stream snapshot receiver",
                                    true, e);
        }
        return rejoinSiteProcessor;
    }

    private String makeSnapshotRequest(Pair<List<byte[]>, Integer> endPoints, long sourceSite)
    {
        List<byte[]> addresses = endPoints.getFirst();
        int port = endPoints.getSecond();

        try {
            JSONStringer jsStringer = new JSONStringer();
            jsStringer.object();
            jsStringer.key("addresses").array();
            for (byte[] addr : addresses) {
                InetAddress inetAddress = InetAddress.getByAddress(addr);
                jsStringer.value(inetAddress.getHostAddress());
            }
            jsStringer.endArray();
            jsStringer.key("port").value(port);
            // make this snapshot only contain data from this site
            // m_recoveryLog.debug("Rejoin source for site " + CoreUtils.hsIdToString(getSiteId()) +
            //                   " is " + CoreUtils.hsIdToString(sourceSite));
            jsStringer.key("target_hsid").value(sourceSite);
            jsStringer.endObject();
            return jsStringer.toString();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to serialize to JSON", true, e);
        }
        // unreachable;
        return null;
    }

    /**
     * SiteTasker run -- load this site!
     */
    @Override
    public void run(SiteProcedureConnection siteConnection) {
        restoreBlock(firstWork.get(), siteConnection);

        while (!m_rejoinSiteProcessor.isEOF()) {
            Pair<Integer, ByteBuffer> rejoinWork = m_rejoinSiteProcessor.poll();
            if (rejoinWork != null) {
                restoreBlock(rejoinWork, siteConnection);
            }
        }
        // m_recoveryLog.debug("Rejoin snapshot transfer is finished");
        m_rejoinSiteProcessor.close();
        // m_rejoinSnapshotBytes = m_rejoinSiteProcessor.bytesTransferred();
        // m_rejoinSiteProcessor = null;
        // m_taskExeStartTime = System.currentTimeMillis();
        /*
         * Don't notify the rejoin coordinator yet. The stream snapshot may
         * have not finished on all nodes, let the snapshot completion
         * monitor tell the rejoin coordinator.
         */
        siteConnection.setRejoinComplete();
    }

    void restoreBlock(Pair<Integer, ByteBuffer> rejoinWork, SiteProcedureConnection siteConnection)
    {
        int tableId = rejoinWork.getFirst();
        ByteBuffer buffer = rejoinWork.getSecond();
        VoltTable table =
            PrivateVoltTableFactory.createVoltTableFromBuffer(buffer.duplicate(),
                    true);
        //m_recoveryLog.info("table " + tableId + ": " + table.toString());
        // Currently, only export cares about this TXN ID.  Since we don't have one handy, and IV2
        // doesn't yet care about export, just use Long.MIN_VALUE
        siteConnection.loadTable(Long.MIN_VALUE, tableId, table);
    }


    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection) {
        run(siteConnection);
    }

    @Override
    public int priority() {
        return 0;
    }
}
