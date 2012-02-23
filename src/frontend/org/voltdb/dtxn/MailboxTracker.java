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

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.EventType;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.agreement.ZKUtil;
import org.voltcore.agreement.ZKUtil.ByteArrayCallback;
import org.voltcore.utils.MiscUtils;

import org.voltdb.MailboxNodeContent;
import org.voltdb.VoltZK;
import org.voltdb.VoltZK.MailboxType;

public class MailboxTracker {
    private final ZooKeeper m_zk;
    private final MailboxUpdateHandler m_handler;
    private final ExecutorService m_es =
            Executors.newSingleThreadExecutor(MiscUtils.getThreadFactory("Mailbox tracker", 1024 * 128));

    private byte m_lastChecksum[] = null;

    private class EventTask implements Runnable {
        private final WatchedEvent m_event;

        public EventTask(WatchedEvent event) {
            m_event = event;
        }

        @Override
        public void run() {
            try {
                getMailboxes(m_event);
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB("Error in mailbox tracker", false, e);
            }
        }
    }

    private final Watcher m_watcher = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            try {
                m_es.submit(new EventTask(event));
            } catch (RejectedExecutionException e) {
                if (m_es.isShutdown()) {
                    return;
                } else {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

    public MailboxTracker(ZooKeeper zk, MailboxUpdateHandler handler) {
        m_zk = zk;
        m_handler = handler;
    }

    public void start() throws InterruptedException, ExecutionException {
        m_es.submit(new EventTask(null)).get();
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdown();
        m_es.awaitTermination(356, TimeUnit.DAYS);
    }

    private void getMailboxes(WatchedEvent event) throws Exception {

        Set<String> mailboxes;

        /*
         * Only set the watch the first time (null) or again if it has been fired
         */
        if (event == null || event.getPath().equals(VoltZK.mailboxes)){
            mailboxes = new TreeSet<String>(m_zk.getChildren(VoltZK.mailboxes, m_watcher));
        } else {
            /*
             * There is no need to do a rebuild if this was a delete. The watch
             * on the parent node will fire because the children changed and the processing will be done then.
             */
            if (event != null && event.getType() == EventType.NodeDeleted) {
                return;
            }
            mailboxes = new TreeSet<String>(m_zk.getChildren(VoltZK.mailboxes, false));
        }

        List<ByteArrayCallback> callbacks = new ArrayList<ByteArrayCallback>();
        for (String mailboxSet : mailboxes) {
            ByteArrayCallback cb = new ByteArrayCallback();

            /*
             * Only set the watch the first time (null) or again if it has been fired
             */
            if (event == null || event.getPath().equals(VoltZK.mailboxes + "/" + mailboxSet)) {
                m_zk.getData(ZKUtil.joinZKPath(VoltZK.mailboxes, mailboxSet), m_watcher, cb, null);
            } else {
                m_zk.getData(ZKUtil.joinZKPath(VoltZK.mailboxes, mailboxSet), m_watcher, cb, null);
            }

            callbacks.add(cb);
        }

        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        Map<MailboxType, List<MailboxNodeContent>> mailboxMap = new HashMap<MailboxType, List<MailboxNodeContent>>();
        for (ByteArrayCallback callback : callbacks) {
            try {
                byte payload[] = callback.getData();
                digest.update(payload);
                JSONObject jsObj = new JSONObject(new String(payload, "UTF-8"));
                readContents(jsObj, mailboxMap);
            } catch (KeeperException.NoNodeException e) {}
        }
        byte digestBytes[] = digest.digest();

        if (m_lastChecksum != null && Arrays.equals( m_lastChecksum, digestBytes)) {
            return;
        }
        m_lastChecksum = digestBytes;
        m_handler.handleMailboxUpdate(mailboxMap);
    }

    private void readContents(JSONObject obj, Map<MailboxType, List<MailboxNodeContent>> mailboxMap)
        throws Exception {
        @SuppressWarnings("unchecked")
        Iterator<String> keys = obj.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            MailboxType type = MailboxType.valueOf(key);

            List<MailboxNodeContent> mailboxes = mailboxMap.get(type);
            if (mailboxes == null) {
                mailboxes = new ArrayList<MailboxNodeContent>();
                mailboxMap.put(type, mailboxes);
            }

            JSONArray mailboxObjects = obj.getJSONArray(key);
            for (int ii = 0; ii < mailboxObjects.length(); ii++) {
                JSONObject mailboxDescription = mailboxObjects.getJSONObject(ii);
                long hsId = mailboxDescription.getLong("HSId");
                Integer partitionId = null;
                if (mailboxDescription.has("partitionId")) {
                    partitionId = mailboxDescription.getInt("partitionId");
                }
                mailboxes.add(new MailboxNodeContent(hsId, partitionId));
            }
        }
    }
}
