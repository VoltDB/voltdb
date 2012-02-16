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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.VoltDB;
import org.voltcore.agreement.ZKUtil;
import org.voltcore.agreement.ZKUtil.ByteArrayCallback;
import org.voltcore.agreement.ZKUtil.ChildrenCallback;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.MiscUtils;

import org.voltdb.MailboxNodeContent;
import org.voltdb.VoltZK;
import org.voltdb.VoltZK.MailboxType;

public class MailboxTracker {
    private final ZooKeeper m_zk;
    private final MailboxUpdateHandler m_handler;
    private final ExecutorService m_es =
            Executors.newSingleThreadExecutor(MiscUtils.getThreadFactory("Mailbox tracker"));

    private final Runnable m_task = new Runnable() {
        @Override
        public void run() {
            try {
                getMailboxDirs();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error in mailbox tracker", false, e);
            }
        }
    };

    private final Watcher m_watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            try {
                m_es.submit(m_task);
            } catch (RejectedExecutionException e) {
                if (m_es.isShutdown()) {
                    return;
                } else {
                    VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

    public MailboxTracker(ZooKeeper zk, MailboxUpdateHandler handler) {
        m_zk = zk;
        m_handler = handler;
    }

    public void start() throws InterruptedException, ExecutionException {
        Future<?> submit = m_es.submit(m_task);
        submit.get();
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdown();
        m_es.awaitTermination(356, TimeUnit.DAYS);
    }

    private void getMailboxDirs() throws Exception {
        Map<MailboxType, List<MailboxNodeContent>> objects =
                new HashMap<VoltZK.MailboxType, List<MailboxNodeContent>>();

        List<String> mailboxes = m_zk.getChildren(VoltZK.mailboxes, false);
        Map<MailboxType, ChildrenCallback> callbacks = new HashMap<MailboxType, ChildrenCallback>();
        for (String dir : mailboxes) {
            MailboxType type = MailboxType.valueOf(dir);
            ChildrenCallback cb = new ChildrenCallback();
            m_zk.getChildren(ZKUtil.joinZKPath(VoltZK.mailboxes, dir), m_watcher, cb, null);
            callbacks.put(type, cb);
        }

        for (java.util.Map.Entry<MailboxType, ChildrenCallback> e : callbacks.entrySet()) {
            Object[] result = e.getValue().get();
            String dir = (String) result[1];
            @SuppressWarnings("unchecked")
            List<String> children = (List<String>) result[3];
            List<MailboxNodeContent> contents = readContents(dir, children);
            objects.put(e.getKey(), contents);
        }

        m_handler.handleMailboxUpdate(objects);
    }

    private List<MailboxNodeContent> readContents(String dir, List<String> children)
    throws Exception {
        ZKUtil.sortSequentialNodes(children);
        List<ByteArrayCallback> callbacks = new ArrayList<ByteArrayCallback>();
        for (String node : children) {
            String path = ZKUtil.joinZKPath(dir, node);
            ByteArrayCallback cb = new ByteArrayCallback();
            m_zk.getData(path, false, cb, null);
            callbacks.add(cb);
        }

        List<String> jsons = new ArrayList<String>();
        for (ByteArrayCallback cb : callbacks) {
            byte[] data = cb.getData();
            jsons.add(new String(data, "UTF-8"));
        }

        return VoltZK.parseMailboxContents(jsons);
    }
}
