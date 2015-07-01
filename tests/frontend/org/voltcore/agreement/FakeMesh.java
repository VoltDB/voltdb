/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltcore.agreement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

public class FakeMesh extends Thread
{
    private static VoltLogger meshLog = new VoltLogger("FAKEMESH");

    public static class Message
    {
        public final long m_src;
        public final long m_dest;
        public final VoltMessage m_msg;
        public final boolean m_close;

        public Message(long src, long dest, VoltMessage msg)
        {
            m_src = src;
            m_dest = dest;
            m_msg = msg;
            m_close = false;
        }

        public Message(long src, long dest, boolean close) {
            m_src = src;
            m_dest = dest;
            m_msg = null;
            m_close = true;
        }
    }

    private Map<Long, Queue<Message>> m_sendQs = new HashMap<Long, Queue<Message>>();
    private Map<Long, Queue<Message>> m_recvQs = new HashMap<Long, Queue<Message>>();
    private Map<Long, Set<Long>> m_failedLinks = new HashMap<Long, Set<Long>>();
    private AtomicBoolean m_shutdown = new AtomicBoolean(false);

    FakeMesh()
    {
    }

    synchronized void registerNode(long HSId, Queue<Message> sendQ, Queue<Message> recvQ)
    {
        if (m_sendQs.containsKey(HSId) || m_recvQs.containsKey(HSId)) {
            meshLog.error("Queue already registered for HSID: " + HSId + ", bailing");
            return;
        }
        m_sendQs.put(HSId, sendQ);
        m_recvQs.put(HSId, recvQ);
    }

    synchronized void unregisterNode(long HSId)
    {
        meshLog.info("Unregistering HSId: " + CoreUtils.hsIdToString(HSId));
        m_sendQs.remove(HSId);
        m_recvQs.remove(HSId);
    }

    synchronized void failLink(long src, long dst)
    {
        if (internalFailLink(src, dst)) {
            meshLog.info("Failing link between source: " + CoreUtils.hsIdToString(src) +
                    " and destination: " + CoreUtils.hsIdToString(dst));
        }
    }

    synchronized private boolean internalFailLink(long src,long dst) {
        Set<Long> dsts = m_failedLinks.get(src);
        if (dsts == null) {
            dsts = new HashSet<Long>();
            m_failedLinks.put(src, dsts);
        }
        return dsts.add(dst);
    }

    synchronized void closeLink(long src, long dst) {
        meshLog.info("Close link between source: " + CoreUtils.hsIdToString(src) +
                " and destination: " + CoreUtils.hsIdToString(dst));
        if (m_recvQs.containsKey(dst)) {
            m_recvQs.get(dst).offer(new Message(src,dst,true));
        }
        internalFailLink(src,dst);
        internalFailLink(dst,src);
    }

    private boolean linkFailed(long src, long dst)
    {
        Set<Long> dsts = m_failedLinks.get(src);
        if (dsts == null || !dsts.contains(dst)) {
            return false;
        }
        return true;
    }

    void shutdown()
    {
        m_shutdown.set(true);
    }

    @Override
    public void start()
    {
        setName("FakeMesh");
        super.start();
    }

    @Override
    public void run()
    {
        while (!m_shutdown.get()) {
            // blunt-force trauma concurrency
            synchronized(this) {
                for (Entry<Long, Queue<Message>> sq : m_sendQs.entrySet()) {
                    Message msg = sq.getValue().poll();
                    if (msg != null) {
                        if (m_recvQs.containsKey(msg.m_dest) && !linkFailed(msg.m_src, msg.m_dest)) {
                            m_recvQs.get(msg.m_dest).offer(msg);
                        }
                    }
                }
            }
        }
    }
}
