/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
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

        public Message(long src, long dest, VoltMessage msg)
        {
            m_src = src;
            m_dest = dest;
            m_msg = msg;
        }
    }

    private Map<Long, Queue<Message>> m_sendQs = new HashMap<Long, Queue<Message>>();
    private Map<Long, Queue<Message>> m_recvQs = new HashMap<Long, Queue<Message>>();
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

    void shutdown()
    {
        m_shutdown.set(true);
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
                        if (m_recvQs.containsKey(msg.m_dest)) {
                            m_recvQs.get(msg.m_dest).offer(msg);
                        }
                    }
                }
            }
        }
    }
}
