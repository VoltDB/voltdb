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

package org.voltcore.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.zk.CoreZK;
import org.voltdb.StartAction;

public class TestHostMessenger {

    private static final ArrayList<HostMessenger> createdMessengers = new ArrayList<HostMessenger>();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        for (HostMessenger hm : createdMessengers) {
            hm.shutdown();
        }
        createdMessengers.clear();
    }

    private HostMessenger createHostMessenger(int index, StartAction action) throws Exception {
        return createHostMessenger(index, action, true);
    }

    private HostMessenger createHostMessenger(int index, StartAction action, boolean start) throws Exception {
        HostMessenger.Config config = new HostMessenger.Config();
        config.internalPort = config.internalPort + index;
        config.zkInterface = "127.0.0.1:" + (2181 + index);
        HostMessenger hm = new HostMessenger(config);
        createdMessengers.add(hm);
        if (start) {
            hm.start();
        }
        return hm;
    }

    @Test
    public void testSingleHost() throws Exception {
        HostMessenger hm = createHostMessenger(0, StartAction.CREATE);

        Mailbox m1 = hm.createMailbox();

        SiteMailbox sm = new SiteMailbox(hm, (-2L << 32));

        hm.createMailbox(sm.getHSId(), sm);

        sm.send(m1.getHSId(), new LocalObjectMessage(null));
        m1.send(sm.getHSId(), new LocalObjectMessage(null));

        LocalObjectMessage lom = (LocalObjectMessage)m1.recv();
        assertEquals(lom.m_sourceHSId, sm.getHSId());

        lom =  (LocalObjectMessage)sm.recv();
        assertEquals(lom.m_sourceHSId, m1.getHSId());
    }

    @Test
    public void testMultiHost() throws Exception {
        HostMessenger hm1 = createHostMessenger(0, StartAction.CREATE);

        final HostMessenger hm2 = createHostMessenger(1, StartAction.CREATE, false);

        final HostMessenger hm3 = createHostMessenger(2, StartAction.CREATE, false);

        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        Thread hm2Start = new Thread() {
            @Override
            public void run() {
                try {
                    hm2.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    exception.set(e);
                }
            }
        };
        Thread hm3Start = new Thread() {
            @Override
            public void run() {
                try {
                    hm3.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    exception.set(e);
                }
            }
        };

        hm2Start.start();
        hm3Start.start();
        hm2Start.join();
        System.out.println(hm2.getZK().getChildren(CoreZK.hostids, false ));
        hm3Start.join();

        if (exception.get() != null) {
            fail(exception.get().toString());
        }

        List<String> root1 = hm1.getZK().getChildren("/", false );
        List<String> root2 = hm2.getZK().getChildren("/", false );
        List<String> root3 = hm3.getZK().getChildren("/", false );
        System.out.println(root1);
        System.out.println(root2);
        System.out.println(root3);
        assertTrue(root1.equals(root2));
        assertTrue(root2.equals(root3));

        List<String> hostids1 = hm1.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids2 = hm2.getZK().getChildren(CoreZK.hostids, false );
        List<String> hostids3 = hm3.getZK().getChildren(CoreZK.hostids, false );
        System.out.println(hostids1);
        System.out.println(hostids2);
        System.out.println(hostids3);
        assertTrue(hostids1.equals(hostids2));
        assertTrue(hostids2.equals(hostids3));

        List<String> hosts3;
        List<String> hosts1;
        hm2.shutdown();
        boolean success = false;
        for (int ii = 0; ii < (200 / 5); ii++) {
            hosts3 = hm3.getZK().getChildren(CoreZK.hosts, false );
            hosts1 = hm1.getZK().getChildren(CoreZK.hosts, false );
            if (hosts3.size() == 2 && hosts1.size() == 2 && hosts1.equals(hosts3)) {
                success = true;
                break;
            }
            Thread.sleep(5);
        }
        assertTrue(success);

        hm1.waitForGroupJoin(2);
        hm3.waitForGroupJoin(2);
    }

}
