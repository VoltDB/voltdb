/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.dtxn;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltcore.zk.ZKTestBase;
import org.voltdb.MailboxNodeContent;
import org.voltdb.VoltZK;
import org.voltdb.VoltZK.MailboxType;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestMailboxTracker extends ZKTestBase {
    private final MailboxUpdateHandler handler = mock(MailboxUpdateHandler.class);

    @Before
    public void setUp() throws Exception {
        setUpZK(1);
    }

    @After
    public void tearDown() throws Exception {
        tearDownZK();
        reset(handler);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMailboxTracker() throws Exception {
        ZooKeeper zk = getClient(0);
        MailboxTracker tracker = new MailboxTracker(zk, handler);

        VoltZK.createPersistentZKNodes(zk);
        String data1 = "{HSId: 1, partitionId: 0}";
        zk.create(VoltZK.mailboxes_executionsites_site, data1.getBytes(),
                  Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String data2 = "{HSId: 2, partitionId: 1}";
        zk.create(VoltZK.mailboxes_executionsites_site, data2.getBytes(),
                  Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // start the mailbox tracker and watch all the changes
        tracker.start();
        @SuppressWarnings("rawtypes")
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        verify(handler).handleMailboxUpdate(captor.capture());
        Map<MailboxType, List<MailboxNodeContent>> value = captor.getValue();
        assertTrue(value.containsKey(MailboxType.ExecutionSite));
        List<MailboxNodeContent> list = value.get(MailboxType.ExecutionSite);
        assertEquals(2, list.size());
        MailboxNodeContent node1 = list.get(0);
        assertEquals(1, node1.HSId.longValue());
        assertEquals(0, node1.partitionId.intValue());
        MailboxNodeContent node2 = list.get(1);
        assertEquals(2, node2.HSId.longValue());
        assertEquals(1, node2.partitionId.intValue());
        tracker.shutdown();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testUpdate() throws Exception {
        ZooKeeper zk = getClient(0);
        ZooKeeper zk2 = getClient(0);
        MailboxTracker tracker = new MailboxTracker(zk, handler);

        VoltZK.createPersistentZKNodes(zk);
        String data1 = "{HSId: 1, partitionId: 0}";
        zk2.create(VoltZK.mailboxes_executionsites_site, data1.getBytes(),
                   Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String data2 = "{HSId: 2, partitionId: 1}";
        zk.create(VoltZK.mailboxes_executionsites_site, data2.getBytes(),
                  Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        tracker.start();

        // The ephemaral node just created will disappear and we should get an update
        zk2.close();
        // wait for the update
        Thread.sleep(50);

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        // should be called twice, once on start() and once on update
        verify(handler, times(2)).handleMailboxUpdate(captor.capture());
        List<Map> values = captor.getAllValues();
        Map<MailboxType, List<MailboxNodeContent>> value = values.get(1);
        assertTrue(value.containsKey(MailboxType.ExecutionSite));
        List<MailboxNodeContent> list = value.get(MailboxType.ExecutionSite);
        assertEquals(1, list.size());
        assertEquals(2, list.get(0).HSId.longValue());
        assertEquals(1, list.get(0).partitionId.intValue());
        tracker.shutdown();
    }
}
