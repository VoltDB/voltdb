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

package org.voltdb;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import junit.framework.TestCase;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientConfigForTest;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.dtxn.MailboxPublisher;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.rejoinfuzz.NonOrgVoltDBProc;

public class RejoinTestBase extends TestCase {
    protected static final ClientConfig m_cconfig = new ClientConfigForTest("ry@nlikesthe", "y@nkees");

    static final Class<?>[] PROCEDURES = { NonOrgVoltDBProc.class };

    VoltProjectBuilder getBuilderForTest() throws UnsupportedEncodingException {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));\n" +
            "create table blah_replicated (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));" +
            "create table PARTITIONED (" +
            "pkey bigint default 0 not null, " +
            "value bigint default 0 not null, " +
            "PRIMARY KEY(pkey));" +
            "create table PARTITIONED_LARGE (" +
            "pkey bigint default 0 not null, " +
            "value bigint default 0 not null, " +
            "data VARCHAR(512) default null," +
            "PRIMARY KEY(pkey));" +
            "create table TEST_INLINED_STRING (" +
            "pkey integer default 0 not null, " +
            "value VARCHAR(36) default 0 not null, " +
            "value1 VARCHAR(17700) default 0 not null, " +
            "PRIMARY KEY(pkey));" +
            "CREATE TABLE ENG798 (" +
            "    c1 VARCHAR(16) NOT NULL," +
            "    c2 BIGINT DEFAULT 0 NOT NULL," +
            "    c3 VARCHAR(36) DEFAULT '' NOT NULL," +
            "    c4 VARCHAR(36)," +
            "    c5 TIMESTAMP," +
            "    c6 TINYINT DEFAULT 0," +
            "    c7 TIMESTAMP" +
            ");" +
            "CREATE VIEW V_ENG798(c1, c6, c2, c3, c4, c5, total) " +
            "    AS SELECT c1, c6, c2, c3, c4, c5, COUNT(*)" +
            "    FROM ENG798 " +
            "    GROUP BY c1, c6, c2, c3, c4, c5;";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("blah", "ival");
        builder.addPartitionInfo("PARTITIONED", "pkey");
        builder.addPartitionInfo("PARTITIONED_LARGE", "pkey");
        builder.addPartitionInfo("TEST_INLINED_STRING", "pkey");
        builder.addPartitionInfo("ENG798", "C1");

        GroupInfo gi = new GroupInfo("foo", true, true);
        builder.addGroups(new GroupInfo[] { gi } );
        UserInfo ui = new UserInfo( "ry@nlikesthe", "y@nkees", new String[] { "foo" } );
        builder.addUsers(new UserInfo[] { ui } );

        ProcedureInfo[] pi = new ProcedureInfo[] {
            new ProcedureInfo(new String[] { "foo" }, "InsertInlinedString", "insert into TEST_INLINED_STRING values (?, ?, ?);", "TEST_INLINED_STRING.pkey:0"),
            new ProcedureInfo(new String[] { "foo" }, "Insert", "insert into blah values (?);", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertSinglePartition", "insert into blah values (?);", "blah.ival:0"),
            new ProcedureInfo(new String[] { "foo" }, "InsertReplicated", "insert into blah_replicated values (?);", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlahSinglePartition", "select * from blah where ival = ?;", "blah.ival:0"),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlah", "select * from blah where ival = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlahReplicated", "select * from blah_replicated where ival = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertPartitioned", "insert into PARTITIONED values (?, ?);", "PARTITIONED.pkey:0"),
            new ProcedureInfo(new String[] { "foo" }, "UpdatePartitioned", "update PARTITIONED set value = ? where pkey = ?;", "PARTITIONED.pkey:1"),
            new ProcedureInfo(new String[] { "foo" }, "SelectPartitioned", "select * from PARTITIONED;", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertPartitionedLarge", "insert into PARTITIONED_LARGE values (?, ?, ?);", "PARTITIONED_LARGE.pkey:0")
        };

        builder.addProcedures(pi);
        builder.addProcedures(PROCEDURES);

        return builder;
    }

    static class Context {
        long catalogCRC;
        ServerThread localServer;
    }

    Context getServerReadyToReceiveNewNode() throws Exception {
        Context retval = new Context();

        VoltProjectBuilder builder = getBuilderForTest();
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("rejoin.jar"), 1, 2, 1, 9998, false);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_adminPort = 9998;
        config.m_isRejoinTest = true;
        retval.localServer = new ServerThread(config);

        //long deploymentCRC = CatalogUtil.getDeploymentCRC(builder.getPathToDeployment());

        // start the fake HostMessenger
        InMemoryJarfile jarFile = new InMemoryJarfile(Configuration.getPathToCatalogForTest("rejoin.jar"));
        retval.catalogCRC = jarFile.getCRC();
        HostMessenger.Config config2 = new HostMessenger.Config();
        config2.internalPort++;
        config2.zkInterface = "127.0.0.1:2182";
        HostMessenger host2 = new HostMessenger(config2);
        retval.localServer.start();
        while (VoltDB.instance().getHostMessenger() != null) {
            Thread.sleep(1);
        }
        Thread.sleep(200);
        host2.start();
        host2.waitForGroupJoin(2);

        MailboxPublisher publisher = new MailboxPublisher(VoltZK.mailboxes + "/1");
        Mailbox mbox = host2.createMailbox();
        Mailbox mbox2 = host2.createMailbox();
        publisher.registerMailbox(MailboxType.ExecutionSite, new MailboxNodeContent(mbox.getHSId(), 0));
        publisher.registerMailbox(MailboxType.Initiator, new MailboxNodeContent(mbox2.getHSId(), 0));
        publisher.publish(host2.getZK());
        VoltZK.createPersistentZKNodes(host2.getZK());
        host2.getZK().create(
                VoltZK.cluster_metadata + "/" + host2.getHostId(),
                "{ interfaces : [ \"localhost\"], clientPort : 23, adminPort : 43, httpPort : 32 }".getBytes("UTF-8"),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        host2.waitForAllHostsToBeReady(2);

        HostMessenger host1 = VoltDB.instance().getHostMessenger();

        host2.closeForeignHostSocket(host1.getHostId());
        // this is just to wait for the fault manager to kick in
        Thread.sleep(50);
        host2.shutdown();
        // this is just to wait for the fault manager to kick in
        Thread.sleep(50);

        // wait until the fault manager has kicked in

        for (int i = 0; host1.countForeignHosts() > 0; i++) {
            if (i > 10) fail();
            Thread.sleep(50);
        }
        assertEquals(0, host1.countForeignHosts());

        return retval;
    }
}
