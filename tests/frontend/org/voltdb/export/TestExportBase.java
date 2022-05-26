/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb_testprocs.regressionsuites.exportprocs.ExportInsertAllowNulls;
import org.voltdb_testprocs.regressionsuites.exportprocs.ExportInsertNoNulls;
import org.voltdb_testprocs.regressionsuites.exportprocs.ExportRollbackInsertNoNulls;

public class TestExportBase extends RegressionSuite {
    protected List<String> m_streamNames = new ArrayList<>();

    /** Shove a table name and pkey in front of row data */
    protected Object[] convertValsToParams(String tableName, final int i,
            final Object[] rowdata)
    {
        final Object[] params = new Object[rowdata.length + 2];
        params[0] = tableName;
        params[1] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            params[ii+2] = rowdata[ii];
        return params;
    }

    /** Push pkey into expected row data */
    protected Object[] convertValsToRow(final int i, final char op,
            final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 2];
        row[0] = (byte)(op == 'I' ? 1 : 0);
        row[1] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            row[ii+2] = rowdata[ii];
        return row;
    }

    /** Push pkey into expected row data */
    protected Object[] convertValsToLoaderRow(final int i, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 1];
        row[0] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            row[ii+1] = rowdata[ii];
        return row;
    }

    @Override
    public Client getClient() throws IOException {
        Client client = super.getClient();
        if (!client.waitForTopology(60_000)) {
            throw new IOException("Timed out waiting for topology info");
        }
        return client;
    }

    @Override
    public Client getAdminClient() throws IOException {
        Client client = super.getAdminClient();
        if (!client.waitForTopology(60_000)) {
            throw new IOException("Timed out waiting for topology info");
        }
        return client;
    }

    protected void quiesce(final Client client)
    throws Exception
    {
        client.drain();
        client.callProcedure("@Quiesce");
    }

    public static final RoleInfo GROUPS[] = new RoleInfo[] {
        new RoleInfo("export", false, false, false, false, false, false),
        new RoleInfo("proc", true, false, true, true, false, false),
        new RoleInfo("admin", true, false, true, true, false, false)
    };

    public static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("export", "export", new String[]{"export"}),
        new UserInfo("default", "password", new String[]{"proc"}),
        new UserInfo("admin", "admin", new String[]{"proc", "admin"})
    };

    /*
     * Test suite boilerplate
     */
    public static final ProcedureInfo[] NONULLS_PROCEDURES = {
        new ProcedureInfo(ExportInsertNoNulls.class, new ProcedurePartitionData ("NO_NULLS", "PKEY", "1"),
                new String[]{"proc"}),
        new ProcedureInfo(ExportRollbackInsertNoNulls.class,
                new ProcedurePartitionData ("NO_NULLS", "PKEY", "1"), new String[]{"proc"}),
    };

    public static final ProcedureInfo[] ALLOWNULLS_PROCEDURES = {
            new ProcedureInfo(ExportInsertAllowNulls.class,
                    new ProcedurePartitionData ("ALLOW_NULLS", "PKEY", "1"), new String[]{"proc"})
    };

    public TestExportBase(String s) {
        super(s);
    }

    /**
     * Wait for export processor to catch up and have nothing to be exported.
     *
     * @param client
     * @throws Exception
     */
    public void waitForExportAllRowsDelivered(Client client, List<String> streamNames) throws Exception {
        boolean passed = false;
        assertFalse(streamNames.isEmpty());
        Set<String> matchStreams = new HashSet<>(streamNames.stream().map(String::toUpperCase).collect(Collectors.toList()));

        //Quiesc to see all data flushed.
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = System.currentTimeMillis() + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                String source = stats.getString("SOURCE");
                Long tts = stats.getLong("TIMESTAMP");
                //Get highest timestamp and watch is change
                if (tts > ts) {
                    ts = tts;
                }
                if (0 != stats.getLong("TUPLE_PENDING")) {
                    passedThisTime = false;
                    System.out.println("Partition Not Zero. pendingTuples:"+stats.getLong("TUPLE_PENDING"));
                    break;
                }
                else {
                    matchStreams.remove(source);
                }
            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                //we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                System.out.println("Passed but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        System.out.println("Passed is: " + passed);
        //System.out.println(stats);
        assertTrue(passed && matchStreams.isEmpty());
    }

    public void setUp() throws Exception {
        super.setUp();
        m_streamNames = new ArrayList<>();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }
}
