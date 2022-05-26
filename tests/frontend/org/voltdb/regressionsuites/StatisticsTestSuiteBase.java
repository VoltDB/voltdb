/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.regressionsuites;

import com.google_voltpatches.common.collect.ImmutableMap;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder.ReuseServer;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.malicious.GoSleep;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class StatisticsTestSuiteBase extends SaveRestoreBase {

    protected final static int SITES = 2;
    protected final static int HOSTS = 3;
    protected final static int KFACTOR = 1;
    protected final static int PARTITIONS = (SITES * HOSTS) / (KFACTOR + 1);
    protected final static String jarName = "statistics-cluster.jar";
    protected final static boolean hasLocalServer = false;
    protected static StringBuilder m_recentAnalysis = null;
    protected final static int FSYNC_INTERVAL_GOLD = 50;

    protected final static String drSchema =
            "CREATE TABLE EMPLOYEE (\n"
                    +   "E_ID INTEGER NOT NULL,\n"
                    +   "E_AGE INTEGER NOT NULL"
                    +   ");\n"
                    +   "DR TABLE EMPLOYEE;\n";

    public StatisticsTestSuiteBase(String name) {
        super(name);
    }

    private String claimRecentAnalysis() {
        String result = "No root cause analysis is available for this failure.";
        if (m_recentAnalysis != null) {
            result = m_recentAnalysis.toString();
            m_recentAnalysis = null;
        }
        return result;
    }

    // validation functions supporting multiple columns
    private boolean checkRowForMultipleTargets(VoltTable result, Map<String, String> columnTargets) {
        for (Entry<String, String> entry : columnTargets.entrySet()) {
            if (!result.getString(entry.getKey()).equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private int countHostsProvidingRows(VoltTable result, Map<String, String> columnTargets,
            boolean enforceUnique) {
        result.resetRowPosition();
        Set<Long> hostsSeen = new HashSet<>();
        while (result.advanceRow()) {
            if (checkRowForMultipleTargets(result, columnTargets)) {
                Long thisHostId = result.getLong("HOST_ID");
                if (enforceUnique) {
                    StringBuilder message = new StringBuilder();
                    message.append("HOST_ID: " + thisHostId + " seen twice in table looking for ");
                    for (Entry<String, String> entry : columnTargets.entrySet()) {
                        message.append(entry.getValue() + " in column " + entry.getKey() + ";");
                    }
                    assertFalse(message.toString(), hostsSeen.contains(thisHostId));
                }
                hostsSeen.add(thisHostId);
            }
        }

        //* Enable this to force a failure with diagnostics */ hostsSeen.add(123456789L);
        // Before possibly failing an assert, prepare to report details of the non-conforming result.
        m_recentAnalysis = null;
        if (HOSTS != hostsSeen.size()) {
            m_recentAnalysis = new StringBuilder();
            m_recentAnalysis.append("Failure follows from these results:\n");
            Set<Long> seenAgain = new HashSet<>();
            result.resetRowPosition();
            while (result.advanceRow()) {
                Long thisHostId = result.getLong("HOST_ID");
                String rowStatus = "Found a non-match";
                if (checkRowForMultipleTargets(result, columnTargets)) {
                    if (seenAgain.add(thisHostId)) {
                        rowStatus = "Added a match";
                    } else {
                        rowStatus = "Duplicated a match";
                    }
                }
                m_recentAnalysis.append(rowStatus
                        + " at host " + thisHostId + " for ");
                for (String key : columnTargets.keySet()) {
                    m_recentAnalysis.append(key + " " + result.getString(key) + ";");
                }
                m_recentAnalysis.append("\n");
            }
        }
        return hostsSeen.size();
    }

    // For the provided table, verify that there is a row for each host in the cluster where
    // the column designated by each key of columnTargets has the value corresponding to this
    // key in columnTargets.  For example, for Initiator stats, if there is an entry
    // <'PROCEDURE_NAME', 'foo> in columnTargets, this will verify that the initiator at each
    // node has seen a procedure invocation for 'foo'
    protected void validateRowSeenAtAllHosts(VoltTable result, Map<String, String> columnTargets,
            boolean enforceUnique) {
        result.resetRowPosition();
        int hostCount = countHostsProvidingRows(result, columnTargets, enforceUnique);
        assertEquals(claimRecentAnalysis(), HOSTS, hostCount);
    }

    protected boolean validateRowSeenAtAllSites(VoltTable result, String columnName, String targetValue,
            boolean enforceUnique) {
        result.resetRowPosition();
        Set<Long> sitesSeen = new HashSet<>();
        while (result.advanceRow()) {
            String colValFromRow = result.getString(columnName);
            if (targetValue.equalsIgnoreCase(colValFromRow)) {
                long hostId = result.getLong("HOST_ID");
                long thisSiteId = result.getLong("SITE_ID");
                thisSiteId |= hostId << 32;
                if (enforceUnique) {
                    assertFalse("SITE_ID: " + thisSiteId + " seen twice in table looking for " + targetValue
                            + " in column " + columnName, sitesSeen.contains(thisSiteId));
                }
                sitesSeen.add(thisSiteId);
            }
        }
        return (HOSTS * SITES) == sitesSeen.size();
    }

    // For the provided table, verify that there is a row for each partition in the cluster where
    // the column designated by 'columnName' has the value 'targetValue'.

    protected void validateRowSeenAtAllPartitions(VoltTable result, String columnName, String targetValue,
            boolean enforceUnique) {
        result.resetRowPosition();
        Set<Integer> partsSeen = new HashSet<>();
        while (result.advanceRow()) {
            String colValFromRow = result.getString(columnName);
            if (targetValue.equalsIgnoreCase(colValFromRow)) {
                int thisPartId = (int) result.getLong("PARTITION_ID");
                if (enforceUnique) {
                    assertFalse("PARTITION_ID: " + thisPartId + " seen twice in table looking for " + targetValue
                            + " in column " + columnName, partsSeen.contains(thisPartId));
                }
                partsSeen.add(thisPartId);
            }
        }
        // Remove the MPI in case it's in there
        partsSeen.remove(MpInitiator.MP_INIT_PID);
        assertEquals(PARTITIONS, partsSeen.size());
    }

    static public Test suite(Class<? extends StatisticsTestSuiteBase> classzz, boolean isCommandLogTest)
            throws IOException {
        return suite(classzz, isCommandLogTest, -1);
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite(Class<? extends StatisticsTestSuiteBase> classzz, boolean isCommandLogTest,
            int replicationPort) throws IOException {
        return suite(classzz, isCommandLogTest, replicationPort, ReuseServer.DEFAULT, ImmutableMap.of());
    }

    static public Test suite(Class<? extends StatisticsTestSuiteBase> classzz, Map<String, String> envs) throws IOException {
        return suite(classzz, false, -1, ReuseServer.DEFAULT, envs);
    }

    static public Test suite(Class<? extends StatisticsTestSuiteBase> classzz, boolean isCommandLogTest,
                             int replicationPort, ReuseServer reuseServer) throws IOException {
        return suite(classzz, isCommandLogTest, replicationPort, reuseServer, ImmutableMap.of());
    }

    static public Test suite(Class<? extends StatisticsTestSuiteBase> classzz, boolean isCommandLogTest,
            int replicationPort, ReuseServer reuseServer, Map<String, String> envs) throws IOException {
        LocalCluster config = null;

        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(classzz);

        // Not really using TPCC functionality but need a database.
        // The testLoadMultipartitionTable procedure assumes partitioning
        // on warehouse id.
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(
                "CREATE TABLE WAREHOUSE (\n"
                + "  W_ID SMALLINT DEFAULT '0' NOT NULL,\n"
                + "  W_NAME VARCHAR(16) DEFAULT NULL,\n"
                + "  W_STREET_1 VARCHAR(32) DEFAULT NULL,\n"
                + "  W_STREET_2 VARCHAR(32) DEFAULT NULL,\n"
                + "  W_CITY VARCHAR(32) DEFAULT NULL,\n"
                + "  W_STATE VARCHAR(2) DEFAULT NULL,\n"
                + "  W_ZIP VARCHAR(9) DEFAULT NULL,\n"
                + "  W_TAX FLOAT DEFAULT NULL,\n"
                + "  W_YTD FLOAT DEFAULT NULL,\n"
                + "  CONSTRAINT W_PK_TREE PRIMARY KEY (W_ID)\n"
                + ");\n"
                + "CREATE TABLE ITEM (\n"
                + "  I_ID INTEGER DEFAULT '0' NOT NULL,\n"
                + "  I_IM_ID INTEGER DEFAULT NULL,\n"
                + "  I_NAME VARCHAR(32) DEFAULT NULL,\n"
                + "  I_PRICE FLOAT DEFAULT NULL,\n"
                + "  I_DATA VARCHAR(64) DEFAULT NULL,\n"
                + "  CONSTRAINT I_PK_TREE PRIMARY KEY (I_ID)\n"
                + ");\n"
                + "CREATE TABLE NEW_ORDER (\n"
                + "  NO_W_ID SMALLINT DEFAULT '0' NOT NULL\n"
                + ");\n");

        project.addPartitionInfo("WAREHOUSE", "W_ID");
        project.addPartitionInfo("NEW_ORDER", "NO_W_ID");
        project.addProcedure(GoSleep.class, new ProcedurePartitionData("NEW_ORDER", "NO_W_ID"));

        project.setClockSkewInterval(getClockSkewInterval(envs));

        // Enable asynchronous logging for test of commandlog test
        if (MiscUtils.isPro() && isCommandLogTest) {
            project.configureLogging(null, null, false, true, FSYNC_INTERVAL_GOLD, null, null);
        }

        /*
         * Create a cluster configuration.
         * Some of the sysproc results come back a little strange when applied to a cluster that is being
         * simulated through LocalCluster -- all the hosts have the same HOSTNAME, just different host ids.
         * So, these tests shouldn't rely on the usual uniqueness of host names in a cluster.
         */
        config = new LocalCluster(jarName, StatisticsTestSuiteBase.SITES,
                StatisticsTestSuiteBase.HOSTS, StatisticsTestSuiteBase.KFACTOR,
                BackendTarget.NATIVE_EE_JNI);
        config.setHasLocalServer(hasLocalServer);
        config.getAdditionalProcessEnv().putAll(envs);
        if (MiscUtils.isPro() && isCommandLogTest) {
            config.setJavaProperty("LOG_SEGMENT_SIZE", "1");
            config.setJavaProperty("LOG_SEGMENTS", "1");
        }

        if (replicationPort > 0) {
            // cluster id is default to 0
            project.addLiteralSchema(drSchema);
            project.setDrProducerEnabled();
            config.setReplicationPort(replicationPort);
            config.overrideAnyRequestForValgrind();
        }

        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config, reuseServer);

        return builder;
    }

    private static Duration getClockSkewInterval(Map<String, String> envs) {
        String intervalString = envs.get("CLOCK_SKEW_SCHEDULER_INTERVAL");
        if (intervalString != null) {
            return Duration.parse(intervalString);
        } else {
            return null;
        }
    }
}
