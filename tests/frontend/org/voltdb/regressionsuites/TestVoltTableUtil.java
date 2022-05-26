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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.Collections;

import com.google_voltpatches.common.collect.Lists;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.volttableutil.VoltTableUtil;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestVoltTableUtil extends RegressionSuite {
    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestVoltTableUtil(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config;
        MultiConfigSuiteBuilder builder =
                new MultiConfigSuiteBuilder(TestVoltTableUtil.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE stations (\n" +
                "  station_id            SMALLINT          NOT NULL,\n" +
                "  name                  VARCHAR(25 BYTES) NOT NULL,\n" +
                "  CONSTRAINT PK_stations PRIMARY KEY (station_id)\n" +
                ");"+

                "CREATE TABLE activity(\n" +
                "  card_id               INTEGER        NOT NULL,\n" +
                "  date_time             TIMESTAMP      NOT NULL,\n" +
                "  station_id            SMALLINT       NOT NULL,\n" +
                "  amount                INTEGER        NOT NULL\n" +
                ");";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            fail();
        }

        // CONFIG #2: Local Site/Partitions running on JNI backend
        config = new LocalCluster("fixedsql-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

    private void initTable() throws Exception {
        Client client = getClient();

        client.callProcedure("stations.insert", 4, "Alewife");
        client.callProcedure("stations.insert", 5, "Davis");
        client.callProcedure("stations.insert", 6, "Porter");
        client.callProcedure("stations.insert", 7, "Harvard");
        client.callProcedure("stations.insert", 8, "Central");
        client.callProcedure("stations.insert", 9, "Kendall/MIT");
        client.callProcedure("stations.insert", 10, "Charles/MGH");

        client.callProcedure("activity.insert", 1, "2018-07-18 02:00:00", 3, 5);
        client.callProcedure("activity.insert", 1, "2018-07-18 03:00:00", 5, 8);
        client.callProcedure("activity.insert", 2, "2018-07-18 04:00:00", 5, 8);
        client.callProcedure("activity.insert", 3, "2018-07-18 05:00:00", 5, 5);
        client.callProcedure("activity.insert", 4, "2018-07-18 06:00:00", 5, 5);
        client.callProcedure("activity.insert", 5, "2018-07-18 07:00:00", 5, 7);
        client.callProcedure("activity.insert", 6, "2018-07-18 08:00:00", 7, 8);
        client.callProcedure("activity.insert", 7, "2018-07-18 09:00:00", 9, 7);
        client.callProcedure("activity.insert", 8, "2018-07-18 10:00:00", 9, 8);
        client.callProcedure("activity.insert", 9, "2018-07-18 11:00:00", 9, 8);
    }


    public void testSimple() throws Exception {
        initTable();

        Client client = getClient();
        ClientResponse cr;

        cr = client.callProcedure("@AdHoc", "select * from stations where station_id>4");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable stations = cr.getResults()[0];

        cr = client.callProcedure("@AdHoc", "select * from activity where amount>5");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable activity= cr.getResults()[0];

        /** **Stations**
        cols (STATION_ID:SMALLINT), (NAME:STRING),
        rows -
                5,                  Davis
                6,                  Porter
                7,                  Harvard
                8,                  Central
                9,                  Kendall/MIT
                10,                 Charles/MGH

            **Activity**
        cols (CARD_ID:INTEGER), (DATE_TIME:TIMESTAMP), (STATION_ID:SMALLINT), (AMOUNT:INTEGER),
        rows -
                1,              2018-07-18 03:00:00.000000,     5,              8
                2,              2018-07-18 04:00:00.000000,     5,              8
                5,              2018-07-18 07:00:00.000000,     5,              7
                6,              2018-07-18 08:00:00.000000,     7,              8
                7,              2018-07-18 09:00:00.000000,     9,              7
                8,              2018-07-18 10:00:00.000000,     9,              8
                9,              2018-07-18 11:00:00.000000,     9,              8
         */

        VoltTable vt = VoltTableUtil.executeSql(
                "select * from activity where STATION_ID = 5 order by card_id",
                Collections.singletonList("activity"), Collections.singletonList(activity));
        assertEquals(3, vt.getRowCount());

        vt = VoltTableUtil.executeSql(
                "select stations.station_id, stations.name, activity.date_time, activity.card_id, activity.amount " +
                        "from stations, activity where stations.station_id = activity.station_id order by stations.station_id",
                Lists.newArrayList("stations", "activity"),
                Lists.newArrayList(stations, activity));
        assertEquals(7, vt.getRowCount());

        vt = VoltTableUtil.executeSql(
                "select stations.station_id, count(activity.card_id) " +
                        "from stations, activity where stations.station_id = activity.station_id " +
                        "group by stations.station_id",
                Lists.newArrayList("stations", "activity"),
                Lists.newArrayList(stations, activity));
        assertEquals(3, vt.getRowCount());

        vt = VoltTableUtil.executeSql(
                "select station_id, card_id, date_time, rank()" +
                        "OVER (partition by station_id order by date_time) " +
                        "from activity where station_id <> 7",
                Lists.newArrayList("stations", "activity"),
                Lists.newArrayList(stations, activity));
        assertEquals(6, vt.getRowCount());

        cr = client.callProcedure("@SystemInformation", "overview");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        final VoltTable statistics= cr.getResults()[0];

        vt = VoltTableUtil.executeSql(
          "select * from sta where KEY LIKE '%HTTP%' ",
          "sta", statistics
        );
        assertEquals(2, vt.getRowCount());
    }

}
