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

package org.voltdb;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientConfigForTest;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb_testprocs.rejoinfuzz.NonOrgVoltDBProc;

public class RejoinTestBase extends JUnit4LocalClusterTest {
    protected static final ClientConfig m_cconfig = new ClientConfigForTest("ry@nlikesthe", "y@nkees");

    public VoltProjectBuilder getBuilderForTest() throws UnsupportedEncodingException {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));\n" +
            "create stream export_ok_blah_with_no_pk (" +
            "ival bigint default 0 not null, " +
            ");\n" +
            "create table blah_replicated (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));" +
            "create table PARTITIONED (" +
            "pkey bigint default 0 not null, " +
            "value bigint default 0 not null, " +
            "PRIMARY KEY(pkey));" +

            "create view vblah_replicated (ival, cnt) as\n" +
            "select ival, count(*) from blah_replicated group by ival;\n" +
            "create view vpartitioned (pkey, cnt) as\n" +
            "select pkey, count(*) from PARTITIONED group by pkey;\n" +
            "create view vrpartitioned (value, cnt) as\n" +
            "select value, count(*) from partitioned group by value;\n" +
            "create view vjoin (pkey, cntt) as\n" +
            "select a.pkey, count(*) from PARTITIONED a join blah_replicated b on a.value = b.ival group by a.pkey;\n" +

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
            "    GROUP BY c1, c6, c2, c3, c4, c5;" +
            "CREATE TABLE GEO_POLY_MP ( " +
            "    ID  BIGINT NOT NULL PRIMARY KEY," +
            "    POLY GEOGRAPHY NOT NULL " +
            ");" +
            "CREATE TABLE GEO_POINT_MP ( " +
            "    ID BIGINT NOT NULL PRIMARY KEY," +
            "    PT GEOGRAPHY_POINT NOT NULL" +
            ");" +
            "CREATE TABLE GEO_POLY_SP ( " +
            "    ID  BIGINT NOT NULL PRIMARY KEY," +
            "    POLY GEOGRAPHY NOT NULL " +
            ");" +
            "CREATE TABLE GEO_POINT_SP ( " +
            "    ID BIGINT NOT NULL PRIMARY KEY," +
            "    PT GEOGRAPHY_POINT NOT NULL" +
            ");"
            ;

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
        builder.addPartitionInfo("GEO_POLY_SP", "ID");
        builder.addPartitionInfo("GEO_POINT_SP", "ID");

        RoleInfo gi = new RoleInfo("foo", true, false, true, true, false, false);
        builder.addRoles(new RoleInfo[] { gi } );
        UserInfo ui = new UserInfo( "ry@nlikesthe", "y@nkees", new String[] { "foo" } );
        builder.addUsers(new UserInfo[] { ui } );

        ProcedurePartitionData data = new ProcedurePartitionData("TEST_INLINED_STRING", "pkey");
        ProcedureInfo[] pi = new ProcedureInfo[] {
            new ProcedureInfo(new String[] { "foo" }, "InsertInlinedString", "insert into TEST_INLINED_STRING values (?, ?, ?);", data),
            new ProcedureInfo(new String[] { "foo" }, "Insert", "insert into blah values (?);", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertSinglePartition", "insert into blah values (?);",
                    new ProcedurePartitionData("blah", "ival")),
            new ProcedureInfo(new String[] { "foo" }, "InsertReplicated", "insert into blah_replicated values (?);", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlahSinglePartition", "select * from blah where ival = ?;",
                    new ProcedurePartitionData("blah", "ival")),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlah", "select * from blah where ival = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlahReplicated", "select * from blah_replicated where ival = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertPartitioned", "insert into PARTITIONED values (?, ?);",
                    new ProcedurePartitionData("PARTITIONED", "pkey")),
            new ProcedureInfo(new String[] { "foo" }, "UpdatePartitioned", "update PARTITIONED set value = ? where pkey = ?;",
                    new ProcedurePartitionData("PARTITIONED", "pkey", "1")),
            new ProcedureInfo(new String[] { "foo" }, "SelectPartitioned", "select * from PARTITIONED order by pkey;", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectCountPartitioned", "select count(*) from PARTITIONED;", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertPartitionedLarge", "insert into PARTITIONED_LARGE values (?, ?, ?);",
                    new ProcedurePartitionData("PARTITIONED_LARGE", "pkey", "0")),
            new ProcedureInfo(new String[] { "foo" }, "InsertMultiPartitionPolygon", "insert into GEO_POLY_MP values (?, ?);", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertMultiPartitionPoint", "insert into GEO_POINT_MP values (?, ?);", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectMultiPartitionPolygon", "select * from GEO_POLY_MP where id = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectMultiPartitionPoint", "select * from GEO_POINT_MP where id = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertSinglePartitionPolygon", "insert into GEO_POLY_SP values (?, ?);", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertSinglePartitionPoint", "insert into GEO_POINT_SP values (?, ?);", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectSinglePartitionPolygon", "select * from GEO_POLY_SP where id = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectSinglePartitionPoint", "select * from GEO_POINT_SP where id = ?;", null),
        };

        builder.addProcedures(pi);
        builder.addProcedure(NonOrgVoltDBProc.class);

        return builder;
    }

    static class Context {
        long catalogCRC;
        ServerThread localServer;
    }
}
