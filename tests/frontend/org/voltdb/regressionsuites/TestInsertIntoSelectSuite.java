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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestInsertIntoSelectSuite extends RegressionSuite {

    public TestInsertIntoSelectSuite(String name) {
        super(name);
    }

    static final String vcDefault = "dachshund";
    static final long intDefault = 121;

    static private class ProcedureTemplate {

        static List<String> partitionedSourceTables = null;
        static List<String> replicatedSourceTables = null;
        static List<String> partitionedAndReplicatedSourceTables = null;

        private static List<String> getPartitionedSourceTables() {
            if (partitionedSourceTables == null) {
                partitionedSourceTables = new ArrayList<String>(2);
                partitionedSourceTables.add("source_p1");
                partitionedSourceTables.add("source_p2");
            }
            return partitionedSourceTables;
        }

        private static List<String> getReplicatedSourceTables() {
            if (replicatedSourceTables == null) {
                replicatedSourceTables = new ArrayList<String>(2);
                replicatedSourceTables.add("source_r1");
                replicatedSourceTables.add("source_r2");
            }
            return replicatedSourceTables;
        }

        private static List<String> getPartitionedAndReplicatedSourceTables() {
            if (partitionedAndReplicatedSourceTables == null) {
                partitionedAndReplicatedSourceTables = new ArrayList<String>();
                partitionedAndReplicatedSourceTables.addAll(getPartitionedSourceTables());
                partitionedAndReplicatedSourceTables.addAll(getReplicatedSourceTables());
            }
            return partitionedAndReplicatedSourceTables;
        }

        private static final String partitionedTargetTable = "target_p";
        private static final String replicatedTargetTable = "target_r";

        // Instance Data

        private final String m_queryFormat;
        private final String m_label;

        private int m_comboCounter = 0;

        private final Map<String, List<String>> m_procNameToStmts = new HashMap<>();

        ProcedureTemplate(String label, String queryFormat) {
            m_queryFormat = queryFormat;
            m_label = label;
            generateStatements();
        }

        private void formatQueryAndGenerateStatements(String queryFormat,
                boolean partitionProcedure,
                String targetTable,
                Collection<String> sourceTables1,
                Collection<String> sourceTables2) {
            Stack<String> formatStack = new Stack<String>();

            formatStack.push(queryFormat);
            while (! formatStack.empty()) {
                String format = formatStack.pop();

                int numTablesNeeded = StringUtils.countMatches(format, "%s");
                if (numTablesNeeded > 0) {

                    Collection<String> whichSet = null;
                    if (numTablesNeeded == 2 ) {
                        // First table should be from
                        whichSet = sourceTables1;
                    } else {
                        whichSet = sourceTables2;
                    }


                    for (String sourceTable : whichSet) {
                        String newFormat = format.replaceFirst("%s", sourceTable);
                        formatStack.push(newFormat);
                    }
                } else {
                    generateStatementsForProcedure(partitionProcedure, targetTable, format);
                }
            }
        }

        private void generateStatements() {

            int numParams = StringUtils.countMatches(m_queryFormat, "?");

            if (numParams > 0) {
                // generate stored procedures from this template that insert into a partitioned table,
                // selecting from both partitioned and replicated tables.
                // partitioned the stored procedures
                formatQueryAndGenerateStatements(m_queryFormat, true, partitionedTargetTable,
                        getPartitionedAndReplicatedSourceTables(), getPartitionedAndReplicatedSourceTables());
            }

            // As above, except that stored procedures are not marked as single-partition
            formatQueryAndGenerateStatements(m_queryFormat, false, partitionedTargetTable,
                    getPartitionedSourceTables(), getPartitionedAndReplicatedSourceTables());

            // generated procedures that insert into replicated tables, selecting from replicated tables
            formatQueryAndGenerateStatements(m_queryFormat, false, replicatedTargetTable,
                    getReplicatedSourceTables(), getReplicatedSourceTables());
        }

        private String generateProcedureName(boolean partitioned, String targetTable) {
            String procName = "insert_into_select_" + m_label + "_" + targetTable;
            String combo = String.format("_combo%02d", (Object)m_comboCounter);
            m_comboCounter++;

            procName += combo;
            if (partitioned) {
                procName += "_partitioned";
            }

            return procName;
        }



        private void generateStatementsForProcedure(boolean partitionProcedure, String targetTable, String query) {
            String procName = generateProcedureName(partitionProcedure, targetTable);

            // Create a map
            //
            // procedureName ->   Ad hoc statement
            //                    create procedure statement (and maybe partition procedure statement)
            //                    verify procedure
            //                    result set produced by HSQL?

            StringBuilder adHocStmt = new StringBuilder();
            adHocStmt.append("INSERT INTO " + targetTable + "\n");
            adHocStmt.append("  " + query + ";\n");

            StringBuilder insertProc = new StringBuilder();
            insertProc.append("\nCREATE PROCEDURE " + procName + " AS\n");
            insertProc.append(adHocStmt.toString());
            if (partitionProcedure) {
                insertProc.append("PARTITION PROCEDURE " + procName + " ON TABLE " + targetTable + " COLUMN bi;\n");
            }

            StringBuilder verifyProc = new StringBuilder();
            verifyProc.append("\nCREATE PROCEDURE verify_" + procName + " AS \n");
            verifyProc.append("  " + query + "\n");
            verifyProc.append("  ORDER BY 1, 2, 3, 4;\n");

            ArrayList<String> stmts = new ArrayList<>();
            stmts.add(adHocStmt.toString());
            stmts.add(insertProc.toString());
            stmts.add(verifyProc.toString());
            m_procNameToStmts.put(procName, stmts);
        }

        Map<String, List<String>> getGeneratedStatements() {
            return m_procNameToStmts;
        }
    }


    static final ProcedureTemplate procedureTemplates[] = new ProcedureTemplate[] {
        new ProcedureTemplate("simple",
                              "select * from %s where bi = ?"),
        new ProcedureTemplate("simple_noparam",
                              "select * from %s"),
        new ProcedureTemplate("join",
                              "select t1.bi, t1.vc, t2.ii, t2.ti " +
                              "from %s as t1 inner join %s as t2 on t1.bi = t2.bi and t1.ii = t2.ii " +
                              "where t1.bi = ?"),
        new ProcedureTemplate("join_noparam",
                              "select t1.bi, t1.vc, t2.ii, t2.ti " +
                              "from %s as t1 inner join %s as t2 on t1.bi = t2.bi and t1.ii = t2.ii"),
        new ProcedureTemplate("subquery",
                              "select * " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s) as t1_subq " +
                              "where t1_subq.bi = ?"),
        new ProcedureTemplate("subquery_noparam",
                              "select * " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s) as t1_subq"),
        new ProcedureTemplate("subquery_inner_filter",
                              "select * " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s where bi = ?) as t1_subq"),
        new ProcedureTemplate("subquery_inner_filter_noparam",
                              "select * " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s) as t1_subq"),
        new ProcedureTemplate("subquery_join",
                              "select t1_subq.bi, t1_subq.vc, t2.ii, t2.ti " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s) as t1_subq " +
                              "inner join %s as t2 on t1_subq.bi = t2.bi and t1_subq.ii = t2.ii " +
                              "where t1_subq.bi = ?"),
        new ProcedureTemplate("subquery_join_noparam",
                              "select t1_subq.bi, t1_subq.vc, t2.ii, t2.ti " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s) as t1_subq " +
                              "inner join %s as t2 on t1_subq.bi = t2.bi and t1_subq.ii = t2.ii"),
        new ProcedureTemplate("join_two_subqueries",
                              "select t1_subq.bi, t1_subq.vc, t2_subq.ii, t2_subq.ti " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s) as t1_subq " +
                              "inner join (select bi, '2nd_subq + ' || vc as vc, ii, ti from %s) as t2_subq " +
                              "on t1_subq.bi = t2_subq.bi and t1_subq.ii = t2_subq.ii " +
                              "where t1_subq.bi = ?"),
        new ProcedureTemplate("join_two_subqueries_noparam",
                              "select t1_subq.bi, t1_subq.vc, t2_subq.ii, t2_subq.ti " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from %s) as t1_subq " +
                              "inner join (select bi, '2nd_subq + ' || vc as vc, ii, ti from %s) as t2_subq " +
                              "on t1_subq.bi = t2_subq.bi and t1_subq.ii = t2_subq.ii"),
        new ProcedureTemplate("nest_subqueries",
                              "select t1_subq.bi, t1_subq.vc, t2_subq.ii, t2_subq.ti " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from " +
                              "(select bi, 'nested ' || vc as vc, ii, ti from %s) as t1_subq_subq) as t1_subq " +
                              "inner join (select bi, '2nd_subq + ' || vc as vc, ii, ti from %s) as t2_subq " +
                              "on t1_subq.bi = t2_subq.bi and t1_subq.ii = t2_subq.ii " +
                              "where t1_subq.bi = ?"),
        new ProcedureTemplate("nest_subqueries_noparam",
                              "select t1_subq.bi, t1_subq.vc, t2_subq.ii, t2_subq.ti " +
                              "from (select bi, 'subq + ' || vc as vc, ii, ti from " +
                              "(select bi, 'nested ' || vc as vc, ii, ti from %s) as t1_subq_subq) as t1_subq " +
                              "inner join (select bi, '2nd_subq + ' || vc as vc, ii, ti from %s) as t2_subq " +
                              "on t1_subq.bi = t2_subq.bi and t1_subq.ii = t2_subq.ii")
    };

    private static Map<String, List<String>> generatedStmtMap = null;

    private static Map<String, List<String>> mapOfAllGeneratedStatements() {

        if (generatedStmtMap == null) {
            generatedStmtMap = new HashMap<>();
            for (ProcedureTemplate t : procedureTemplates) {
                for (Map.Entry<String, List<String>> e : t.getGeneratedStatements().entrySet()) {
                    generatedStmtMap.put(e.getKey(), e.getValue());
                }
            }
        }

        return generatedStmtMap;
    }

    private static int numberOfParametersNeeded(String procName) {
        List<String> stmts = mapOfAllGeneratedStatements().get(procName);
        int numParams = StringUtils.countMatches(stmts.get(0), "?");
        return numParams;
    }

    private static List<String> generatedProcedures() {
        List<String> procs = new ArrayList<>();
        for (List<String> stmts : mapOfAllGeneratedStatements().values()) {
            procs.add(stmts.get(1));
            procs.add(stmts.get(2));
        }

        return procs;
    }

    static private String generateSchema() {
        StringBuilder sb = new StringBuilder();

        // Target tables: 1 partitioned, 1 replicated
        // Source tables: 2 partitioned, 2 replicated
        sb.append(
                "CREATE TABLE target_p (bi bigint not null," +
                "vc varchar(100) default '" + vcDefault +"'," +
                "ii integer default " + intDefault + "," +
                "ti tinyint default " + intDefault + " not null);" +
                "partition table target_p on column bi;" +

                "CREATE TABLE target_r (bi bigint not null," +
                "vc varchar(100) default '" + vcDefault +"'," +
                "ii integer default " + intDefault + "," +
                "ti tinyint default " + intDefault + " not null);" +

                "CREATE TABLE source_p1 (bi bigint not null," +
                "vc varchar(100)," +
                "ii integer," +
                "ti tinyint);" +
                "partition table source_p1 on column bi;" +

                "CREATE TABLE source_p2 (bi bigint not null," +
                "vc varchar(100)," +
                "ii integer," +
                "ti tinyint);" +
                "partition table source_p2 on column bi;" +

                "CREATE TABLE source_r1 (bi bigint not null," +
                "vc varchar(100)," +
                "ii integer," +
                "ti tinyint);" +

                "CREATE TABLE source_r2 (bi bigint not null," +
                "vc varchar(4)," +
                "ii integer," +
                "ti tinyint);" +

                // test that we can do inline insert with a sequential
                // scan node.  Everything is partitioned.
                "CREATE TABLE ENG12834_SRC ( " +
                "  ID         BIGINT NOT NULL, " +
                "  NONE       VARCHAR(10), " +
                "  A1         VARCHAR(10), " +
                "  A2         VARCHAR(20) " +
                "); " +
                "PARTITION TABLE ENG12834_SRC ON COLUMN ID; " +

                // test that we can do inline insert with an index
                // scan node.
                "CREATE TABLE ENG12834_SRC_INDEX ( " +
                "  ID         BIGINT NOT NULL PRIMARY KEY, " +
                "  NONE       VARCHAR(10), " +
                "  A1         VARCHAR(10), " +
                "  A2         VARCHAR(20) " +
                "); " +
                "PARTITION TABLE ENG12834_SRC_INDEX ON COLUMN ID; " +

                "CREATE TABLE ENG12834_DST ( " +
                "  ID         BIGINT PRIMARY KEY NOT NULL, " +
                "  DEF        INTEGER DEFAULT 100, " +
                "  A1         VARCHAR(10), " +
                "  A2         VARCHAR(20) " +
                "); " +
                "PARTITION TABLE ENG12834_DST ON COLUMN ID; " +

                "CREATE TABLE ENG12834_P0 ( " +
                "                 ID      INTEGER NOT NULL, " +
                "                 TINY    TINYINT, " +
                "                 SMALL   SMALLINT, " +
                "                 INT     INTEGER, " +
                "                 BIG     BIGINT, " +
                "                 NUM     FLOAT, " +
                "                 DEC     DECIMAL, " +
                "                 VCHAR_INLINE     VARCHAR(14), " +
                "                 VCHAR_INLINE_MAX VARCHAR(63 BYTES), " +
                "                 VCHAR            VARCHAR(64 BYTES), " +
                "                 VCHAR_JSON       VARCHAR(1000), " +
                "                 TIME    TIMESTAMP, " +
                "                 VARBIN  VARBINARY(100), " +
                "                 POINT   GEOGRAPHY_POINT, " +
                "                 POLYGON GEOGRAPHY " +
                "                 ); " +
                "PARTITION TABLE ENG12834_P0 ON COLUMN ID; " +

                "CREATE TABLE ENG12834_R7 ( " +
                "                 ID      INTEGER  UNIQUE, " +
                "                 TINY    TINYINT  UNIQUE, " +
                "                 SMALL   SMALLINT UNIQUE, " +
                "                 INT     INTEGER  UNIQUE, " +
                "                 BIG     BIGINT   UNIQUE, " +
                "                 NUM     FLOAT    UNIQUE, " +
                "                 DEC     DECIMAL  UNIQUE, " +
                "                 VCHAR_INLINE     VARCHAR(42 BYTES)   UNIQUE, " +
                "                 VCHAR_INLINE_MAX VARCHAR(15)         UNIQUE, " +
                "                 VCHAR            VARCHAR(16)         UNIQUE, " +
                "                 VCHAR_JSON       VARCHAR(4000 BYTES) UNIQUE, " +
                "                 TIME    TIMESTAMP       UNIQUE, " +
                "                 VARBIN  VARBINARY(100)  UNIQUE, " +
                "                 POINT   GEOGRAPHY_POINT, " +
                "                 POLYGON GEOGRAPHY, " +
                "                 PRIMARY KEY (ID, VCHAR, VARBIN), " +
                "); " +
                "CREATE TABLE ENG_13059_R1 ( " +
                "        ID      INTEGER DEFAULT 0 NOT NULL PRIMARY KEY, " +
                "        TINY    TINYINT, " +
                "        VCHAR   VARCHAR(64 BYTES), " +
                "        VCHAR_INLINE VARCHAR(63 bytes), " +
                "        VBIPV6  VARBINARY(16) " +
                "      ); " +
                ""
                  );

        sb.append(

                // select all rows from target tables, to verify inserted rows
                "create procedure get_all_target_p_rows as select * from target_p order by bi, vc, ii, ti;" +
                "create procedure get_all_target_r_rows as select * from target_r order by bi, vc, ii, ti;" +

                // A very simple insert into select statement
                "create procedure insert_p_source_p as insert into target_p (bi, vc, ii, ti) select * from source_p1 where bi = ?;" +
                "partition procedure insert_p_source_p on table target_p column bi;" +

                // an insert into select statement that makes use of default values
                "create procedure insert_p_use_defaults as insert into target_p (bi, ti) select bi, ti from source_p1 where bi = ?;" +
                "partition procedure insert_p_use_defaults on table target_p column bi;" +

                // an insert into select statement with unordered columns
                "create procedure insert_p_use_defaults_reorder as insert into target_p (ti, bi) select ti, bi from source_p1 where bi = ?;" +
                "partition procedure insert_p_use_defaults_reorder on table target_p column bi;" +

                // group by in the subquery
                "create procedure insert_p_source_p_agg as insert into target_p (bi, vc, ii, ti) " +
                "select bi, max(vc), max(ii), min(ti)" + " from source_p1 where bi = ? group by bi;" +
                "partition procedure insert_p_source_p_agg on table target_p column bi;" +

                // transpose ti, ii, columns so there are implicit integer->tinyint and tinyint->integer casts
                "create procedure insert_p_source_p_cast as insert into target_p (bi, vc, ti, ii) select * from source_p1 where bi = ?;" +
                "partition procedure insert_p_source_p_cast on table target_p column bi;" +

                // source_p2.ii contains values that will not fit into tinyint, so this procedure should throw an out-of-range conversion exception
                "create procedure insert_p_source_p_cast_out_of_range as " +
                "insert into target_p (bi, vc, ti, ii) " +
                "select * from source_p2 where bi = ?;" +
                "partition procedure insert_p_source_p_cast_out_of_range on table target_p column bi;" +

                // Implicit string->int and int->string conversion.
                "create procedure insert_p_source_p_nonsensical_cast as insert into target_p (bi, ii, vc, ti) select * from source_p1 where bi = ?;" +
                "partition procedure insert_p_source_p_nonsensical_cast on table target_p column bi;" +

                // Target table and source table the same
                "create procedure select_and_insert_into_source as " +
                "insert into source_p1 (bi, vc, ti, ii) select bi, vc, ti, 1000 * ii from source_p1 where bi = ? order by bi, ti;" +
                "partition procedure select_and_insert_into_source on table source_p1 column bi;" +

                // HSQL seems to want a cast for the parameter
                // Note that there is no filter in source_r2
                "create procedure insert_param_in_select_list as " +
                "insert into target_p (bi, vc, ii, ti) " +
                "select cast(? as bigint), vc, ii, ti from source_r2 order by ii;" +
                "partition procedure insert_param_in_select_list on table target_p column bi;" +

                // try to insert into the wrong partition
                "create procedure insert_wrong_partition as " +
                "insert into target_p (bi, ti) select ti, cast(? as tinyint) from source_r2; " +
                "partition procedure insert_wrong_partition on table target_p column bi; " +

                // try to violate a not null constraint
                "create procedure insert_select_violate_constraint as " +
                "insert into target_p (bi, ti) " +
                "select bi, case ti when 55 then null else ti end from source_p1 where bi = ? order by ti asc;" +
                "partition procedure insert_select_violate_constraint on table target_p column bi; " +

                ""
                );

        // Generate CREATE STORED PROCEDURE, PARTITION STORED PROCEDURE statements from each procedure template,
        // as well as verify procedures for checking results
        for (String proc : generatedProcedures()) {
            sb.append(proc);
        }

        return sb.toString();
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestInsertIntoSelectSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        try {
            project.addLiteralSchema(generateSchema());
        } catch (IOException error) {
            fail(error.getMessage());
        }

        boolean success;

        // JNI
        config = new LocalCluster("iisf-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CLUSTER (disable to opt for speed over coverage...
        config = new LocalCluster("iisf-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // ... disable for speed) */

        config = new LocalCluster("iisf-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    private static void clearTargetTables(Client client) throws Exception {
        ClientResponse resp = client.callProcedure("@AdHoc", "delete from target_p");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("@AdHoc", "delete from target_r");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
    }

    private static void clearTables(Client client) throws Exception {
        ClientResponse resp = client.callProcedure("@AdHoc", "delete from source_p1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("@AdHoc", "delete from source_p2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("@AdHoc", "delete from source_r1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("@AdHoc", "delete from source_r2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        clearTargetTables(client);

        clearENG12834Tables(client);
    }

    private static void clearENG12834Tables(Client client) throws Exception {
        ClientResponse cr;

        cr = client.callProcedure("@AdHoc", "truncate table ENG12834_P0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "truncate table ENG12834_R7;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "truncate table ENG12834_SRC;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "truncate table ENG12834_SRC_INDEX;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "truncate table ENG12834_DST;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());


    }
    private void initializeTables(Client client) throws Exception {

        ClientResponse resp = null;

        clearTables(client);

        for (int i=0; i < 10; i++) {

            resp = client.callProcedure("SOURCE_P1.insert", i, Long.toHexString(i), i, i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P1.insert", i, Long.toHexString(-i), -i, -i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P1.insert", i, Long.toHexString(i * 11), i * 11, i * 11);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P1.insert", i, Long.toHexString(i * -11), i * -11, i * -11);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());


            resp = client.callProcedure("SOURCE_R1.insert", i, Long.toHexString(i), i, i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_R1.insert", i, Long.toHexString(-i), -i, -i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_R1.insert", i, Long.toHexString(i * 11), i * 11, i * 11);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_R1.insert", i, Long.toHexString(i * -11), i * -11, i * -11);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            int j = i + 5;

            resp = client.callProcedure("SOURCE_P2.insert", j, Long.toHexString(j), j, j);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P2.insert", j, Long.toHexString(-j), -j, -j);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P2.insert", j, Long.toHexString(j * 11), j * 11, (j * 11) % 128);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P2.insert", j, Long.toHexString(j * -11), j * -11, -((j * 11) % 128));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());


            resp = client.callProcedure("SOURCE_R2.insert", j, Long.toHexString(j), j, j);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_R2.insert", j, Long.toHexString(-j).substring(0, 3), -j, -j);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_R2.insert", j, Long.toHexString(j * 11), j * 11, (j * 11) % 128);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_R2.insert", j, Long.toHexString(j * -11).substring(0, 3), j * -11, -((j * 11) % 128));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        }

        assertEmptyTable(client, "ENG12834_SRC");
        assertEmptyTable(client, "ENG12834_SRC_INDEX");
        assertEmptyTable(client, "ENG12834_DST");
        resp = client.callProcedure("ENG12834_SRC.insert", 10, "mumble", "bazzle", "zazzle");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        resp = client.callProcedure("ENG12834_SRC.insert", 11, "zozzle", "zizzle", "drome");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("ENG12834_SRC_INDEX.insert", 10, "mumble", "bazzle", "zazzle");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        resp = client.callProcedure("ENG12834_SRC_INDEX.insert", 11, "zozzle", "zizzle", "drome");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        if ( ! isHSQL() ) {
            assertEmptyTable(client, "ENG12834_P0");
            assertEmptyTable(client, "ENG12834_R7");
            resp = client.callProcedure("@AdHoc", "INSERT INTO ENG12834_R7 (ID, NUM)   VALUES (100, -9076199947.9223885);");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        }
    }

    private static void assertEmptyTable(Client client, String tableName) throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse resp;
        VoltTable vt;
        String SQL = String.format("select count(*) from %s;", tableName);
        resp = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        vt = resp.getResults()[0];
        assertContentOfTable(new Object[][] { { Long.valueOf(0) } }, vt);
    }

    private static VoltTable getRows(Client client, String adHocQuery) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse resp = client.callProcedure("@AdHoc", adHocQuery);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        return resp.getResults()[0];
    }

    public void testPartitionedTableSimple() throws Exception
    {
        final Client client = getClient();
        ClientResponse resp;

        // Running the procedure with the first parameter (100) will cause 0 rows to be inserted
        // The second parameter (5) will insert 4 rows into the target table
        long[] params = new long[] {100, 5};
        String[] procs = new String[] {"insert_p_source_p", "insert_p_source_p_cast"};

        for (long param : params) {
            for (String proc : procs) {

                initializeTables(client);

                resp = client.callProcedure(proc, param);
                assertEquals(ClientResponse.SUCCESS, resp.getStatus());

                long numRowsInserted = resp.getResults()[0].asScalarLong();

                // verify that the corresponding rows in both tables are the same
                String selectAllSource = "select * from source_p1 where bi = " + param + " order by bi, ii";
                String selectAllTarget = "select * from target_p order by bi, ii";

                resp = client.callProcedure("@AdHoc", selectAllSource);
                assertEquals(ClientResponse.SUCCESS, resp.getStatus());
                VoltTable sourceRows = resp.getResults()[0];

                resp = client.callProcedure("@AdHoc", selectAllTarget);
                assertEquals(ClientResponse.SUCCESS, resp.getStatus());
                VoltTable targetRows = resp.getResults()[0];

                int i = 0;
                while(targetRows.advanceRow()) {
                    assertEquals(true, sourceRows.advanceRow());
                    assertEquals(sourceRows.getLong(0), targetRows.getLong(0));
                    assertEquals(sourceRows.getString(1), targetRows.getString(1));
                    assertEquals(sourceRows.getLong(2), targetRows.getLong(2));
                    assertEquals(sourceRows.getLong(3), targetRows.getLong(3));
                    i++;
                }

                assertEquals(numRowsInserted, i);
            }
        }
    }

    public void testSelectWithAggregation() throws Exception {
        final Client client = getClient();
        final long partitioningValue = 7;

        initializeTables(client);

        ClientResponse resp = client.callProcedure("insert_p_source_p_agg", partitioningValue);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        validateTableOfScalarLongs(resp.getResults()[0], new long[] {1});

        resp = client.callProcedure("@AdHoc", "select * from target_p order by bi");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable targetRows = resp.getResults()[0];

        assertTrue(targetRows.advanceRow());

        assertEquals(partitioningValue, targetRows.getLong(0));
        assertEquals(Long.toHexString(-partitioningValue), targetRows.getString(1));
        assertEquals(partitioningValue * 11, targetRows.getLong(2));
        assertEquals(partitioningValue * -11, targetRows.getLong(3));

        assertFalse(targetRows.advanceRow());
    }

    public void testOutOfRangeImplicitCasts() throws Exception {
        final Client client = getClient();
        final long partitioningValue = 14;

        initializeTables(client);

        verifyProcFails(client, "out of range", "insert_p_source_p_cast_out_of_range", partitioningValue);
    }

    public void testNonsensicalCasts() throws Exception {
        final Client client = getClient();
        final long partitioningValue = 5;

        initializeTables(client);

        verifyProcFails(client, "invalid character value",
                "insert_p_source_p_nonsensical_cast", partitioningValue);
    }

    public void testInsertIntoSelectWithDefaults() throws Exception {
        final Client client = getClient();

        ClientResponse resp;
        long partitioningValue = 8;

        // Both inserts use the select to produce values only for a subset of columns.
        String[] procs = new String[] {"insert_p_use_defaults", "insert_p_use_defaults_reorder"};

        for (String proc : procs) {
            initializeTables(client);

            resp = client.callProcedure(proc, partitioningValue);
            validateTableOfScalarLongs(resp.getResults()[0], new long[] {4});

            String selectSp1 = "select * from source_p1 where bi = ? order by bi, ti";
            String selectTarget = "select * from target_p order by bi, ti";

            resp = client.callProcedure("@AdHoc", selectTarget);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            VoltTable targetRows = resp.getResults()[0];

            resp = client.callProcedure("@AdHoc", selectSp1, partitioningValue);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            VoltTable sp1Rows = resp.getResults()[0];

            while (targetRows.advanceRow()) {
                assertTrue(sp1Rows.advanceRow());

                assertEquals(sp1Rows.getLong(0), targetRows.getLong(0));
                assertEquals(vcDefault, targetRows.getString(1));
                assertEquals(intDefault, targetRows.getLong(2));
                assertEquals(sp1Rows.getLong(3), targetRows.getLong(3));
            }
            assertFalse(sp1Rows.advanceRow());
        }
    }

    public void testInsertIntoSelectSameTable() throws Exception {
        final Client client = getClient();
        initializeTables(client);

        final long partitioningValue = 3;
        ClientResponse resp = client.callProcedure("select_and_insert_into_source", partitioningValue);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        validateTableOfScalarLongs(resp.getResults()[0], new long[] {4});

        String selectOrigRows = "select * from source_p1 where bi = ? and abs(ii) < 1000 order by bi, ii";
        String selectNewRows = "select * from source_p1 where bi = ? and abs(ii) > 1000 order by bi, ii";

        resp = client.callProcedure("@AdHoc", selectOrigRows, partitioningValue);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable origRows = resp.getResults()[0];

        resp = client.callProcedure("@AdHoc", selectNewRows, partitioningValue);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable newRows = resp.getResults()[0];

        while (origRows.advanceRow()) {
            assertTrue(newRows.advanceRow());

            assertEquals(origRows.getLong(0), newRows.getLong(0));
            assertEquals(origRows.getString(1), newRows.getString(1));
            assertEquals(origRows.getLong(2) * 1000, newRows.getLong(2));
            assertEquals(origRows.getLong(3), newRows.getLong(3));

        }
        assertFalse(newRows.advanceRow());
    }

    public void testSelectListParam() throws Exception {
        final Client client = getClient();
        initializeTables(client);

        final long partitioningValue = 7;
        ClientResponse resp = client.callProcedure("insert_param_in_select_list", partitioningValue);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        // tables should be identical except for "bi"
        VoltTable sourceRows = getRows(client, "select * from source_r2 order by ii");
        VoltTable targetRows = getRows(client, "select * from target_p order by ii");

        //fail("target: " + targetRows);

        while (sourceRows.advanceRow()) {
            assertTrue(targetRows.advanceRow());

            assertEquals(partitioningValue, targetRows.getLong(0));
            assertEquals(sourceRows.getString(1), targetRows.getString(1));
            assertEquals(sourceRows.getLong(2), targetRows.getLong(2));
            assertEquals(sourceRows.getLong(3), targetRows.getLong(3));
        }
        assertFalse(targetRows.advanceRow());
    }

    public void testViolateConstraint() throws Exception {
        final Client client = getClient();
        initializeTables(client);
        final long partitioningValue = 5;

        verifyProcFails(client, "CONSTRAINT VIOLATION", "insert_select_violate_constraint", partitioningValue);

        // the insert statement violated a constraint so there should still be no data in the table
        validateTableOfLongs(client , "select count(*) from target_p", new long[][] {{0}});
    }

    public void testInsertWrongPartitionFails() throws Exception {

        if (m_config.getNodeCount() > 1) {
            Client client = getClient();
            initializeTables(client);

            final long partitioningValue = 9;
            verifyProcFails(client, "Mispartitioned tuple in single-partition insert statement.",
                    "insert_wrong_partition", partitioningValue);
        }
    }

    public void testFailureToPlan() throws Exception {
        // queries which try to copy rows from one partition to another should fail
        Client client = getClient();
        initializeTables(client);

        verifyStmtFails(client, "insert into target_p " +
                        "select sp1.bi, sp2.vc, sp1.ii, sp2.ti " +
                        "from source_p1 as sp1 inner join source_p2 as sp2 " +
                        "on sp1.ii = sp2.ii",
                        "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.");

        verifyStmtFails(client, "insert into target_r " +
                        "select sr1.bi, sr1.vc, sr1.ii, sr1.ti  " +
                        "from source_r1 as sr1, source_p1 as sp1 where sr1.bi = sp1.bi",
                        "statement may not access partitioned data for insertion into replicated table");

        verifyStmtFails(client, "insert into target_p (vc, ii, ti) " +
                        "select vc, ii, ti from source_p1",
                        "Partitioning column must be assigned a value produced " +
                        "by the subquery in an INSERT INTO ... SELECT statement.");

        verifyStmtFails(client, "insert into target_p " +
                "select bi + 1, vc, ii, ti from source_p1",
                "Partitioning could not be determined for INSERT INTO ... SELECT statement");

        // two fragment plan for subquery
        verifyStmtFails(client, "insert into target_p " +
                        "select max(bi), max(vc), ii, min(ti) from source_p2 " +
                        "group by source_p2.ii",
                        "INSERT INTO ... SELECT statement subquery is too complex");
    }

    public void testSelectListConstants() throws Exception {
        Client client = getClient();

        // This statements illustrate existing limitations
        // of partitioning inference.
        //
        // Constants in the select list of the subquery
        // do not help refine partitioning.

        // In this example, the subquery is multipart, but
        // we are only inserting into one partition---only two fragments
        // are required in this plan.
        verifyStmtFails(client, "insert into target_p " +
                "select 9, vc, ii, ti " +
                "from source_p1 as sp1",

                "Partitioning could not be determined");
        // this whole statement should be single-partition!
        verifyStmtFails(client, "insert into target_p " +
                "select 9, vc, ii, ti " +
                "from source_p1 as sp1 where sp1.bi = 9",
                "Partitioning could not be determined");

        // Note however that this issue is not specific to
        // INSERT INTO ... SELECT.  This fails to plan as well:
        verifyStmtFails(client,
                "select count(*) " +
                "from target_p " +
                "inner join " +
                "(select 9 as bi, vc, ii, ti from source_p1) as ins_sq " +
                "on target_p.bi = ins_sq.bi",
                "This query is not plannable.  "
                + "The planner cannot guarantee that all rows would be in a single partition.");
    }

    public void testInsertIntoSelectGeneratedProcs() throws Exception
    {
        Set<Map.Entry<String, List<String>>> allEntries = mapOfAllGeneratedStatements().entrySet();
        System.out.println("\n\nRUNNING testInsertIntoSelectGeneratedProcs with " +
                allEntries.size() + " stored procedures\n\n");

        final Client client = getClient();
        initializeTables(client);

        for (long partitioningValue = 4; partitioningValue < 11; partitioningValue++) {
            for (Map.Entry<String, List<String>> e : allEntries) {
                clearTargetTables(client);

                // The strategy here is:
                //   Insert rows via stored procedure that invokes INSERT INTO ... <some_query>.
                //   Select the inserted rows back, compare with the table produced by <some_query>,
                //     verify the tables are equal.
                //   Do the same verification with ad hoc SQL.

                String proc = e.getKey();
                boolean needsParams = (numberOfParametersNeeded(proc) > 0);

                String prefix = "Assertion failed running stored procedure " + proc + ": ";

                // insert rows with stored procedure
                ClientResponse resp;
                if (needsParams) {
                    resp = client.callProcedure(proc, partitioningValue);
                }
                else {
                    resp = client.callProcedure(proc);
                }
                assertEquals(prefix + "procedure call failed", ClientResponse.SUCCESS, resp.getStatus());
                VoltTable insertResult = resp.getResults()[0];
                insertResult.advanceRow();

                // make sure we actually inserted something
                long numRowsInserted = insertResult.getLong(0);

                // fetch the rows we just inserted
                if (proc.contains("target_p")) {
                    resp = client.callProcedure("get_all_target_p_rows");
                }
                else {
                    resp = client.callProcedure("get_all_target_r_rows");
                }
                assertEquals(prefix + "could not fetch rows of target table", ClientResponse.SUCCESS, resp.getStatus());
                VoltTable actualRows = resp.getResults()[0];

                if (needsParams) {
                    resp = client.callProcedure("verify_" + proc, partitioningValue);
                }
                else {
                    resp = client.callProcedure("verify_" + proc);
                }
                // Fetch the rows we expect to have inserted
                assertEquals(prefix + "could not verify rows of target table", ClientResponse.SUCCESS, resp.getStatus());
                VoltTable expectedRows = resp.getResults()[0];

                assertTablesAreEqual(prefix, expectedRows, actualRows);
                int actualNumRows = actualRows.getRowCount();
                assertEquals(prefix + "insert statement returned " + numRowsInserted + " but only " + actualNumRows + " rows selected from target table",
                        actualNumRows, numRowsInserted);

                // Now try the corresponding ad hoc statement
                String adHocQuery = e.getValue().get(0);
                prefix = "Assertion failed running ad hoc SQL: " + adHocQuery;
                clearTargetTables(client);

                // insert rows with stored procedure
                if (needsParams) {
                    resp = client.callProcedure("@AdHoc", adHocQuery, partitioningValue);
                }
                else {
                    resp = client.callProcedure("@AdHoc", adHocQuery);
                }
                assertEquals(prefix + "ad hoc statement failed", ClientResponse.SUCCESS, resp.getStatus());
                insertResult = resp.getResults()[0];
                insertResult.advanceRow();

                numRowsInserted = insertResult.getLong(0);

                // fetch the rows we just inserted
                if (proc.contains("target_p")) {
                    resp = client.callProcedure("get_all_target_p_rows");
                }
                else {
                    resp = client.callProcedure("get_all_target_r_rows");
                }
                assertEquals(prefix + "could not fetch rows of target table", ClientResponse.SUCCESS, resp.getStatus());
                actualRows = resp.getResults()[0];

                expectedRows.resetRowPosition();
                assertTablesAreEqual(prefix, expectedRows, actualRows);
                actualNumRows = actualRows.getRowCount();
                assertEquals(prefix + "insert statement returned " + numRowsInserted + " but only " + actualNumRows + " rows selected from target table",
                        actualNumRows, numRowsInserted);
            }
        }
    }

    public void testENG12834() throws Exception {
        Client client = getClient();

        initializeTables(client);
        // Make sure we can insert into
        // a subset of ENG12834_DST's columns.
        testENG12834Project(client, "ENG12834_SRC");
        testENG12834Project(client, "ENG12834_SRC_INDEX");

        if ( ! isHSQL() ) {
            // We have somewhat different behavior here.
            // HSQLDB throws a constraint error which we don't.
            ClientResponse cr;
            String SQL = "INSERT INTO ENG12834_P0  SELECT   *  FROM ENG12834_R7  T1     ORDER BY SMALL DESC;";
            validateDMLTupleCount(client, SQL, 1);
            cr = client.callProcedure("@AdHoc", "SELECT * FROM ENG12834_P0;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            Object [][] expected = new Object[][] {
                { Integer.valueOf(100), null, null, null, null, Double.valueOf(-9076199947.9223885),
                  null, null, null, null, null, null, null, null, null }
            };
            assertContentOfTable(expected, cr.getResults()[0]);
        }
    }

    private void testENG12834Project(Client client, String tableName) throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "truncate table ENG12834_DST;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        String SQL = String.format("insert into ENG12834_DST (id, a1, a2) select id, a1, a2 from %s;",
                                   tableName);
        validateDMLTupleCount(client, SQL, 2);
        cr = client.callProcedure("@AdHoc", "select * from ENG12834_DST order by id;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        Object[][] expected;
        expected = new Object[][] {
            { Integer.valueOf(10), Integer.valueOf(100), "bazzle", "zazzle" },
            { Integer.valueOf(11), Integer.valueOf(100), "zizzle", "drome"  },

        };
        VoltTable vt = cr.getResults()[0];
        assertContentOfTable(expected, vt);
    }

    public void testENG13059() throws Exception {
        if (!isHSQL()) {
            Client client = getClient();

            assertSuccessfulDML(client,
                    "insert into eng_13059_r1 values (1, 1, " +
                            "'foo', 'inlined', " +
                            "  x'48454C4C4F');"
                    );
            // An insert that casts the VARBINARY field to a VARCHAR (which apparently is legal)

            assertSuccessfulDML(client, "INSERT INTO ENG_13059_R1 (VCHAR) SELECT VBIPV6 FROM ENG_13059_R1 ");

            VoltTable vt = client.callProcedure("@AdHoc", "select * from eng_13059_r1 order by id").getResults()[0];
            assertContentOfTable(new Object[][] {
                {0, null, "HELLO", null,      null},
                {1, 1,    "foo",   "inlined", new byte[] {0x48, 0x45, 0x4C, 0x4C, 0x4F}}
            }, vt);
        }
    }
}
