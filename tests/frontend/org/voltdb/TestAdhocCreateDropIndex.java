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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

import java.io.IOException;
import java.util.stream.Stream;

public class TestAdhocCreateDropIndex extends AdhocDDLTestBase {

    // Add a test for partitioning a table

    @Test
    public void testBasicCreateIndex() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "create table FOO (" +
                     "ID integer not null," +
                     "VAL bigint, " +
                     "constraint PK_TREE primary key (ID)" +
                     ");\n" +
                     "create table FOO_R (" +
                     "ID integer not null," +
                     "VAL bigint, " +
                     "constraint PK_TREE_R primary key (ID)" +
                     ");\n" +
                     "Partition table FOO on column ID;\n";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            // Create index on tables
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index FOODEX on FOO (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on a partitioned table");
            }
            assertTrue(findIndexInSystemCatalogResults("FOODEX"));
            // Create index on replicated tables
            assertFalse(findIndexInSystemCatalogResults("FOODEX_R"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index FOODEX_R on FOO_R (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on a replicated table");
            }
            assertTrue(findIndexInSystemCatalogResults("FOODEX_R"));

            // Create unique index on partitioned tables
            assertFalse(findIndexInSystemCatalogResults("UNIQFOODEX"));
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "create assumeunique index UNIQFOODEX on FOO (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a unique index on a partitioned table");
            }
            assertTrue(findIndexInSystemCatalogResults("UNIQFOODEX"));
            // Can create redundant unique index on a table
            try {
                m_client.callProcedure("@AdHoc",
                        "create unique index UNIQFOODEX2 on FOO (ID);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create redundant unique index");
            }
            // It's going to get dropped because it's redundant, so don't expect to see it here
            assertFalse(findIndexInSystemCatalogResults("UNIQFOODEX2"));

            // drop an index we added
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index FOODEX;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop an index");
            }
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
            // can't drop it twice
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index FOODEX;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop bad index without if exists", threw);
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
            // unless we use if exists
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index FOODEX if exists;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a bad index with if exists");
            }
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreatePartialIndex() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "create table FOO (" +
                     "ID integer not null," +
                     "TS timestamp, " +
                     "constraint PK_TREE primary key (ID)" +
                     ");\n" +
                     "partition table FOO on column ID;\n" +
                     "create table FOO_R (" +
                     "ID integer not null," +
                     "TS timestamp, " +
                     "constraint PK_TREE_R primary key (ID)" +
                     ");\n" +
                     "";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            // Create a partial index on the partitioned table
            assertFalse(findIndexInSystemCatalogResults("partial_FOO_ts"));
            try {
                // Use a timestamp constant to validate ENG-9283
                m_client.callProcedure("@AdHoc",
                        "create index partial_FOO_ts on FOO (TS) where TS > '2000-01-01';");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a partial index on a partitioned table");
            }
            assertTrue(findIndexInSystemCatalogResults("partial_FOO_ts"));

            // Create a partial index on the replicated table.
            // It is unlikely that switching to use a replicated table will
            // uncover a failure when the partitioned table test apparently
            // succeeded, UNLESS that partitioned table schema change
            // succeeded BUT left the schema in a compromised state that is
            // operational but no longer mutable.
            // This has happened in the past because of issues with
            // regenerating the SQL DDL syntax that effectively recreates the
            // pre-existing schema. This kind of error will only be discovered
            // by a subsequent attempt to alter the schema.
            // Uncovering that failure mode may be the most useful role
            // of this additional test step.
            assertFalse(findIndexInSystemCatalogResults("partial_FOO_R_ts"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index partial_FOO_R_ts on FOO_R (TS) where TS > '2000-01-01';");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a partial index on a replicated table" +
                     " after apparently succeeding with a partitioned table.");
            }
            assertTrue(findIndexInSystemCatalogResults("partial_FOO_R_ts"));

        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreateDropIndexonView() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "create table FOO (" +
                     "ID integer not null," +
                     "VAL bigint, " +
                     "VAL1 float," +
                     "constraint PK_TREE primary key (ID)" +
                     ");\n" +
                     "Partition table FOO on column ID;\n";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            // create a basic view
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));
            try {
                m_client.callProcedure("@AdHoc",
                    "create view FOOVIEW (VAL, VAL1, TOTAL) as " +
                    "select VAL, VAL1, COUNT(*) from FOO group by VAL, VAL1;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a view");
            }
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));

            // Create index on view
            assertFalse(findIndexInSystemCatalogResults("VALDEX"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index SimpleIndex on FOOVIEW (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on a view");
            }
            assertTrue(findIndexInSystemCatalogResults("SimpleIndex"));

            // drop index
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index SimpleIndex;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop an index on a view");
            }
            assertFalse(findIndexInSystemCatalogResults("SimpleIndex"));

            // can't drop index twice
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index SimpleIndex;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop bad index without if exists", threw);
            assertFalse(findIndexInSystemCatalogResults("SimpleIndex"));

            // should be able to execute drop index on non-existing index
            // with "if exists" clause
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index SimpleIndex if exists;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a bad index with if exists");
            }
            assertFalse(findIndexInSystemCatalogResults("SimpleIndex"));

            // recreate index
            try {
                m_client.callProcedure("@AdHoc",
                        "create index ComplexIndex on FOOVIEW (VAL, TOTAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to recreate an index on a view");
            }
            assertTrue(findIndexInSystemCatalogResults("ComplexIndex"));

            // drop index
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index ComplexIndex if exists;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop an index on a view");
            }
            assertFalse(findIndexInSystemCatalogResults("ComplexIndex"));
        }
        finally {
            teardownSystem();
        }
    }

    public static String unsafeIndexExprErrorMsg = "The index definition uses operations that cannot be applied";


    private void ENG12024TestHelper(String ddlTemplate, String testExpression, boolean isSafeForDDL) throws Exception {
        String ddl = String.format(ddlTemplate, testExpression);

        // Create index on empty table first.
        assertFalse(findIndexInSystemCatalogResults("IDX_ENG_12024"));
        try {
            m_client.callProcedure("@AdHoc", ddl);
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Should be able to create an index on a empty table:\n" + ddl);
        }
        assertTrue(findIndexInSystemCatalogResults("IDX_ENG_12024"));
        try {
            m_client.callProcedure("@AdHoc", "DROP INDEX IDX_ENG_12024;");
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Should be able to drop index IDX_ENG_12024.");
        }
        assertFalse(findIndexInSystemCatalogResults("IDX_ENG_12024"));

        // Populate the table with some data and try again.
        try {
            m_client.callProcedure("@AdHoc", "INSERT INTO T_ENG_12024 VALUES (1, 2, 'ABC');");
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Should be able to insert data into T_ENG_12024.");
        }
        try {
            m_client.callProcedure("@AdHoc", ddl);
            if (! isSafeForDDL) {
                fail("Create index DDL on non-empty table with unsafe operators did not fail as expected:\n" + ddl);
            }
        }
        catch (ProcCallException pce) {
            if (isSafeForDDL || ! pce.getMessage().contains(unsafeIndexExprErrorMsg)) {
                fail("Unexpected create index DDL failure (isSafeForDDL = " +
                        (isSafeForDDL ? "true" : "false") + "):\n" + ddl);
            }
        }
        m_client.callProcedure("@AdHoc", "DROP INDEX IDX_ENG_12024 IF EXISTS;");
        m_client.callProcedure("@AdHoc", "DELETE FROM T_ENG_12024;");
    }

    @Test
    public void testCreateUnsafeIndexOnNonemptyTable() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "CREATE TABLE T_ENG_12024 (a INT, b INT, c VARCHAR(10));";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            String ddlTemplateForColumnExpression = "CREATE INDEX IDX_ENG_12024 ON T_ENG_12024(%s);";
            String ddlTemplateForBooleanExpression = "CREATE INDEX IDX_ENG_12024 ON T_ENG_12024(a) WHERE %s;";

            ENG12024TestHelper(ddlTemplateForColumnExpression, "a", true);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a + b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a - b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a * b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a / b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "c || c", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "repeat(c, 100)", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "LOG(a), b", false);

            ENG12024TestHelper(ddlTemplateForBooleanExpression, "NOT a > b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a IS NULL", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a = b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a <> b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a < b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a > b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a <= b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a >= b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "c LIKE 'abc%'", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "c STARTS WITH 'abc'", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a IS NOT DISTINCT FROM b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a > b AND b > 0", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a > b OR b > 0", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "LOG10(b) < 1", false);
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testENG15047() {
        final VoltDB.Configuration config = new VoltDB.Configuration();
        final String ddl = "CREATE TABLE P4 (\n" +
                        "INT     INTEGER  DEFAULT 0 PRIMARY KEY, -- also crashes on UNIQUE\n" +
                        "VCHAR_JSON        VARCHAR(100) DEFAULT 'foo' NOT NULL," +
                        "BIG     BIGINT   DEFAULT 0,\n" +
                        ");\n" +
                        "CREATE INDEX DIDX0 ON P4 (LOG10(P4.BIG));";
        try {
            createSchema(config, ddl, 2, 1, 0);
            startSystem(config);
            Stream.of(
                    "INSERT INTO P4(BIG) values(1);",                   // passes
                    "INSERT INTO P4(VCHAR_JSON) VALUES('0');",          // fails: constraint violation
                    "UPSERT INTO P4(INT, VCHAR_JSON) VALUES(1, '0');",  // fails: constraint violation
                    "UPDATE P4 SET BIG = 0 WHERE BIG = 1;",             // fails: constraint violation
                    "SELECT DISTINCT * FROM P4;")                       // passes
                    .forEachOrdered(stmt -> {
                        try {
                            m_client.callProcedure("@AdHoc", stmt);
                        } catch (IOException | ProcCallException e) { } // ignore query execution exceptions
                    });
        } catch (Exception e) {
            // ignore exceptions
        } finally {
            try {
                teardownSystem();
            } catch (Exception e) {}
        }
    }

    @Test
    public void testENG15213() throws Exception {
        final VoltDB.Configuration config = new VoltDB.Configuration();
        final String ddl = "CREATE TABLE P5 (i INTEGER, j FLOAT);";
        try {
            createSchema(config, ddl, 2, 1, 0);
            startSystem(config);
            Stream.of(
                    Pair.of("CREATE INDEX PI1 ON P5(i, j);", true),                         // normal index on columns only: passes
                    Pair.of("CREATE INDEX PI2 ON P5(i, LOG10(j));", true),                  // normal index with unsafe expression on empty table: passes
                    Pair.of("CREATE INDEX PI3 ON P5(i) WHERE j > 0;", true),                // partial index with columns and safe predicate on empty table: passes
                    Pair.of("CREATE INDEX PI4 ON P5(i) WHERE LOG(j) > 1 OR j <= 0;", true), // partial index with columns and unsafe predicate on empty table: passes
                    Pair.of("CREATE INDEX PI5 ON P5(LOG(i)) WHERE LOG(j) > 1 OR j <= 0;", true), // partial index with unsafe expression and unsafe predicate on empty table: passes
                    Pair.of("DROP INDEX PI2; DROP INDEX PI4; DROP INDEX PI5", true),        // clean up indexes with unsafe operations
                    Pair.of("INSERT INTO P5 values(0, 0);", true),                          // Table has rows: passes
                    Pair.of("CREATE INDEX PI11 ON P5(i, j);", true),                        // normal index on columns only: passes
                    Pair.of("CREATE INDEX PI31 ON P5(i) WHERE j > 0;", true),               // partial index with columns and safe predicate on non-empty table: passes
                    Pair.of("CREATE INDEX PI21 ON P5(i, LOG10(j));", false),                 // normal index with unsafe expression on non-empty table: rejected
                    Pair.of("CREATE INDEX PI41 ON P5(i) WHERE LOG(j) > 1 OR j <= 0;", false),// partial index with columns and unsafe predicate on non-empty table: rejected
                    Pair.of("CREATE INDEX PI51 ON P5(LOG(i)) WHERE LOG(j) > 1 OR j <= 0;", false))  // partial index with unsafe expression and unsafe predicate on non-empty table: rejected
                    .forEachOrdered(stmtAndShouldPass -> {
                        final String stmt = stmtAndShouldPass.getFirst();
                        final boolean shouldPass = stmtAndShouldPass.getSecond();
                        try {
                            m_client.callProcedure("@AdHoc", stmt);
                            assertTrue("Query \"" + stmt + "\" should pass", shouldPass);
                        } catch (IOException | ProcCallException e) {
                            assertFalse("Query \"" + stmt + "\" should fail", shouldPass);
                        }
                    });
        } finally {
            try {
                teardownSystem();
            } catch (Exception e) {}
        }

    }

    @Test
    public void testENG15220() throws Exception {
        final VoltDB.Configuration config = new VoltDB.Configuration();
        final String ddl = "CREATE TABLE R1 (ID INTEGER NOT NULL PRIMARY KEY, TINY TINYINT);";
        try {
            createSchema(config, ddl, 2, 1, 0);
            startSystem(config);
            Stream.of(
                    "CREATE VIEW VR6 (TINY, ID) AS SELECT TINY, MIN(ID) FROM R1 GROUP BY TINY;",
                    "INSERT INTO R1(ID, TINY) VALUES(1, 11);",
                    "CREATE ASSUMEUNIQUE INDEX DIDX2 ON R1 (ID);",
                    "CREATE UNIQUE INDEX DIDX20 ON R1 (ID);")
                    .forEachOrdered(stmt -> {
                        try {
                            m_client.callProcedure("@AdHoc", stmt);
                        } catch (IOException | ProcCallException e) {
                            fail("Query \"" + stmt + "\" should have worked fine");
                        }
                    });
            Stream.of(
                    Pair.of("DROP VIEW VR6;", true),
                    Pair.of("TRUNCATE TABLE R1;", true),                            // roll back
                    Pair.of("PARTITION TABLE R1 ON COLUMN ID;", true),              // partitioned table
                    Pair.of("CREATE VIEW VR6 (TINY, ID) AS SELECT TINY, MIN(ID) FROM R1 GROUP BY TINY;", true),
                    Pair.of("INSERT INTO R1(ID, TINY) VALUES(1, 11);", true),
                    Pair.of("CREATE ASSUMEUNIQUE INDEX DIDX2 ON R1 (ID);", false))  // "ASSUMEUNIQUE is not valid for an index that includes the partitioning column. Please use UNIQUE instead"
                    .forEachOrdered(stmtAndShouldPass -> {
                        final String stmt = stmtAndShouldPass.getFirst();
                        final boolean shouldPass = stmtAndShouldPass.getSecond();
                        try {
                            m_client.callProcedure("@AdHoc", stmt);
                            assertTrue("Query \"" + stmt + "\" should have passed", shouldPass);
                        } catch (IOException | ProcCallException e) {
                            assertFalse("Query \"" + stmt + "\" should have failed", shouldPass);
                        }
                    });
        } finally {
            try {
                teardownSystem();
            } catch (Exception e) {}
        }

    }

    @Test
    public void testENG15273() throws Exception {
        final VoltDB.Configuration config = new VoltDB.Configuration();
        final String ddl = "CREATE TABLE R21 (\n" +
                "ID INTEGER NOT NULL,\n" +
                "POLYGON GEOGRAPHY,\n" +
                "PRIMARY KEY (ID)\n" +
                ");\n" +
                "CREATE INDEX IDX_R21_POLY ON R21 (POLYGON);\n";
        try {
            createSchema(config, ddl, 2, 1, 0);
            startSystem(config);
            Stream.of(
                    "insert into R21 (ID) values -9355;",
                    "CREATE VIEW DV1 (POLYGON) AS SELECT MIN(POLYGON) FROM R21;",
                    "insert into R21 (ID, POLYGON) values 84, PolygonFromText('POLYGON((0.0 0.0, 0.0 -64.0, 51.0 0.0, 0.0 0.0))');",
                    "UPDATE R21 SET POLYGON = POLYGON;")
                    .forEachOrdered(stmt -> {
                        try {
                            m_client.callProcedure("@AdHoc", stmt);
                        } catch (IOException | ProcCallException e) {
                            fail("Query \"" + stmt + "\" should have worked fine");
                        }
                    });
        } finally {
            try {
                teardownSystem();
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testENG15742() throws Exception {
        final VoltDB.Configuration config = new VoltDB.Configuration();
        final String ddl = "CREATE TABLE R21 (\n" +
                "VB VARBINARY NOT NULL\n" +
                ");\n";
        try {
            createSchema(config, ddl, 2, 1, 0);
            startSystem(config);
            // we don't allow using VARBINARY type in LIKE/STARTS WITH expression
            Stream.of(
                    "CREATE INDEX testIdx ON R21(VB) WHERE x'03' like VB;",
                    "CREATE INDEX testIdx ON R21(VB) WHERE VB starts with x'03';")
                    .forEachOrdered(stmt -> {
                        try {
                            m_client.callProcedure("@AdHoc", stmt);
                            fail("We should not allow allow using VARBINARY type in LIKE/STARTS WITH expression");
                        } catch (ProcCallException pce) {
                            pce.printStackTrace();
                            assertTrue(pce.getMessage().contains("incompatible data type in operation"));
                            try {
                                assertFalse(findIndexInSystemCatalogResults("testIdx"));
                            } catch (Exception e) {
                                fail(e.getMessage());
                            }
                        } catch (IOException e) {
                            fail("Query \"" + stmt + "\" should have worked fine");
                        }
                    });
        } finally {
            try {
                teardownSystem();
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testENG15734() throws Exception {
        final VoltDB.Configuration config = new VoltDB.Configuration();
        final String ddl = "CREATE TABLE P4 (\n" +
                "ID INTEGER,\n" +
                "FL FLOAT,\n" +
                "V1 VARCHAR(63),\n" +
                "V2 VARCHAR(64) DEFAULT '0'" +
                ");\n" +
                "CREATE INDEX DIDX4 ON P4(ID) WHERE V2 LIKE UPPER(V1);\n";
        try {
            createSchema(config, ddl, 2, 1, 0);
            startSystem(config);
            Stream.of(
                    "INSERT INTO P4(FL) VALUES(6.2);",
                    "UPDATE P4 SET V1 = V2;",
                    "TRUNCATE TABLE P4;")
                    .forEachOrdered(stmt -> {
                        try {
                            m_client.callProcedure("@AdHoc", stmt);
                        } catch (IOException | ProcCallException e) {
                            fail("Query \"" + stmt + "\" should have worked fine");
                        }
                    });
        } finally {
            try {
                teardownSystem();
            } catch (Exception e) { }
        }
    }
}
