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

package org.voltdb.compiler;

import org.junit.After;
import org.junit.Before;

import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;

import junit.framework.TestCase;

/**
 * This file is based on, and now contains, the 'export to target' tests from
 * TestVoltCompiler; it's a separate file because TestVoltCompiler was edging
 * up to 4000 lines, which is quite large enough for one test suite.
 *
 * Specifically, this file has tests for
 * CREATE TABLE ... EXPORT|MIGRATE TO TARGET|TOPIC
 * and related forms of statements such as ALTER TABLE.
 *
 * The bizarre RANDOM upcasing of some KEYWORDS but not OTHERS comes from
 * TestVoltCompiler; please don't blame me.
 *
 */

public class TestVoltCompilerTableExport extends TestCase {

    @Override
    @Before
    public void setUp() {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", getName());
    }

    @Override
    @After
    public void tearDown() {
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", getName());
    }


    /////////////////////////////////////////////
    // COMMON COMMAND EXECUTION
    ////////////////////////////////////////////

    void execOne(String ddl) {
        try {
            VoltProjectBuilder pb = new VoltProjectBuilder();
            pb.addLiteralSchema(ddl);
            boolean result = pb.compile(Configuration.getPathToCatalogForTest("test-table-export.jar"));
            if (!result) {
                System.err.printf("*** Compilation failed for command:%n%s%n*** Expected success%n", ddl);
                fail("Unexpected compilation failure (see stderr)");
            }
        }
        catch (Exception ex) {
            System.err.printf("*** Unexpected exception %s for command:%n%s%n", ex.getClass().getSimpleName(), ddl);
            ex.printStackTrace();
            fail("Unexpected exception (see stderr)");
        }
    }

    void execOneNeg(String ddl) {
        try {
            VoltProjectBuilder pb = new VoltProjectBuilder();
            pb.addLiteralSchema(ddl);
            boolean result = pb.compile(Configuration.getPathToCatalogForTest("test-table-export.jar"));
            if (result) {
                System.err.printf("*** Compilation did not fail for command:%n%s%n*** Expected failure%n", ddl);
                fail("Unexpected compilation success (see stderr)");
            }
        }
        catch (Exception ex) {
            System.err.printf("*** Unexpected exception %s for command:%n%s%n", ex.getClass().getSimpleName(), ddl);
            ex.printStackTrace();
            fail("Unexpected exception (see stderr)");
        }
    }

    void exec(String ddlFmt) {
        execOne(insert(ddlFmt, "TARGET"));
        execOne(insert(ddlFmt, "TOPIC"));
    }

    void execNeg(String ddlFmt) {
        execOneNeg(insert(ddlFmt, "TARGET"));
        execOneNeg(insert(ddlFmt, "TOPIC"));
    }

    String insert(String ddlFmt, String type) {
        String[] s = ddlFmt.split("%s", -1);
        if (s.length < 2) {
            System.err.printf("*** No format specifier in command:%n%s%n", ddlFmt);
            fail("Missing format specifier (see stderr)");
        }
        return String.join(type, s);
    }


    /////////////////////////////////////////////
    // EXPORT TO TARGET OR TOPIC
    ////////////////////////////////////////////

    public void testExport() {
        // basic case
        exec("create table foo EXPORT TO %s foo (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // with ON filer
        exec("create table foo EXPORT TO %s foo ON INSERT,DELETE,UPDATE_NEW (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");
    }

    public void testExportNegative() {
        // can't mix UPDATE and UPDATE_NEW
        execNeg("create table foo EXPORT TO %s foo ON INSERT,DELETE,UPDATE,UPDATE_NEW (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // should use UPDATE instead of UPDATE_OLD + UPDATE_NEW
        execNeg("create table foo EXPORT TO %s foo ON UPDATE_OLD,UPDATE_NEW (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // bad trigger
        execNeg("create table foo EXPORT TO %s foo ON INSERT,BANANA,DELETE (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");
    }

    public void testExportWithTTL() {
        // basic case
        exec("create table foo EXPORT TO %s foo (a integer NOT NULL, b integer, c timestamp default now() not null, PRIMARY KEY(a)) \n" +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;");

        // TTL column must not be nullable
        execNeg("create table foo EXPORT TO %s foo (a integer NOT NULL, b integer, c timestamp, PRIMARY KEY(a)) \n" +
                "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;");

        // TTL column must be timestamp
        execNeg("create table foo EXPORT TO %s foo (a integer NOT NULL, b integer, c bigint not null, PRIMARY KEY(a)) \n" +
                "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;");

        // with ON
        exec("create table foo EXPORT TO %s bar ON INSERT,DELETE,UPDATE (a integer NOT NULL, b integer, c timestamp default now() not null, PRIMARY KEY(a)) \n" +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;");
    }

    public void testExportAlter() {
        // alter ON triggers
        exec("create table foo EXPORT TO %s foo ON UPDATE_OLD (a integer NOT NULL, b integer, PRIMARY KEY(a));\n" +
             "alter table foo  alter EXPORT TO %s foo ON UPDATE_NEW;\n");

        // alter ON triggers, defaulting to insert
        exec("create table foo EXPORT TO %s foo ON UPDATE_OLD (a integer NOT NULL, b integer, PRIMARY KEY(a));\n" +
             "alter table foo  alter EXPORT TO %s foo;\n");

        // can't alter target/topic name
        execNeg("create table foo EXPORT TO %s foo ON UPDATE (a integer NOT NULL, b integer, PRIMARY KEY(a));\n" +
                "alter table foo  alter EXPORT TO %s foobar ON UPDATE;\n");

        // there's no ALTER EXPORT... WITH (yet?)
        execNeg("create table foo EXPORT TO %s foo WITH KEY (a) VALUE (b) (a integer NOT NULL, b integer, c integer PRIMARY KEY(a));\n" +
                "alter table foo  alter EXPORT TO %s foo WITH KEY (a) VALUE (b, c);\n");

        // alter ttl (except column name)
        exec("create table ttl EXPORT TO %s banana (a integer not null, b integer, c timestamp default now() not null, d timestamp not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "alter table ttl alter USING TTL 1234 SECONDS ON COLUMN c BATCH_SIZE 200 MAX_FREQUENCY 20;\n");

        // alter ttl column name
        exec("create table ttl EXPORT TO %s banana (a integer not null, b integer, c timestamp default now() not null, d timestamp not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "alter table ttl alter USING TTL 20 MINUTES ON COLUMN d BATCH_SIZE 10 MAX_FREQUENCY 3;\n");

        // alter ttl column name, default batch and freq
        exec("create table ttl EXPORT TO %s banana (a integer not null, b integer, c timestamp default now() not null, d timestamp not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "alter table ttl alter USING TTL 20 MINUTES ON COLUMN d;\n");

        // add TTL to export
        exec("create table ttl EXPORT TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a));\n " +
             "alter table ttl add USING TTL 20 MINUTES ON COLUMN c;\n");

        // drop TTL from export
        exec("create table ttl EXPORT TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c;\n" +
             "alter table ttl drop TTL;\n");
    }


    /////////////////////////////////////////////
    // MIGRATE TO TARGET OR TOPIC
    ////////////////////////////////////////////

    public void testMigrate() {
        // basic case, migrate with TTL
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             " USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n");

        // partitioned
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c;\n" +
             "partition table ttl on column a;\n");

        // with explicit batch size; partitioned
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10;\n" +
             "partition table ttl on column a;\n");

        // explicit batch size, max freq; partitioned
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "partition table ttl on column a;\n");

        // with batch size, max freq, partitioned and DR
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "partition table ttl on column a;\n" +
             "dr table ttl;");

        // no TTL; partitioned
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, PRIMARY KEY(a));\n" +
             "partition table ttl on column a;\n");
    }

    public void testMigrateNegative() {
        // shouldn't have batch size here since it is a parameter for ttl
        execNeg("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, PRIMARY KEY(a)) " +
                "BATCH_SIZE 10;\n" +
                "partition table ttl on column a;\n");

        // with ON filter - not applicable to MIGRATE
        execNeg("create table foo MIGRATE TO %s foo ON INSERT,DELETE,UPDATE_NEW (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");
    }

    public void testMigrateAlter() {
        // there is no 'alter migrate' syntax
        execNeg("create table foo MIGRATE TO %s foo ON UPDATE_OLD (a integer NOT NULL, b integer, PRIMARY KEY(a));\n" +
                "alter table foo  alter MIGRATE TO %s foo ON UPDATE_NEW;\n");

        // alter ttl (except column name)
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "alter table ttl alter USING TTL 1200 SECONDS ON COLUMN c BATCH_SIZE 200 MAX_FREQUENCY 20;\n");

        // alter ttl column name - not allowed for migrate
        execNeg("create table ttl MIGRATE TO %s banana (a integer not null, b integer, c timestamp default now() not null, d timestamp not null, PRIMARY KEY(a)) " +
                "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
                "alter table ttl alter USING TTL 20 MINUTES ON COLUMN d;\n");

        // alter ttl, order of clauses changed
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "alter table ttl alter USING TTL 1201 SECONDS ON COLUMN c MAX_FREQUENCY 20 BATCH_SIZE 200 ;\n");

        // alter ttl, default batch size
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c MAX_FREQUENCY 20;\n");

        // alter ttl, default max freq
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10;\n" +
             "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 200 MAX_FREQUENCY 20;\n");

        // alter ttl, default batch & freq
        exec("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
             "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
             "alter table ttl alter USING TTL 1200 SECONDS ON COLUMN c;\n");

        // Cannot add TTL on migrate
        execNeg("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a));\n " +
                "alter table ttl add USING TTL 20 MINUTES ON COLUMN c;\n");

         // Cannot drop TTL on migrate
        execNeg("create table ttl MIGRATE TO %s TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
                "USING TTL 20 MINUTES ON COLUMN c;\n" +
                "alter table ttl drop TTL;\n");
    }


    /////////////////////////////////////////////
    // EXPORT TO TARGET
    ////////////////////////////////////////////

    public void testExportToTarget() {
        // Nothing needed yet; keep as placeholder
    }

    public void testExportToTargetNegative() {
        // target can't become a topic
        execOneNeg("create table foo EXPORT TO TARGET foo ON UPDATE (a integer NOT NULL, b integer, PRIMARY KEY(a));\n" +
                   "alter table foo  alter EXPORT TO TOPIC foo ON UPDATE;\n");
    }


    /////////////////////////////////////////////
    // MIGRATE TO TARGET
    ////////////////////////////////////////////

    public void testMigrateToTarget() {
        // Nothing needed yet; keep as placeholder
    }

    public void testMigrateToTargetNegative() {
        // Nothing needed yet; keep as placeholder
    }


    /////////////////////////////////////////////
    // EXPORT TO TOPIC
    ////////////////////////////////////////////

    public void testExportToTopic() {
        // with WITH KEY,VALUE
        execOne("create table foo EXPORT TO TOPIC foo WITH KEY(a) VALUE(b)  (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // with WITH KEY
        execOne("create table foo EXPORT TO TOPIC foo WITH KEY(a)  (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // with WITH VALUE
        execOne("create table foo EXPORT TO TOPIC foo WITH VALUE(a,b)  (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // with ON and WITH
        execOne("create table foo EXPORT TO TOPIC foo ON INSERT,DELETE,UPDATE WITH KEY(a) VALUE(b) (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");
    }

    public void testExportToTopicNegative() {
        // topic can't become a target
        execOneNeg("create table foo EXPORT TO TOPIC foo ON UPDATE (a integer NOT NULL, b integer, PRIMARY KEY(a));\n" +
                   "alter table foo  alter EXPORT TO TARGET foo ON UPDATE;\n");

        // empty WITH clause
        execOneNeg("create table foo EXPORT TO TOPIC foo WITH (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // wrong order for ON and WITH
        execOneNeg("create table foo EXPORT TO TOPIC foo WITH KEY(a) VALUE(b) ON INSERT,DELETE,UPDATE (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");
    }


    /////////////////////////////////////////////
    // MIGRATE TO TOPIC
    ////////////////////////////////////////////

    public void testMigrateToTopic() {
        // with WITH KEY,VALUE
        execOne("create table foo MIGRATE TO TOPIC foo WITH KEY(a) VALUE(b)  (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // with WITH KEY
        execOne("create table foo MIGRATE TO TOPIC foo WITH KEY(a)  (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // with WITH VALUE
        execOne("create table foo MIGRATE TO TOPIC foo WITH VALUE(a,b)  (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");
    }

    public void testMigrateToTopicNegative() {
        // Nothing needed yet; keep as placeholder
    }


    /////////////////////////////////////////////
    // ODDBALLS
    ////////////////////////////////////////////

    public void testOddballCases() {
        // Can't export and migrate at the same time
        execNeg("create table foo EXPORT TO %s foo MIGRATE TO %s bar (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // Can't migrate and export at the same time
        execNeg("create table foo MIGRATE TO %s bar EXPORT TO %s foo (a integer NOT NULL, b integer, PRIMARY KEY(a));\n");

        // missing keyword 'target' or 'topic'
        execOneNeg("create table ttl MIGRATE TO TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
                   "USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10;\n" +
                   "partition table ttl on column a;\n");
    }
}
