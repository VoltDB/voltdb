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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.hsqldb_voltpatches.SQLLexer;
import org.hsqldb_voltpatches.SQLLexer.HSQLDDLNoun;
import org.hsqldb_voltpatches.SQLLexer.HSQLDDLVerb;
import org.junit.Test;

public class TestSQLLexer {

    private void checkDDL(final String strIn, final String expectString)
    {
        String result = SQLLexer.extractDDLToken(strIn);
        assertEquals(expectString, result);
    }

    @Test
    public void testDDLLexer()
    {
        checkDDL("", null);
        checkDDL("ryan loves the yankees", null);

        checkDDL("drop table pants", "drop");
        checkDDL("DROP table pants", "drop");
        checkDDL("dRoP table pants", "drop");
        checkDDL("   drop table pants    ", "drop");
        checkDDL("droptable pants", null);
        checkDDL("-- drop table pants", null);
        checkDDL("droop table pants", null);

        checkDDL("create table pants", "create");
        checkDDL("CREATE table pants", "create");
        checkDDL("CrEaTe table pants", "create");
        checkDDL("   create table pants    ", "create");
        checkDDL("createtable pants", null);
        checkDDL("-- create table pants", null);
        checkDDL("crate table pants", null);

        checkDDL("alter table pants", "alter");
        checkDDL("ALTER table pants", "alter");
        checkDDL("ALteR table pants", "alter");
        checkDDL("   alter table pants    ", "alter");
        checkDDL("altertable pants", null);
        checkDDL("-- alter table pants", null);
        checkDDL("altar table pants", null);

        checkDDL("export table pants", "export");
        checkDDL("EXPORT table pants", "export");
        checkDDL("ExPoRt table pants", "export");
        checkDDL("   export table pants    ", "export");
        checkDDL("exporttable pants", null);
        checkDDL("-- export table pants", null);
        checkDDL("exprot table pants", null);

        checkDDL("import class org.dont.exist", "import");
        checkDDL("IMPORT class org.dont.exist", "import");
        checkDDL("ImPoRt class org.dont.exist", "import");
        checkDDL("    import class org.dont.exist", "import");
        checkDDL("importclass org.dont.exist", null);
        checkDDL("-- import class org.dont.exist", null);
        checkDDL("improt class org.dont.exist", null);

        checkDDL("partition table pants", "partition");
        checkDDL("PARTITION table pants", "partition");
        checkDDL("pArTiTioN table pants", "partition");
        checkDDL("   partition table pants    ", "partition");
        checkDDL("partitiontable pants", null);
        checkDDL("-- partition table pants", null);
        checkDDL("partitoin table pants", null);
    }

    @Test
    public void testIsPermitted()
    {
        assertTrue(SQLLexer.isPermitted("create table PANTS (ID int, RENAME varchar(50));"));
        assertTrue(SQLLexer.isPermitted("create table PANTS (\n ID int,\n RENAME varchar(50)\n);"));
        assertTrue(SQLLexer.isPermitted("create view PANTS (ID int, RENAME varchar(50));"));
        assertTrue(SQLLexer.isPermitted("create index PANTS (ID int, RENAME varchar(50));"));
        assertFalse(SQLLexer.isPermitted("create tabel PANTS (ID int, RENAME varchar(50));"));
        assertFalse(SQLLexer.isPermitted("craete table PANTS (ID int, RENAME varchar(50));"));
        assertTrue(SQLLexer.isPermitted("create role pants with pockets;"));
        assertTrue(SQLLexer.isPermitted("create role\n pants\n with cuffs;\n"));

        assertTrue(SQLLexer.isPermitted("drop table pants;"));
        assertTrue(SQLLexer.isPermitted("drop view pants;"));
        assertTrue(SQLLexer.isPermitted("drop index pants;"));
        assertFalse(SQLLexer.isPermitted("dorp table pants;"));
        assertFalse(SQLLexer.isPermitted("drop tabel pants;"));

        assertTrue(SQLLexer.isPermitted("alter table pants add column blargy blarg;"));
        assertTrue(SQLLexer.isPermitted("alter table pants add constraint blargy blarg;"));
        assertTrue(SQLLexer.isPermitted("alter index pants"));
        assertFalse(SQLLexer.isPermitted("alter table pants rename to shorts;"));
        assertFalse(SQLLexer.isPermitted("alter index pants rename to shorts;"));
        assertFalse(SQLLexer.isPermitted("alter table pants alter column rename to shorts;"));
        assertFalse(SQLLexer.isPermitted("altre table pants blargy blarg;"));
        assertFalse(SQLLexer.isPermitted("alter tabel pants blargy blarg;"));
    }

    void checkValidHSQLPreprocessing(String ddl, HSQLDDLVerb verb, HSQLDDLNoun noun, String name, String secondName, boolean ifexists, boolean cascade) {
        SQLLexer.HSQLDDLInfo info = SQLLexer.preprocessHSQLDDL(ddl);
        assertNotNull(info);
        assertEquals(verb, info.verb);
        assertEquals(noun, info.noun);
        assertEquals(name, info.name);
        assertEquals(secondName, info.secondName);
        assertEquals(ifexists, info.ifexists);
        assertEquals(cascade, info.cascade);
    }

    void checkInvalidHSQLPreprocessing(String ddl) {
        SQLLexer.HSQLDDLInfo info = SQLLexer.preprocessHSQLDDL(ddl);
        assertEquals(null, info);
    }

    @Test
    public void testHSQLPreprocessing() {
        // basic test
        checkValidHSQLPreprocessing("create table foo ...", HSQLDDLVerb.CREATE, HSQLDDLNoun.TABLE, "foo", null, false, false);

        // some tests borrowed from above
        checkValidHSQLPreprocessing("create table PANTS (ID int, RENAME varchar(50));", HSQLDDLVerb.CREATE, HSQLDDLNoun.TABLE, "pants", null, false, false);
        checkValidHSQLPreprocessing("create table PANTS (\n ID int,\n RENAME varchar(50)\n);", HSQLDDLVerb.CREATE, HSQLDDLNoun.TABLE, "pants", null, false, false);
        checkValidHSQLPreprocessing("create view PANTS (ID int, RENAME varchar(50));", HSQLDDLVerb.CREATE, HSQLDDLNoun.VIEW, "pants", null, false, false);
        checkValidHSQLPreprocessing("create index PANTS (ID int, RENAME varchar(50));", HSQLDDLVerb.CREATE, HSQLDDLNoun.INDEX, "pants", null, false, false);
        checkValidHSQLPreprocessing("create index PANTS on Fuz (ID int, RENAME varchar(50));", HSQLDDLVerb.CREATE, HSQLDDLNoun.INDEX, "pants", "fuz", false, false);
        checkInvalidHSQLPreprocessing("create tabel PANTS (ID int, RENAME varchar(50));");
        checkInvalidHSQLPreprocessing("craete table PANTS (ID int, RENAME varchar(50));");
        checkInvalidHSQLPreprocessing("create role pants with pockets;");
        checkInvalidHSQLPreprocessing("create role\n pants\n with cuffs;\n");
        checkValidHSQLPreprocessing("drop table pants;", HSQLDDLVerb.DROP, HSQLDDLNoun.TABLE, "pants", null, false, false);
        checkValidHSQLPreprocessing("drop table pants if exists;", HSQLDDLVerb.DROP, HSQLDDLNoun.TABLE, "pants", null, true, false);
        checkValidHSQLPreprocessing("drop view pants;", HSQLDDLVerb.DROP, HSQLDDLNoun.VIEW, "pants", null, false, false);
        checkValidHSQLPreprocessing("drop view pants cascade;", HSQLDDLVerb.DROP, HSQLDDLNoun.VIEW, "pants", null, false, true);
        checkValidHSQLPreprocessing("drop index pants   IF    EXISTS   CAsCaDe  ;", HSQLDDLVerb.DROP, HSQLDDLNoun.INDEX, "pants", null, true, true);
        checkInvalidHSQLPreprocessing("dorp table pants;");
        checkInvalidHSQLPreprocessing("drop tabel pants;");
        checkValidHSQLPreprocessing("drop table pants cascade if exists;", HSQLDDLVerb.DROP, HSQLDDLNoun.TABLE, "pants", null, true, false);// wrong cascade order
        checkValidHSQLPreprocessing("drop table pants if exists.;", HSQLDDLVerb.DROP, HSQLDDLNoun.TABLE, "pants", null, false, false); // period invalidates if exists
        checkValidHSQLPreprocessing("alter table pants add column blargy blarg;", HSQLDDLVerb.ALTER, HSQLDDLNoun.TABLE, "pants", null, false, false);
        checkValidHSQLPreprocessing("alter table pants add constraint blargy blarg;", HSQLDDLVerb.ALTER, HSQLDDLNoun.TABLE, "pants", null, false, false);
        checkValidHSQLPreprocessing("alter index pants", HSQLDDLVerb.ALTER, HSQLDDLNoun.INDEX, "pants", null, false, false);
        checkValidHSQLPreprocessing("alter table pants rename to shorts IF \nexists ;", HSQLDDLVerb.ALTER, HSQLDDLNoun.TABLE, "pants", null, true, false);
        checkValidHSQLPreprocessing("alter index pants rename to shorts CASCADE;", HSQLDDLVerb.ALTER, HSQLDDLNoun.INDEX, "pants", null, false, true);
        checkValidHSQLPreprocessing("alter table pants alter column rename to shorts;", HSQLDDLVerb.ALTER, HSQLDDLNoun.TABLE, "pants", null, false, false);
        checkValidHSQLPreprocessing("alter table pants alter column rename to shorts CASCADE;", HSQLDDLVerb.ALTER, HSQLDDLNoun.TABLE, "pants", null, false, true);
        checkValidHSQLPreprocessing("alter table FOO drop column VIEWCOL cascade;", HSQLDDLVerb.ALTER, HSQLDDLNoun.TABLE, "foo", null, false, true);
        checkInvalidHSQLPreprocessing("altre table pants blargy blarg;");
        checkInvalidHSQLPreprocessing("alter tabel pants blargy blarg;");
        checkValidHSQLPreprocessing("CREATE ASSUMEUNIQUE INDEX absVal ON T2 (     ABS(area * 2) ,   ABS(volume / 2) );", HSQLDDLVerb.CREATE, HSQLDDLNoun.INDEX, "absval", "t2", false, false);
    }
}
