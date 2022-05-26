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

package org.voltdb.utils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.voltcore.utils.CoreUtils;
import org.voltdb.catalog.DatabaseConfiguration;

public class TestSqlCommandParserInteractive extends TestCase {

    static ExecutorService executor = CoreUtils.getSingleThreadExecutor("TestSqlCommandParser");

    static class CommandStuff
    {
        PipedInputStream pis;
        ByteArrayOutputStream baos;
        PipedOutputStream pos;
        Future<List<String>> result = null;

        CommandStuff()
        {
            pis = new PipedInputStream();
            baos = new ByteArrayOutputStream();
            try {
                pos = new PipedOutputStream(pis);
            } catch (Exception e) {}
        }

        private Callable<List<String>> makeQueryTask(final InputStream in, final OutputStream out)
        {
            return new Callable<List<String>>() {
                @Override
                public List<String> call() {
                    return SQLCommand.getParserTestQueries(in, out);
                }
            };
        }

        public Future<List<String>> openQuery()
        {
            result = executor.submit(makeQueryTask(pis, baos));
            return result;
        }

        public void submitText(String text) throws Exception
        {
            pos.write(text.getBytes(), 0, text.length());
        }

        public void waitOnResult()
        {
            while (!result.isDone()) {
                Thread.yield();
            }
        }

        public Future<List<String>> getResult()
        {
            return result;
        }

        // we add a two spaces and character at the beginning of the prompt
        // to indicate a multi-line statement definition is in progress
        public boolean isContinuationPrompt() {
            // get the prompt after the last statement
            String lastPrompt = baos.toString().substring(baos.toString().lastIndexOf("\n") + 1);
            return lastPrompt.startsWith("  ");
        }

        public void close() throws Exception
        {
            pos.close();
        }
    }

    // Verify all the basic single line DML/DQL works
    public void testSimpleQueries() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String query = "select * from goats";
        cmd.submitText(query + ";\n");
        cmd.waitOnResult();
        assertEquals(1, result.get().size());
        assertEquals(query, result.get().get(0));
        System.out.println("RESULT: " + result.get());

        result = cmd.openQuery();
        query = "insert into goats values ('chicken', 'cheese')";
        cmd.submitText(query + ";\n");
        cmd.waitOnResult();
        assertEquals(1, result.get().size());
        assertEquals(query, result.get().get(0));
        System.out.println("RESULT: " + result.get());

        result = cmd.openQuery();
        query = "update goats set livestock = 'chicken' where dairy = 'cheese'";
        cmd.submitText(query + ";\n");
        cmd.waitOnResult();
        assertEquals(1, result.get().size());
        assertEquals(query, result.get().get(0));
        System.out.println("RESULT: " + result.get());

        result = cmd.openQuery();
        query = "delete from goats where dairy = 'cheese'";
        cmd.submitText(query + ";\n");
        cmd.waitOnResult();
        assertEquals(1, result.get().size());
        assertEquals(query, result.get().get(0));
        System.out.println("RESULT: " + result.get());
        cmd.close();
    }

    public void testSemicolonSeparation() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String query1 = "select * from goats";
        cmd.submitText(query1 + ";");
        Thread.sleep(100);
        assertFalse(result.isDone());
        String query2 = "delete from boats";
        // add some whitespace and extra ;;;
        cmd.submitText(query2 + " ;;  ; ;   ; ");
        Thread.sleep(100);
        assertFalse(result.isDone());
        String query3 = "insert into stoats values (0, 1)";
        cmd.submitText(query3 + ";\n");
        cmd.waitOnResult();
        List<String> got = result.get();
        assertEquals(3, got.size());
        assertEquals(query1, got.get(0));
        assertEquals(query2, got.get(1));
        assertEquals(query3, got.get(2));
        cmd.close();
    }

    public void testQuotedSemicolons() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        cmd.submitText("insert into goats values ('whywouldyoudothis?;', 'Ihateyou!')");
        // despite the semicolon/CR, that query is not finished
        Thread.sleep(100);
        //assertFalse(result.isDone());
        cmd.submitText(";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        // carriage return becomes a space in the parser
        assertEquals("insert into goats values ('whywouldyoudothis?;', 'Ihateyou!')",
                result.get().get(0));
        cmd.close();
    }

    public void testComments() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();

        assertFalse(cmd.isContinuationPrompt());
        cmd.submitText("--insert into goats values (0, 1); select * from goats;\n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        assertFalse(cmd.isContinuationPrompt());
        cmd.submitText(";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(0, result.get().size());

        result = cmd.openQuery();
        assertFalse(cmd.isContinuationPrompt());
        cmd.submitText("insert into goats values (0, 1)");
        Thread.sleep(100);
        assertFalse(result.isDone());
        // a new prompt is obtained only after \n
        assertFalse(cmd.isContinuationPrompt());
        cmd.submitText("; --select * from goats;\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        // Note: sqlcmd split the queries by semicolon
        // for each fragment, it removes comments at the beginning of a line,
        // trim whitespace, etc.
        assertEquals(1, result.get().size());
        assertEquals("insert into goats values (0, 1)", result.get().get(0));

        // test more comments
        result = cmd.openQuery();
        cmd.submitText("CREATE TABLE T (\n"
                + " column1 integer, -- comment\n"
                + " column2 integer,\n"
                + " -- column3 integer,\n"
                + "column4 integer);\n"
                + "\n"
                );
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());

        cmd.submitText("select * from T; -- select * from T;\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());

        cmd.close();
    }

    public void testUnionStatement() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String query = "select * from goats union select * from chickens";
        cmd.submitText(query + ";\n");
        cmd.waitOnResult();
        assertEquals(1, result.get().size());
        assertEquals(query, result.get().get(0));
        System.out.println("RESULT: " + result.get());
        cmd.close();
    }

    public void testCreateTable() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String create = "create table foo (col1 integer, col2 varchar(50) default ';')";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));
    }

    public void testMultiLineCreate() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        assertFalse(cmd.isContinuationPrompt());
        cmd.submitText("create table foo (\n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        assertTrue(cmd.isContinuationPrompt());
        cmd.submitText("col1 integer,\n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        assertTrue(cmd.isContinuationPrompt());
        cmd.submitText("col2 varchar(50) default ';'\n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        assertTrue(cmd.isContinuationPrompt());
        cmd.submitText(");\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals("create table foo (\ncol1 integer,\ncol2 varchar(50) default ';'\n)",
                result.get().get(0));
    }

    public void testAlterTable() throws Exception
    {
        String[] alterStmts = new String[] {
                "alter table foo add column newcol varchar(50)",
                "alter table foo drop column",
                "alter table foo alter column oldcol integer",
                // test various cases with whitespace and quoted IDs
                "alter table\"foo\"drop column",
                "alter table \"drop \" drop column",
                "alter table  \"foo\"\"foo\" alter column newcol integer",
                "alter table \"alter\" alter column foo float",
                "alter table \"create view\" drop column"
        };

        CommandStuff cmd = new CommandStuff();
        for (int i = 0; i < alterStmts.length; ++i) {
            Future<List<String>> result = cmd.openQuery();
            assertFalse(cmd.isContinuationPrompt());
            cmd.submitText(alterStmts[i] + ";\n");
            cmd.waitOnResult();
            System.out.println("RESULT: " + result.get());
            assertEquals(1, result.get().size());
            assertEquals(alterStmts[i], result.get().get(0));
        }
    }

    public void testDropTable() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String drop = "drop table foo if exists";
        cmd.submitText(drop + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(drop, result.get().get(0));
    }

    public void testCreateView() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        assertFalse(cmd.isContinuationPrompt());
        String create = "create view foo (col1, col2) as select col1, count(*) from foo group by col1";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));

        // From ENG-6641
        result = cmd.openQuery();
        assertFalse(cmd.isContinuationPrompt());
        create = "create view foo\n" +
                 "(\n" +
                 "C1\n" +
                 ",C2\n" +
                 ", TOTAL\n" +
                 ")\n" +
                 "as\n" +
                 "select C1\n" +
                 ", C2\n" +
                 ", COUNT(*)\n" +
                 "from bar\n" +
                 "group by C1\n" +
                 ", C2\n";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
    }

    public void testCreateProcedure() throws Exception
    {
        // Check all the DQL/DML possibilities, plus combined subquery select
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String create = "create procedure foo as select col1, count(*) from foo group by col1";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));

        result = cmd.openQuery();
        create = "create procedure foo as select foo, bar from (select goat, chicken from hats) bats where bats.wings > 1";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));

        result = cmd.openQuery();
        create = "create procedure foo as insert into foo values (0, 1)";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));

        result = cmd.openQuery();
        create = "create procedure foo as update goats set livestock = 'chicken' where dairy = 'cheese'";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));

        result = cmd.openQuery();
        create = "create procedure foo as delete from goats where livestock = 'chicken'";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));

        // test with role
        result = cmd.openQuery();
        create = "create procedure foo allow default,adhoc as select foo, bar from (select goat, chicken from hats) bats where bats.wings > 1";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));
    }

    public void testCreateMultiStmtProcedure() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        cmd.submitText("create procedure pr as begin\n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("select * from t;\n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("insert into t values (1);\n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("end;\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals("create procedure pr as begin\nselect * from t;\ninsert into t values (1);\nend",
                result.get().get(0));

        String sql = "create procedure thisproc as "
        + "begin \n"
        + "select * from r where f = 'begin' or f = 'END';"
        + "select a, "
        + "case when a > 100.00 then 'Expensive' else 'Cheap' end "
        + "from t;"
        + "end";
        result = cmd.openQuery();
        cmd.submitText("create procedure thisproc as begin \n");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("select * from r where f = 'begin' or f = 'END';");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("select a, case when a > 100.00 then 'Expensive' else 'Cheap' end ");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("from t;");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("end;\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(sql, result.get().get(0));

        // case/end with no whitespace before and after it
        sql = "create procedure thisproc as begin "
                + "SELECT a, "
                + "10+case when id < 0 then (id+0)end+100 from aaa;"
                + "end";
        result = cmd.openQuery();
        cmd.submitText("create procedure thisproc as begin ");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("SELECT a, 10+case when id < 0 then (id+0)end+100 from aaa;");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("end;\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(sql, result.get().get(0));

        sql = "create procedure mumble as begin "
                + "select * from t order by case when t.a < 1 then asc else desc end; "
                + "end";
        result = cmd.openQuery();
        cmd.submitText("create procedure mumble as begin ");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("select * from t order by case when t.a < 1 then asc else desc end; ");
        Thread.sleep(100);
        assertFalse(result.isDone());
        cmd.submitText("end;\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(sql, result.get().get(0));
    }

    public void testSubQuery() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String query = "select foo, bar from (select goat, chicken from hats) bats where bats.wings > 1";
        cmd.submitText(query + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(query, result.get().get(0));
    }

    public void testExec() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String exec = "exec selectGoats ';' 'dude' 2";
        cmd.submitText(exec + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(exec, result.get().get(0));
    }

    public void testInsertIntoSelect() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String insert = "insert into hats (foo, bar) select goat, chicken from hats";
        cmd.submitText(insert + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(insert, result.get().get(0));

        // Test double-quoted identifiers with embedded parentheses
        // and escaped double quotes.
        result = cmd.openQuery();
        insert = "insert into\"hats\" (\"foo\", \"b\"\"ar\") " +
                "( ( ( (((( (select goat, chicken from hats))))))))";
        cmd.submitText(insert + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(insert, result.get().get(0));

        // double quoted identifiers with embedded semicolons
        // are yet not handled correctly---this test will fail
        // works from ENG-12846 multi stmt sp - changed parser
         result = cmd.openQuery();
         insert = "insert into hats (\"fo;o\", bar) " +
             "( ( ( (((( (select goat, chicken from hats))))))))";
         cmd.submitText(insert + ";\n");
         cmd.waitOnResult();
         System.out.println("RESULT: " + result.get());
         assertEquals(1, result.get().size());
         assertEquals(insert, result.get().get(0));
    }

    /**
     * This test basically copies from INSERT INTO...SELECT test above.
     * @throws Exception
     */
    public void testUpsertIntoSelect() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String upsert = "upsert into hats (foo, bar) select goat, chicken from hats";
        cmd.submitText(upsert + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(upsert, result.get().get(0));

        // Test double-quoted identifiers with embedded parentheses
        // and escaped double quotes.
        result = cmd.openQuery();
        upsert = "upsert into\"hats\" (\"foo\", \"b\"\"ar\") " +
                "( ( ( (((( (select goat, chicken from hats))))))))";
        cmd.submitText(upsert + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(upsert, result.get().get(0));
    }

    public void testCreateRole() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String create = "create role goats with ADMINISTRATOR";
        cmd.submitText(create + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(create, result.get().get(0));
    }

    public void testSetDatabaseConfig() throws Exception
    {
        CommandStuff cmd = new CommandStuff();
        Future<List<String>> result = cmd.openQuery();
        String set = "set DR=ACTIVE";
        cmd.submitText(set + ";\n");
        cmd.waitOnResult();
        System.out.println("RESULT: " + result.get());
        assertEquals(1, result.get().size());
        assertEquals(set, result.get().get(0));
    }
}
