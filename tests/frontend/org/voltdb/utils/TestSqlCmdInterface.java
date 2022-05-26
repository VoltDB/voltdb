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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.junit.Test;
import org.voltdb.parser.SQLLexer;
import org.voltdb.parser.SQLParser;
import org.voltdb.parser.SQLParser.FileInfo;

import com.google_voltpatches.common.base.Joiner;

public class TestSqlCmdInterface
{
    private int ID = -1;
    private final String logFilename = "/tmp/logHelp.txt"; // For ENG-3440

    // 1) To test a single select statement
    @Test
    public void testParseQuery1() {
        String raw = "select * from dummy";
        ID = 1;
        assertThis(raw, 1, ID);
    }

    // 1) To test a single select statement
    @Test
    public void testFormerlyDangerousCompoundStatements2() {
        String raw =
                "create procedure named1         as delete from dummy;" +
                "create procedure named2         as truncate table dummy;" +
                "create procedure named3         as drop table dummy;" +
                "create procedure named4butforgotas drop table dummy;" +
                "create procedure named5         as create table dummy;" +
                "create procedure named6         as upsert into dummy values(1);" +
                "create table garbled10 ( yes integer,                        execute (delete from valid where yes = -1));" +
                "create table garbled11 ( yes integer,                                 delete from valid where yes = -1 );" +
                "";
        ID = 2;
        // Extra whitespace is just for human readability.
        raw = raw.replaceAll("\\s+", " ");
        String expected = raw.replaceAll("\\s*;\\s*", ";");
        assertThis(raw, expected, 8, ID);
    }

     // 3) To test 2 select statements which are separated by one or more semicolons
    //    and zero or more white spaces
    @Test
    public void testParseQuery3() {
        String raw = "   select * from Dummy where id  =    1;;;;    " +
                      "   select * from          dummy2;        ";
        ID = 3;
        String expected = raw.replaceAll("\\s*;+\\s*", ";").trim();
        assertThis(raw, expected, 2, ID);
    }

    // 5) To test 2 select statements in which one of them is incomplete
    @Test
    public void testParseQuery5() {
        String raw = "select * fRom dummy;;;;select *";
        ID = 5;
        String expected = raw.replaceAll("\\s*;+\\s*", ";") + ";";
        assertThis(raw, expected, 2, ID);
    }

    // 6) To test 2 select statements in which one of them is incomplete
    @Test
    public void testParseQuery6() {
        String raw = "     INSERT INTO      Dummy " +
                      "            vALUES (value1, NULL, null, '', ...);";
        ID = 6;
        String expected = raw.replaceAll("\\s*;+\\s*", ";").trim();
        assertThis(raw, expected, 1, ID);
    }

    // 7) To test 2 select statements with union
    //    VoltDB has not supported UNION, yet.
    //    So this is a negative test.
    @Test
    public void testParseQuery7() {
        String raw = "SELECT * FROM table UNION SELECT * FROM table2;";
        String expected = raw;
        ID = 7;
        assertThis(raw, expected, 1, ID);
    }

    // Test cases 8, 9, 10 moved to TestSplitSQLStatements

    // As of today, 07/13/2012, sqlcmd does not support create, yet.
    // Just to check what's got returned.
    // 11) create table xxx (col1_name type(), col2_name type());
    @Test
    public void testParseQuery11() {
        String raw = "  create tAble xxx   (col1_name type(), col2_name type());";
        ID = 11;
        String expected = raw.replaceAll("\\s*;+\\s*", ";").trim();
        assertThis(raw, expected, 1, ID);
    }

    // 13) To test 2 sql statements starting with different key words
    //     which are separated by a semicolon.
    //     This test case is derived from case 12.
    @Test
    public void testParseQuery13() {
        String raw = " select * From dummy;   create tAble xxx  (col1_name type(), col2_name type()) ;  ";
        ID = 2;
        String expected = raw.replaceAll("\\s*;+\\s*", ";").trim();
        assertThis(raw, expected, 2, ID);
    }

    // 14) To test a bogus string containing semicolon(s).
    @Test
    public void testParseQuery14() {
        // SQLCommand.mockVoltDBForTest(new ClientForTest());
        String raw = "   ssldgjdsgjdsjjg dskfkdskeevnskdh   ;   ksjghtrewoito dsfharw  ; ";
        ID = 14;
        String expected = raw;
        // sqlcmd always replace semicolons with ONE space
        expected = expected.replaceAll("\\s*;+\\s*", ";").trim();
        assertThis(raw, expected, 2, ID);
    }

    // 15) select/delete/update/insert
    @Test
    public void testParseQuery15() {
        ID = 15;
        // raw1 contains two select statements separated by a space
        String raw1 = "select * from votes limit 10 ; select count(*) from votes ;";
        String raw2 = "delete from votes where   PHONE_NUMBER = 3082086134      ;";
        // raw3 contains two select statements separated by multiple spaces
        String raw3 = "select count(*) from votes  ; select count(*) from votes;";
        // The combination of raw5 & raw6 is just one sql statement
        String raw4 = "update votes set CONTESTANT_NUMBER = 7 ";
        String raw5 = "where PHONE_NUMBER = 2150002906 ;";
        String raw6 = "insert into votes vAlues (2150000000, 'PA', 6);";
        String raw = raw1 + raw2 + raw3 + raw4 + raw5 + raw6;
        String copy = raw;
        copy = copy.replaceAll("\\s*;+\\s*", ";");
        assertThis(raw, copy, 7, ID);
    }

    // The function 'alter table' is not supported, yet. Therefore, the
    // key word 'alter' is unrecognized. This test is kind of like test
    // case 8. More test cases are derived from this one.
    @Test
    public void testParseQuery16() {
        ID = 16;
        String raw1 = "select * from votes limit 12 ;";
        String raw2 = "delete from votes where PHONE_NUMBER = 3082086134 ;";
        String raw3 = "   alter table xxxx rename to new_tbl_name; ";
        String raw4 = "select cOunt(*) from dummy ;";
        String raw = raw1 + raw2 + raw3 + raw4;
        String expected = raw.replaceAll("\\s*;+\\s*", ";");
        assertThis(raw, expected, 4, ID);
    }

    // Starting to test stored procedures
    @Test
    public void testParseQuery18() {
        ID = 18;
        String raw = "select * from dummy   ;    exec @SystemCatalog   tables;";
        String expected = raw.replaceAll("\\s*;+\\s*", ";");
        assertThis(raw, expected, 2, ID);
    }

    // insert white spaces
    @Test
    public void testParseQuery19() {
        ID = 19;
        String raw = " insert into tablename (col1, col2) values ('   1st 2nd 3rd  ', '   ');";
        String expected = raw.replaceAll("\\s*;+\\s*", ";").trim();
        assertThis(raw, expected, 1, ID);
    }

    // insert NULLs
    @Test
    public void testParseQuery20() {
        ID = 20;
        String raw = " insert into votes (phone-number, state, CONTESTANT_NUMBER) " +
                "values (978-475-      0001, 'MA', null);";
        String expected = raw.replaceAll("\\s*;+\\s*", ";").trim();
        assertThis(raw, expected, 1, ID);
    }

    @Test
    public void testParseQuery21() throws IOException {
        ID = 21;
        SQLCommand.testFrontEndOnly();
        final String fileName = "./tests/frontend/org/voltdb/utils/localQry.txt";
        String fileCmd = "file " + fileName;
        final List<FileInfo> filesInfo = SQLParser.parseFileStatement(null, fileCmd);
        final File sqlFile = filesInfo.get(0).getFile();
        assertTrue(sqlFile.exists());
        File matchFile = new File(fileName);
        assertEquals("Expected equal file objects", matchFile, sqlFile);

        SQLCommand.executeScriptFiles(filesInfo, null, null);

        String raw = SQLCommand.getTestResult();

        int numOfQueries = -1;
        String qryFrmFile = "";
        String contents = null;
        try {
            Scanner scanner = new Scanner(sqlFile);
            contents = scanner.useDelimiter("\\A").next();
            scanner.close();
        }
        catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        // code is updated now so that we do not have a separate statement for block comment
        int blockCommentCount = 0;
        try {
            Pattern regex = Pattern.compile("(?:/\\*.*\\*/)", Pattern.DOTALL | Pattern.MULTILINE);

            Matcher regexMatcher = regex.matcher(contents);
            StringBuffer sb = new StringBuffer();
            while (regexMatcher.find()) {
                regexMatcher.appendReplacement(sb, "");
                ++blockCommentCount;
            }
            // Add the last segment of input to the new String
            regexMatcher.appendTail(sb);
        } catch (PatternSyntaxException ex) {
            // Syntax error in the regular expression
            System.err.println(ex.getDescription());
            System.exit(1);
        }
        // Simplify the script file content to simulate what the SQLCommand
        // frontend will do to it.  This algorithm MAY not be 100% reliable
        // on all input scripts -- input scripts checked in this way may have
        // have to comply with certain formatting conventions.
        Scanner opnScanner = new Scanner(sqlFile);
        // Read each line in the file
        while(opnScanner.hasNext()) {
            String line = opnScanner.nextLine();
            // To filter out sql comments starting with '--'
            // Note that currently, we filter out the comments lines with
            // leading '--'. For instance:
            // 1) --this commenting line will be filtered out
            // and statements starting with C style block comments
            // 2) /* comments will be removed */ select * from t;
            if (line.matches("--.*")) {
                // The value of numOfQueries hides in a special structured comment
                if (line.matches("^--num=\\d+$")) {
                    numOfQueries = Integer.parseInt(line.replaceAll("\\D+", ""));
                }
            }
            else {
                qryFrmFile = qryFrmFile.concat(line).concat(" ");
            }
        }
        opnScanner.close();
        qryFrmFile = qryFrmFile.replaceAll("\\s*;+\\s*", ";");
        assertThis(raw, qryFrmFile, numOfQueries, ID, blockCommentCount);
    }

    // To test parseQueryProcedureCallParameters()
    // To test a valid query: 'select * from dummy' as a proc call.
    @Test
    public void testParseQueryProcedureCallParameters22() {
        ID = 22;
        String query = "select * from dummy";
        assertTrue(SQLParser.parseExecuteCallWithoutParameterTypes(query) == null);
    }

    // To assert the help page printed by SQLCommand.printHelp() is identical to the
    // original static help file 'SQLCommandReadme.txt'. For ENG-3440
    @Test
    public void testPrintHelpMenu26() throws IOException {
        ID = 26;
        String msg = "\nTest ID: " + ID + "\n";
        String err1 = null, err2 = null;

        String orgReadme = "./src/frontend/org/voltdb/utils/" + SQLCommand.getReadme();
        FileOutputStream fos = new FileOutputStream(logFilename);
        SQLCommand.printHelp(fos);
        FileInputStream fstream1 = new FileInputStream(logFilename);
        FileInputStream fstream2 = new FileInputStream(orgReadme);

        DataInputStream in1 = new DataInputStream(fstream1);
        BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
        DataInputStream in2 = new DataInputStream(fstream2);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));

        try {
            String strLine1 = null, strLine2 = null;
            int cnt = 0;
            while ((strLine1 = br1.readLine()) != null && (strLine2 = br2.readLine()) != null) {
                err1 = "Expected Content: #" + strLine1 + "#\n";
                err1 = "  Actual Content: #" + strLine2 + "#\n";
                assertTrue(msg+err1, strLine1.equals(strLine2));
                cnt++;
            }
            err2 = "The value of line count cannot be zero! cnt = " + cnt + "\n";
            assertNotSame(msg+err2, 0, cnt);
        }
        finally {
            // Silence the resource leak warnings.
            br1.close();
            br2.close();
        }
    }

    @Test
    public void testParseCreateView()
    {
        ID = 28;
        String create = "create view foo (bas, as) as select bar, count(*) from foo group by val;";
        assertThis(create, create, 1, ID);
    }

    @Test
    public void testParseCreateStmtProcedure()
    {
        ID = 29;
        String create = "create procedure foo as select * from blat;";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as insert into blat values (?, ?);";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as update into blat values (?, ?);";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as delete into blat values (?, ?);";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as SELECT * FROM table UNION SELECT * FROM table2;";
        assertThis(create, create, 1, ID);
    }

    // test select statement with FROM subquery
    @Test
    public void testParseQuery30() {
        String raw = "SELECT * FROM (SELECT * FROM table2);";
        String expected = raw;
        ID = 30;
        assertThis(raw, expected, 1, ID);
    }

    // test select statement with IN subquery
    @Test
    public void testParseQuery31() {
        String raw = "SELECT * FROM table1 WHERE (A,C) IN ( SELECT A,C FROM table2);";
        String expected = raw;
        ID = 31;
        assertThis(raw, expected, 1, ID);
    }

    // test select statement with EXISTS subquery
    @Test
    public void testParseQuery32() {
        String raw = "SELECT * FROM table1 WHERE EXISTS( SELECT 1FROM table2);";
        String expected = raw;
        ID = 32;
        assertThis(raw, expected, 1, ID);
    }

    @Test
    public void testParseCreateMultiStmtProcedure()
    {
        ID = 33;
        String create = "create procedure foo as begin select * from blat; "
                + "select * from foo; end;";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as begin insert into blat values (?, ?); end;";
        assertThis(create, create, 1, ID);
    }

    private void assertThis(String qryStr, int numOfQry, int testID) {
        List<String> parsed = SQLLexer.splitStatements(qryStr).getCompletelyParsedStmts();
        String msg = "Test ID: " + testID + ". ";
        assertNotNull(msg + "SQLCommand.parseQuery returned a NULL obj!!", parsed);
        assertEquals(msg, numOfQry, parsed.size());
        String parsedString = Joiner.on(";").join(parsed);
        assertTrue(msg, qryStr.equalsIgnoreCase(parsedString));
    }

    private void assertThis(String qryStr, String cleanQryStr, int numOfQry, int testID) {
        assertThis(qryStr, cleanQryStr, numOfQry, testID, 0);
    }

    private void assertThis(String qryStr, String cleanQryStr, int numOfQry, int testID, int blockCommentCount) {
        List<String> parsed = SQLLexer.splitStatements(qryStr).getCompletelyParsedStmts();
        String msg = "\nTest ID: " + testID + ". ";
        String err1 = "\nExpected # of queries: " + numOfQry + "\n";
        err1 += "Actual # of queries: " + parsed.size() + "\n";
        assertEquals(msg+err1, numOfQry, parsed.size());
        String parsedString = Joiner.on(";").join(parsed) + ";";
        String err2 = "\nExpected queries: \n#" + cleanQryStr + "#\n";
        err2 += "Actual queries: \n#" + parsedString + "#\n";

        if (blockCommentCount == 0) {
            // If there is sql comments in block(s), then skip the assertion below
            assertTrue(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
        }
        else {
            assertFalse(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
        }
    }

    @Test
    public void testParseFileBatchDDL()
    {
        ID = 50;
        List<FileInfo> filesInfo = null;

        filesInfo = SQLParser.parseFileStatement(null, "FILE  haha.sql;");
        assertEquals(1, filesInfo.size());
        assertFalse(filesInfo.get(0).isBatch());

        filesInfo = SQLParser.parseFileStatement(null, "FILE -batch heehee.sql hahaa.sql;");
        assertEquals(2, filesInfo.size());
        assertTrue(filesInfo.get(0).isBatch());
        assertTrue(filesInfo.get(1).isBatch());

        // space allowed for file names
        filesInfo = SQLParser.parseFileStatement(null, "FILE 'file 1.sql';");
        assertEquals(1, filesInfo.size());
        assertFalse(filesInfo.get(0).isBatch());
        assertEquals("file 1.sql", filesInfo.get(0).getFile().toString());

        filesInfo = SQLParser.parseFileStatement(null, "FILE 'abc.sql' 'def.sql' 'ghi jkl.sql'");
        assertEquals(3, filesInfo.size());
        assertFalse(filesInfo.get(0).isBatch());
        assertFalse(filesInfo.get(1).isBatch());
        assertFalse(filesInfo.get(2).isBatch());
        assertEquals("abc.sql", filesInfo.get(0).getFile().toString());
        assertEquals("def.sql", filesInfo.get(1).getFile().toString());
        assertEquals("ghi jkl.sql", filesInfo.get(2).getFile().toString());

        // for batch file processing
        filesInfo = SQLParser.parseFileStatement(null, "FILE -batch heehee.sql;");
        assertEquals(1, filesInfo.size());
        assertTrue(filesInfo.get(0).isBatch());

        filesInfo = SQLParser.parseFileStatement(null, "FILE -batch heehee.sql hahaa.sql;");
        assertEquals(2, filesInfo.size());
        assertTrue(filesInfo.get(0).isBatch());
        assertTrue(filesInfo.get(1).isBatch());

        // space allowed for file names in batch
        filesInfo = SQLParser.parseFileStatement(null, "FILE -batch 'file 1.sql';");
        assertEquals(1, filesInfo.size());
        assertTrue(filesInfo.get(0).isBatch());
        assertEquals("file 1.sql", filesInfo.get(0).getFile().toString());

        filesInfo = SQLParser.parseFileStatement(null, "FILE -batch 'file 1.sql' 'file 2.sql' 3.sql");
        assertEquals(3, filesInfo.size());
        assertTrue(filesInfo.get(0).isBatch());
        assertTrue(filesInfo.get(1).isBatch());
        assertTrue(filesInfo.get(2).isBatch());
        assertEquals("file 1.sql", filesInfo.get(0).getFile().toString());
        assertEquals("file 2.sql", filesInfo.get(1).getFile().toString());
        assertEquals("3.sql", filesInfo.get(2).getFile().toString());

        filesInfo = SQLParser.parseFileStatement(null, "FILE -batch 'abc.sql' 'def.sql' 'ghi jkl.sql'");
        assertEquals(3, filesInfo.size());
        assertTrue(filesInfo.get(0).isBatch());
        assertTrue(filesInfo.get(1).isBatch());
        assertTrue(filesInfo.get(2).isBatch());
        assertEquals("abc.sql", filesInfo.get(0).getFile().toString());
        assertEquals("def.sql", filesInfo.get(1).getFile().toString());
        assertEquals("ghi jkl.sql", filesInfo.get(2).getFile().toString());

    }
}
