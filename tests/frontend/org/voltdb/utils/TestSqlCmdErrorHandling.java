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

/*
 * This test is used for testing round trip DDL through Adhoc and SQLcmd.
 * We first build a catalog and pull the canonical DDL from it.
 * Then we feed this DDL to a bare server through Adhoc/SQLcmd,
 * pull the canonical DDL again, and check whether it remains the same.
 */

package org.voltdb.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSqlCmdErrorHandling extends TestCase {

    private final String m_lastError = "ThisIsObviouslyNotAnAdHocSQLCommand;\n";

    private ServerThread m_server;
    private Client m_client;

    @Override
    public void setUp() throws Exception
    {
        String[] mytype = new String[] { "integer", "varbinary", "decimal", "float" };
        String simpleSchema =
                "create table intkv (" +
                "  key integer, " +
                "  myinteger integer default 0, " +
                "  myvarbinary varbinary default 'ff', " +
                "  mydecimal decimal default 10.10, " +
                "  myfloat float default 9.9, " +
                "  PRIMARY KEY(key) );" +
                "\n" +
                "";

        // Define procs that to complain when sqlcmd passes them garbage parameters.
        for (String type : mytype) {
            simpleSchema += "create procedure myfussy_" + type + "_proc as" +
                    " insert into intkv (key, my" + type + ") values (?, ?);" +
                    "\n";
        }

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.setUseDDLSchema(false);
        String catalogPath = Configuration.getPathToCatalogForTest("sqlcmderror.jar");
        assertTrue(builder.compile(catalogPath, 1, 1, 0));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = catalogPath;
        config.m_pathToDeployment = builder.getPathToDeployment();
        m_server = new ServerThread(config);
        m_server.start();
        m_server.waitForInitialization();

        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost");

        assertEquals("sqlcmd dry run failed -- maybe some sqlcmd component (the voltdb jar file?) needs to be rebuilt.",
                0, callSQLcmd(true, ";\n"));

        assertEquals("sqlcmd --stop-on-error=false dry run failed.",
                0, callSQLcmd(false, ";\n"));

        // Execute the constrained write to end all constrained writes.
        // This poisons all future executions of the badWriteCommand() query.
        ClientResponse response = m_client.callProcedure("@AdHoc", badWriteCommand());
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        VoltTable[] results = response.getResults();
        assertEquals(1, results.length);
        VoltTable result = results[0];
        assertEquals(1, result.asScalarLong());

        // Assert that the procs don't complain when fed good parameters.
        // Keep these dry run key values out of range of the test cases.
        // Also make sure they have an even number of digits so they can be used as hex byte values.
        int goodValue = 1000;
        for (String type : mytype) {
            response = m_client.callProcedure("myfussy_" + type + "_proc", goodValue, "" + goodValue);
            ++goodValue; // keeping keys unique
            assertEquals(ClientResponse.SUCCESS, response.getStatus());
            results = response.getResults();
            assertEquals(1, results.length);
            result = results[0];
            assertEquals(1, result.asScalarLong());
        }
    }

    @Override
    public void tearDown() throws InterruptedException
    {
        m_client.close();
        m_server.shutdown();
        m_server.join();
    }

    public String writeCommand(int id)
    {
        return "insert into intkv (key, myinteger) values(" + id + ", " + id + ");\n";
    }

    public String badWriteCommand()
    {
        return "insert into intkv (key, myinteger) values(0, 0);\n";
    }

    public String badExecCommand(String type, int id, String badValue)
    {
        return "exec myfussy_" + type + "_proc " + id + " '" + badValue + "'\n";
    }

    private static String execWithNullCommand(String type, int id) {
        return "exec myfussy_" + type + "_proc " + id + " null\n";
    }

    public String badFileCommand()
    {
        return "file 'ButThereIsNoSuchFileAsThis'\n";
    }

    private String createFileWithContent(String inputText) throws IOException {
        File created = File.createTempFile("sqlcmdInput", "txt");
        created.deleteOnExit();
        FileOutputStream fostr = new FileOutputStream(created);
        byte[] bytes = inputText.getBytes("UTF-8");
        fostr.write(bytes);
        fostr.close();
        return created.getCanonicalPath();
    }

    public boolean checkIfWritten(int id) throws NoConnectionsException, IOException, ProcCallException
    {
        ClientResponse response = m_client.callProcedure("@AdHoc",
                "select count(*) from intkv where key = " + id);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        VoltTable[] results = response.getResults();
        assertEquals(1, results.length);
        VoltTable result = results[0];
        return 1 == result.asScalarLong();
    }

    @SuppressWarnings("resource")
    private int callSQLcmdBase(boolean stopOnError, String inputText,
            boolean fastModeDDL, String expectedErrorMsg) throws Exception {
        String commandPath = "bin/sqlcmd";

        File f = new File("ddl.sql");
        f.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(inputText.getBytes());
        fos.close();

        File out = new File("out.log");

        File error = new File("error.log");

        ProcessBuilder pb = null;
        if (fastModeDDL) {
            pb = new ProcessBuilder(commandPath, "--ddl-file=" + f.getPath());
        } else {
            pb = new ProcessBuilder(commandPath, "--stop-on-error=" + (stopOnError ? "true" : "false"));
            pb.redirectInput(f);
        }
        pb.redirectOutput(out);
        pb.redirectError(error);
        Process process = pb.start();

        // Set timeout to 1 minute
        final long timeout = 60000; // 60,000 millis -- give up after 1 minute of trying.
        long starttime = System.currentTimeMillis();
        long elapsedtime = 0;
        long pollcount = 1;
        do {
            Thread.sleep(1000);
            try {
                int exitValue = process.exitValue();
                // Only verbosely report the successful exit after verbosely reporting a delay.
                // Frequent false alarms might lead to raising the sleep time.
                if (pollcount > 1) {
                    elapsedtime = System.currentTimeMillis() - starttime;
                    System.err.println("External process (" + commandPath + ") exited after being polled " +
                            pollcount + " times over " + elapsedtime + "ms");
                }
                //*/enable for debug*/ System.err.println(commandPath + " returned " + exitValue);
                //*/enable for debug*/ System.err.println(" in " + (System.currentTimeMillis() - starttime)+ "ms");
                //*/enable for debug*/ System.err.println(" on input:\n" + inputText);

                if (fastModeDDL) {
                    String errorMsg = new Scanner(error).useDelimiter("\\Z").next();
                    if (expectedErrorMsg != null && !expectedErrorMsg.equals("")) {
                        assertTrue(errorMsg.contains(expectedErrorMsg));
                    }
                }

                return exitValue;
            }
            catch (Exception e) {
                elapsedtime = System.currentTimeMillis() - starttime;
                ++pollcount;
                System.err.println("External process (" + commandPath + ") has not yet exited after " + elapsedtime + "ms");
            }
        } while (elapsedtime < timeout);

        System.err.println("Standard output from timed out " + commandPath + ":");
        FileInputStream cmdOut = new FileInputStream(out);
        byte[] transfer = new byte[1000];
        while (cmdOut.read(transfer) != -1) {
            System.err.write(transfer);
        }
        cmdOut.close();
        System.err.println("Error outpout from timed out " + commandPath + ":");
        FileInputStream cmdErr = new FileInputStream(error);
        while (cmdErr.read(transfer) != -1) {
            System.err.write(transfer);
        }
        cmdErr.close();
        fail("External process (" + commandPath + ") timed out after " + elapsedtime + "ms on input:\n" + inputText);
        return -1;
    }

    private int callSQLcmd(boolean stopOnError, String inputText) throws Exception {
        return callSQLcmdBase(stopOnError, inputText, false, "");
    }

    private int callSQLcmdDDLMode(String inputText, String errorMessage) throws Exception {
        return callSQLcmdBase(false, inputText, true, errorMessage);
    }

    public void test10Error() throws Exception
    {
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, m_lastError));
    }

    public void test20ErrorThenWrite() throws Exception
    {
        int id = 20;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void test30ErrorThenWriteThenError() throws Exception
    {
        int id = 30;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id) + m_lastError;
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void test40BadWrite() throws Exception
    {
        String inputText = badWriteCommand();
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
    }

    public void test50BadWriteThenWrite() throws Exception
    {
        int id = 50;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badWriteCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void test60BadFileThenWrite() throws Exception
    {
        int id = 60;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badFileCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void test70BadNestedFileWithWriteThenWrite() throws Exception
    {
        int id = 80;
        assertFalse("pre-condition violated", checkIfWritten(id));
        assertFalse("pre-condition violated", checkIfWritten( -id));
        String inputText = badFileCommand() + writeCommand( -id);
        String filename = createFileWithContent(inputText);
        inputText = "file '" + filename + "';\n" + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a file-scripted post-error write", checkIfWritten( -id));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void test11Error() throws Exception
    {
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, m_lastError));
    }

    public void test21ErrorThenStopBeforeWrite() throws Exception
    {
        int id = 21;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void test31ErrorThenStopBeforeWriteOrError() throws Exception
    {
        int id = 31;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id) + m_lastError;
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void test41BadWrite() throws Exception
    {
        String inputText = badWriteCommand();
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
    }

    public void test51BadWriteThenStopBeforeWrite() throws Exception
    {
        int id = 51;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badWriteCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void test61BadFileStoppedBeforeWrite() throws Exception
    {
        int id = 61;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badFileCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void test71BadNestedFileStoppedBeforeWrites() throws Exception
    {
        int id = 81;
        assertFalse("pre-condition violated", checkIfWritten(id));
        assertFalse("pre-condition violated", checkIfWritten( -id));
        String inputText = badFileCommand() + writeCommand( -id);
        String filename = createFileWithContent(inputText);
        inputText = "file '" + filename + "';\n" + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a file-scripted post-error write", checkIfWritten( -id));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    // The point here is not so much the --stop-on-first-error behavior,
    // but the unified handling of unconvertible exec parameters within sqlcmd.
    //TODO: Unclear at this point is what advantage sqlcmd's custom handling has
    // over an alternative dumbed-down approach that let the server sort out the
    // parameter conversions and validations. Any known cases where sqlcmd does
    // this better or differently than the server would be expected to are
    // good candidates for testing here so that if anyone messes with this code
    // we will at least know it is time to "release note" the change.
    public void test101BadExecsThenStopBeforeWrite() throws Exception
    {
        int id = 101;
        subtestBadExec("integer", id++, "garbage");
        subtestBadExec("integer", id++, "1 and still garbage");
        subtestBadExec("varbinary", id++, "garbage");
        subtestBadExec("varbinary", id++, "1"); // one hex digit -- specialized varbinary poison
        subtestBadExec("decimal", id++, "garbage");
        subtestBadExec("decimal", id++, "1.0 and still garbage");
        subtestBadExec("float", id++, "garbage");
        subtestBadExec("float", id++, "1.0 and still garbage");
    }

    private void subtestBadExec(String type, int id, String badValue) throws Exception
    {
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badExecCommand(type, id, badValue) + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void test125ExecWithNulls() throws Exception
    {
        int id = 125;
        String[] types = new String[] {"integer", "varbinary", "decimal", "float"};
        for (String type : types) {
            subtestExecWithNull(type, id++);
        }
    }

    private void subtestExecWithNull(String type, int id) throws Exception {
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = execWithNullCommand(type, id);
        assertEquals("sqlcmd was expected to succeed, but failed", 0, callSQLcmd(true, inputText));
        assertTrue("did not write row as expected", checkIfWritten(id));
    }

    @SuppressWarnings("resource")
    private int callSQLcmdWithErrors(String optionArg, String errorMessage) throws Exception {
        String commandPath = "bin/sqlcmd";

        File out = new File("out.log");
        File error = new File("error.log");

        ProcessBuilder pb = new ProcessBuilder(commandPath, optionArg);

        pb.redirectOutput(out);
        pb.redirectError(error);
        Process process = pb.start();

        // Only doing cmd line arguments work, SQLCMD process should finish in 1 second.
        final int SLEEP = 200;
        final int TIMES = 50; // 10s before timing out
        for (int i = 0; i < TIMES; i++) {
            Thread.sleep(SLEEP);
            try {
                int exitValue = process.exitValue();

                String message = new Scanner(out).useDelimiter("\\Z").next();
                assertTrue(message.contains(errorMessage));
                return exitValue;
            } catch (Exception e) {
                System.err.println("External process (" + commandPath + ") has not yet exited after " + (i+1) * SLEEP + "ms");
            }
        }

        // sqlcmd process has not finished, time out this tests
        return -1;
    }

    private final String prompts = "sqlcmd did not fail as expected";
    public void testDDLModeBadCommandLineInput() throws Exception {
        String errorMsgPrefix = "Missing input value for ";
        assertEquals(prompts, 255, callSQLcmdWithErrors("--servers=", errorMsgPrefix + "--servers"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--port=", errorMsgPrefix + "--port"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--user=", errorMsgPrefix + "--user"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--password=", errorMsgPrefix + "--password"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--kerberos=", errorMsgPrefix + "--kerberos"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--output-format=", errorMsgPrefix + "--output-format"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--stop-on-error=", errorMsgPrefix + "--stop-on-error"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--ddl-file=", errorMsgPrefix + "--ddl-file"));

        assertEquals(prompts, 255, callSQLcmdWithErrors("--ddl-file= haha.txt", "DDL file not found at path: haha.txt"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--output-format=haha", "Invalid value for --output-format"));
        assertEquals(prompts, 255, callSQLcmdWithErrors("--stop-on-error=haha", "Invalid value for --stop-on-error"));
    }

    public void testDDLModeBadInput() throws Exception
    {
        String inputDDL = "";

        inputDDL = "CREATE TABLE NONSENSE (id INTEGER);\n"
                + "INSERT INTO NONSENSE VALUES_HHH  (1);\n";
        assertEquals(prompts, 255, callSQLcmdDDLMode(inputDDL, "DDL mixed with DML and queries is unsupported."));

        inputDDL = "CREATE TABLE NONSENSE (id INTEGER);\n"
                + "INSERT INTO NONSENSE VALUES (1);\n";
        assertEquals(prompts, 255, callSQLcmdDDLMode(inputDDL, "DDL mixed with DML and queries is unsupported."));
    }
}
