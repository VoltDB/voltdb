/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class TestSqlCmdErrorHandling extends TestCase {

    private static final String m_lastError = "ThisIsObviouslyNotAnAdHocSQLCommand;\n";

    private LocalCluster m_cluster;
    private Client m_client;
    private boolean m_verboseForDebug = false;

    private String m_serversString = "localhost";
    private String m_portString = "21212";
    private String m_addressString;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
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

        startServer(simpleSchema);

        m_client = ClientFactory.createClient();

        m_addressString = m_cluster.getListenerAddresses().get(0);
        String[] split =  m_addressString.split(":");
        m_serversString = split[0];
        if (split.length > 1) {
            m_portString = split[1];
        }
        else {
            m_portString = "21212";
        }
        m_client.createConnection(m_addressString);

        // Execute the constrained write to end all constrained writes.
        // This poisons all future executions of the badWriteCommand() query.
        ClientResponse response = m_client.callProcedure("@AdHoc", badWriteCommand());
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        VoltTable[] results = response.getResults();
        assertEquals(1, results.length);
        VoltTable result = results[0];
        assertEquals(1, result.asScalarLong());

        assertEquals("sqlcmd dry run failed -- maybe some sqlcmd component (the voltdb jar file?) needs to be rebuilt.",
                0, callSQLcmd(true, /**/"\n"));//";\n"));

        assertEquals("sqlcmd --stop-on-error=false dry run failed.",
                0, callSQLcmd(false, /**/"\n"));//";\n"));

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

    /**
     * @param simpleSchema
     * @throws IOException
     * @throws InterruptedException
     */
    private void startServer(String simpleSchema) throws IOException,
            InterruptedException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.setUseDDLSchema(false);
        m_cluster = new LocalCluster("TestSqlCmdErrorHandling" + ".jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(m_cluster.compile(builder));
        m_cluster.setHasLocalServer(false);
        m_cluster.startUp();
        Thread.sleep(1000);
    }

    @Override
    protected void tearDown() throws Exception
    {
        m_client.close();
        m_cluster.shutDown();
        super.tearDown();
    }

    public String writeCommand(int id)
    {
        return "insert into intkv (key, myinteger) values(" + id + ", " + id + ");\n";
    }

    private static String badWriteCommand()
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
        File created = File.createTempFile("sqlcmdInput", ".txt");
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

    private int callSQLcmd(boolean stopOnError, String inputText) throws Exception
    {
        final String commandPath = "bin/sqlcmd";
        final long timeout = 60000; // 60,000 millis -- give up after 1 minute of trying.

        File f = new File("ddl.sql");
        f.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(inputText.getBytes());
        fos.close();

        File out = File.createTempFile("testsqlcmdout", ".log");
        out.deleteOnExit();

        File error = File.createTempFile("testsqlcmderr", ".log");
        error.deleteOnExit();

        ProcessBuilder pb =
//                // Enable elapsed time reports to stderr.
                new ProcessBuilder(commandPath, "--debugdelay=,,",
//                // Use up all alloted time on the first error to selectively exercise timeout diagnostics."
//                new ProcessBuilder(commandPath, "--debugdelay=,,10000",
//                new ProcessBuilder(commandPath,
                        "--stop-on-error=" + (stopOnError ? "true" : "false"),
                        "--servers=" + m_serversString,
                        "--port=" + m_portString);
        pb.redirectInput(f);
        pb.redirectOutput(out);
        pb.redirectError(error);
        Process process = pb.start();

        // Set timeout to 1 minute
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

                    //*/enable for debug*/ if (pollcount % 10 == 0) {
                    //*/enable for debug*/     dumpProcessTree();
                    //*/enable for debug*/ }
                }
                if (m_verboseForDebug && exitValue != 0) {
                    System.err.println("Standard input for failed " + commandPath + ":");
                    streamFileToErr(f);
                    System.err.println("*****");
                    System.err.println("Standard output from failed " + commandPath + ":");
                    streamFileToErr(out);
                    System.err.println("*****");
                    System.err.println("Error output from failed " + commandPath + ":");
                    streamFileToErr(error);
                    System.err.println("*****");
                    System.err.flush();
                }
                f.delete();
                out.delete();
                error.delete();
                return exitValue;
            }
            catch (Exception e) {
                elapsedtime = System.currentTimeMillis() - starttime;
                ++pollcount;
                System.err.println("External process (" + commandPath + ") has not yet exited after " + elapsedtime + "ms");
            }
        } while (elapsedtime < timeout);

        process.destroy();

        System.err.println("Standard input for timed out " + commandPath + ":");
        streamFileToErr(f);
        f.delete();
        System.err.println("*****");
        System.err.println("Standard output from timed out " + commandPath + ":");
        streamFileToErr(out);
        out.delete();
        System.err.println("*****");
        System.err.println("Error output from timed out " + commandPath + ":");
        streamFileToErr(error);
        error.delete();
        System.err.println("*****");
        dumpProcessTree();
        System.err.println("*****");
        fail("External process (" + commandPath + ") timed out after " + elapsedtime + "ms on input:\n" + inputText);
        return 0;
    }

    /**
     * @param f
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static void streamFileToErr(File f) throws IOException {
        try {
            byte[] transfer = new byte[1000];
            int asRead = -1;
            FileInputStream cmdIn = new FileInputStream(f);
            while ((asRead = cmdIn.read(transfer)) != -1) {
                System.err.write(transfer, 0, asRead);
            }
            cmdIn.close();
        }
        catch (FileNotFoundException fnfe) {
            System.err.println("ERROR: TestSqlCmdErrorHandling could not find file " + f.getPath());
        } finally {
            System.err.flush();
        }
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     * @throws FileNotFoundException
     */
    private static void dumpProcessTree() throws IOException, InterruptedException, FileNotFoundException {
        File psout = new File("psout.log");
        File pserror = new File("pserr.log");
        ProcessBuilder pspb = new ProcessBuilder("ps", "auxfww");
        pspb.redirectOutput(psout);
        pspb.redirectError(pserror);
        Process psprocess = pspb.start();

        long psstarttime = System.currentTimeMillis();
        Thread.sleep(1000);
        try {
            psprocess.exitValue();
        }
        catch (Exception e) {
            long pselapsed = System.currentTimeMillis() - psstarttime;
            System.err.println("External process (ps) has not yet exited after " + pselapsed + "ms");
        }

        FileInputStream psOuts = new FileInputStream(psout);
        byte[] pstransfer = new byte[1000];
        while (psOuts.read(pstransfer) != -1) {
            System.err.write(pstransfer);
        }
        psOuts.close();
    }

    public void test10Error() throws Exception
    {
        System.out.println("Starting test10Error");
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, m_lastError));
    }

    public void notest20ErrorThenWrite() throws Exception
    {
        System.out.println("Starting test20ErrorThenWrite");
        int id = 20;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void notest30ErrorThenWriteThenError() throws Exception
    {
        System.out.println("Starting test30ErrorThenWriteThenError");
        int id = 30;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id) + m_lastError;
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void notest40BadWrite() throws Exception
    {
        System.out.println("Starting test40BadWrite");
        String inputText = badWriteCommand();
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
    }

    public void notest50BadWriteThenWrite() throws Exception
    {
        System.out.println("Starting test50BadWriteThenWrite");
        int id = 50;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badWriteCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void notest60BadFileThenWrite() throws Exception
    {
        System.out.println("Starting test60BadFileThenWrite");
        int id = 60;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badFileCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(false, inputText));
        assertTrue("skipped a post-error write", checkIfWritten(id));
    }

    public void notest70BadNestedFileWithWriteThenWrite() throws Exception
    {
        System.out.println("Starting test70BadNestedFileWithWriteThenWrite");
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

    public void notest11Error() throws Exception
    {
        System.out.println("Starting test11Error");
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, m_lastError));
    }

    public void notest21ErrorThenStopBeforeWrite() throws Exception
    {
        System.out.println("Starting test21ErrorThenStopBeforeWrite");
        int id = 21;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void notest31ErrorThenStopBeforeWriteOrError() throws Exception
    {
        System.out.println("Starting test31ErrorThenStopBeforeWriteOrError");
        int id = 31;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = m_lastError + writeCommand(id) + m_lastError;
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void notest41BadWrite() throws Exception
    {
        System.out.println("Starting test41BadWrite");
        String inputText = badWriteCommand();
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
    }

    public void notest51BadWriteThenStopBeforeWrite() throws Exception
    {
        System.out.println("Starting test51BadWriteThenStopBeforeWrite");
        int id = 51;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badWriteCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void notest61BadFileStoppedBeforeWrite() throws Exception
    {
        System.out.println("Starting test61BadFileStoppedBeforeWrite");
        int id = 61;
        assertFalse("pre-condition violated", checkIfWritten(id));
        String inputText = badFileCommand() + writeCommand(id);
        assertEquals("sqlcmd did not fail as expected", 255, callSQLcmd(true, inputText));
        assertFalse("did a post-error write", checkIfWritten(id));
    }

    public void notest71BadNestedFileStoppedBeforeWrites() throws Exception
    {
        System.out.println("Starting test71BadNestedFileStoppedBeforeWrites");
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
    public void notest101BadExecsThenStopBeforeWrite() throws Exception
    {
        System.out.println("Starting test101BadExecsThenStopBeforeWrite");
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

    public void notest125ExecWithNulls() throws Exception
    {
        System.out.println("Starting test125ExecWithNulls");
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
}
