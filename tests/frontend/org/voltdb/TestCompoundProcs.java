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

package org.voltdb;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2CallOptions;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestCompoundProcs {

    private static ServerThread serverThread;
    private static Client2 client;
    private static AtomicReference<ClientResponse> lateResponse = new AtomicReference<>(new ClientResponseImpl());

    @Rule
    public final TestName testname = new TestName();

    @BeforeClass
    public static void prologue() throws Exception {
        System.out.println("=-=-=-= Prologue =-=-=-=");
        ServerThread.resetUserTempDir();
        compile();
        start();
        client = ClientFactory.createClient(new Client2Config().lateResponseHandler(TestCompoundProcs::lateResponseHandler));
        client.connectSync("localhost");
        populate();
    }

    @AfterClass
    public static void epilogue() throws Exception {
        System.out.println("=-=-=-= Epilogue =-=-=-=");
        if (client != null) {
            client.close();
            client = null;
        }
        if (serverThread != null) {
            serverThread.shutdown();
            serverThread.join();
            serverThread = null;
        }
    }

    @Before
    public void setup() throws Exception {
        System.out.printf("=-=-=-= Start test %s =-=-=-=\n", testname.getMethodName());
    }

    @After
    public void takedown() throws Exception {
        System.out.printf("=-=-=-=  End test %s =-=-=-=\n", testname.getMethodName());
    }

    // ==== assorted procedure helper routines ====

    private static void lateResponseHandler(ClientResponse resp, String host, int port) {
        lateResponse.set(resp);
    }

    private static VoltTable[] result(String s) {
        VoltTable vt = new VoltTable(new VoltTable.ColumnInfo("RESULT", VoltType.STRING));
        vt.addRow(s);
        return new VoltTable[] { vt };
    }

    private static Object checkResult(ClientResponse resp, VoltType type) {
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable[] rez = resp.getResults();
        assertNotNull(rez);
        assertEquals(1, rez.length);
        VoltTable vt = rez[0];
        boolean b = vt.advanceRow();
        assertTrue(b);
        Object o = vt.get(0, type);
        assertNotNull(o);
        return o;
    }

    private static int checkFailed(ClientResponse resp, int expect) {
        int status = resp.getStatus();
        assertEquals(expect, status);
        return status;
    }

    private static String extractResult(ClientResponse resp) {
        return (String)checkResult(resp, VoltType.STRING);
    }

    private static long extractLong(ClientResponse resp) {
        return ((Long)checkResult(resp, VoltType.BIGINT)).longValue();
    }

    // ==== volt procedure definitions ====

    // Transaction that always aborts
    public static class WillFailMP extends VoltProcedure {
        public long run(long delay) {
            System.out.printf("MP proc executing 'run' on thread %s\n", Thread.currentThread().getName());
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException ex) {
                // ignored
            }
            throw new VoltAbortException("sic transit gloria mundi");
        }
    }

    // Transaction that will print its thread name and do nothing else
    public static class SayThreadSP extends VoltProcedure {
        public long run(int dummy) {
            System.out.printf("SP proc executing 'run' on thread %s\n", Thread.currentThread().getName());
            return 0;
        }
    }

    // Run two table lookups in parallel, and process results in a third procedure.
    // Implementation is asynchronous.
    public static class MyCompoundProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== MyCompoundProc.run ==");
            printThread("run");
            newStageList(this::getData)
                .then(this::doInsert)
                .then(this::finishUp)
                .build();
            return 0;
        }

        private void getData(ClientResponse[] nil) {
            printThread("getData");
            queueProcedureCall("MySpProc", 1);
            queueProcedureCall("MyOtherSpProc", 2);
        }

        private void doInsert(ClientResponse[] resp) {
            printThread("doInsert");
            String val1 = extractResult(resp[0]), val2 = extractResult(resp[1]);
            int id = (int)(Math.random() * 1000000);
            System.out.printf("== First stage results: '%s' '%s', new id: %d ==\n", val1, val2, id);
            queueProcedureCall("MyLastProc", id, val1, val2);
            queueProcedureCall("TestCompoundProcs$SayThreadSP", 0);
        }

        private void finishUp(ClientResponse[] resp) {
            printThread("finishUp");
            long count = extractLong(resp[0]);
            System.out.printf("== Second stage results: %d rows inserted ==\n", count);
            completeProcedure(123L);
            System.out.println("== MyCompoundProc.run done ==");
        }

        private static void printThread(String func) {
            System.out.printf("NT proc executing '%s' on thread %s\n", func, Thread.currentThread().getName());
        }
    }

    // Like the above but fails halfway through with an unhandled execption
    // Specifically: fails in first stage with arbitrary exception
    public static class IllBehavedProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== IllBehavedProc.run ==");
            newStageList(this::getData)
                .build();
            return 0;
        }

        private void getData(ClientResponse[] nil) {
            queueProcedureCall("MySpProc", 1);
            queueProcedureCall("MyOtherSpProc", 2);
            throw new RuntimeException("If it were done when ’tis done, then ’twere well it were done quickly.");
            // -- Macbeth, Act 1, Scene 7
        }
    }

    // Same as IllBehavedProc but with the proper abort exception
    public static class AbortExceptionProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== AbortExceptionProc.run ==");
            newStageList(this::getData)
                .build();
            return 0;
        }

        private void getData(ClientResponse[] nil) {
            queueProcedureCall("MySpProc", 1);
            queueProcedureCall("MyOtherSpProc", 2);
            throw new CompoundProcAbortException("He has killed me, mother. Run away, I pray you!");
            // Exit LADY MACDUFF, pursued by MURDERERS
            // -- Macbeth, Act 4, Scene 2
        }
    }

    // Completes with outstanding queued proc calls.
    // Logs a warning message on the server, but we do not check that.
    public static class PrematureProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== PrematureProc.run ==");
            newStageList(this::getData)
                .then(this::finishUp)
                .build();
            return 0;
        }

        private void getData(ClientResponse[] nil) {
            queueProcedureCall("MySpProc", 1);
            completeProcedure(999L);
            queueProcedureCall("MyOtherSpProc", 2);
        }

        private void finishUp(ClientResponse[] resp) {
            abortProcedure("should never get here");
        }
    }

    // It is an ancient Procedure,
    // And he stoppeth one of three.
    public static class PartialFailureProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== PartialFailureProc.run ==");
            newStageList(this::getData)
                .then(this::abandon)
                .build();
            return 0;
        }

        private void getData(ClientResponse[] nil) {
            queueProcedureCall("TestCompoundProcs$WillFailMP", 100); // abort after 0.1 secs
            queueProcedureCall("MySpProc", 1);
            queueProcedureCall("MyOtherSpProc", 2);
        }

        private void abandon(ClientResponse[] resp) {
            String val1 = extractResult(resp[1]), val2 = extractResult(resp[2]);
            int status = checkFailed(resp[0], ClientResponse.USER_ABORT);
            System.out.printf("== First stage failed as expected, status %d\n", status);
            abortProcedure("I shot the ALBATROSS");
            System.out.println("== PartialFailureProc.run done ==");
        }
    }

    // Fails to complete
    public static class IncompleteProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== IncompleteProc.run ==");
            newStageList(this::getData)
                .then(this::doInsert)
                .build();
            return 0;
        }

        private void getData(ClientResponse[] nil) {
            queueProcedureCall("MySpProc", 1);
            queueProcedureCall("MyOtherSpProc", 2);
        }

        private void doInsert(ClientResponse[] resp) {
            String val1 = extractResult(resp[0]), val2 = extractResult(resp[1]);
            int id = (int)(Math.random() * 1000000);
            System.out.printf("== First stage results: '%s' '%s', new id: %d ==\n", val1, val2, id);
            queueProcedureCall("MyLastProc", id, val1, val2);
        }
    }

    // Fails in run method
    public static class EarlyFailProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== EarlyFailProc.run ==");
            newStageList(this::getData)
                .build();
            throw new RuntimeException("Procs like us, baby, we were weren't born to run");
        }

        private void getData(ClientResponse[] nil) {
            abortProcedure("Should never get here");
        }
    }

    // Proc with some arguments
    public static class ArgumentativeProc extends VoltCompoundProcedure {
        private String s;
        private int n;

        public long run(String s, int n) {
            System.out.println("== ArgumentativeProc.run ==");
            this.s = s;
            this.n = n;
            newStageList(this::finishUp)
                .build();
            return 0;
        }

        private void finishUp(ClientResponse[] nil) {
            System.out.printf("Arguments: s='%s' n='%d'\n", s, n);
            completeProcedure(0L);
            System.out.println("== ArgumentativeProc.run done ==");
        }
    }

    // Tests changing the stage list on-the-fly
    // which is not allowed
    public static class SwitchProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== SwitchProc.run ==");
            newStageList(this::firstThing)
                .then(this::lastThing)
                .build();
            return 0;
        }

        private void firstThing(ClientResponse[] nil) {
            queueProcedureCall("MySpProc", 1);  // this is never executed
            newStageList(this::newLastThing)
                .build(); // fails
        }

        private void lastThing(ClientResponse[] resp) {
            abortProcedure("Should never get here");
        }

        private void newLastThing(ClientResponse[] resp) {
            abortProcedure("Should never get here either");
       }
    }

    // Tests too many calls
    public static class ExcessiveProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== ExcessiveProc.run ==");
            newStageList(this::oneThing)
                .then(this::lastThing)
                .build();
            return 0;
        }

        private void oneThing(ClientResponse[] nil) {
            System.out.println("10 queued calls should be ok");
            for (int i=0; i<10; i++)
                queueProcedureCall("MySpProc", 1);
            System.out.println("The 11th call should fail");
            queueProcedureCall("MyOtherSpProc", 2);
            // above causes NT proc failure
        }

        private void lastThing(ClientResponse[] resp) {
            abortProcedure("Should never get here");
        }
    }

    // Compound proc can't call compound proc
    public static class NastyNestyProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== NastyNestyProc.run ==");
            newStageList(this::oneThing)
                .then(this::lastThing)
                .build();
            return 0;
        }

        private void oneThing(ClientResponse[] nil) {
            queueProcedureCall("TestCompoundProcs$MyCompoundProc");
            // above causes NT proc failure
        }

        private void lastThing(ClientResponse[] resp) {
            abortProcedure("Should never get here");
        }
    }

    // Procedure taking a 'long' time of 2 seconds
    public static class LongProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== LongProc.run ==");
            printThread("run");
            newStageList(this::getData)
                .then(this::finishUp)
                .build();
            return 0;
        }

        private void getData(ClientResponse[] nil) {
            printThread("getData");
            queueProcedureCall("MySpProc", 1);
            queueProcedureCall("MyOtherSpProc", 2);
        }

        private void finishUp(ClientResponse[] resp) {
            printThread("finishUp");
            System.out.printf("== Think for 2 seconds ==\n");

            try { Thread.sleep(2000); } catch (Exception e) {}

            completeProcedure(123L);
            System.out.println("== LongProc.run done ==");
        }

        private static void printThread(String func) {
            System.out.printf("NT proc executing '%s' on thread %s\n", func, Thread.currentThread().getName());
        }
    }

    // Sets application status
    public static class AppStatusProc extends VoltCompoundProcedure {

        public long run() {
            System.out.println("== AppStatusProc.run ==");
            newStageList(this::failFast)
                .build();
            return 0;
        }

        private void failFast(ClientResponse[] nil) {
            setAppStatusCode((byte)53);
            setAppStatusString("And my poor fool is hang'd!");
            abortProcedure("He dies");
        }
    }

    // ==== initial schema ===

    final private static String SCHEMA =
        "create table dummy (intval integer not null, primary key(intval));\n" +
        "partition table dummy on column intval;\n" +
        "" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$MyCompoundProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$IllBehavedProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$AbortExceptionProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$PrematureProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$PartialFailureProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$IncompleteProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$EarlyFailProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$ArgumentativeProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$SwitchProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$ExcessiveProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$NastyNestyProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$LongProc;\n" +
        "create compound procedure from class org.voltdb.TestCompoundProcs$AppStatusProc;\n" +
        "create procedure from class org.voltdb.TestCompoundProcs$WillFailMP;\n" +
        "create procedure partition on table dummy column intval from class org.voltdb.TestCompoundProcs$SayThreadSP;\n" +
        "" +
        "create table foo (intval integer not null, strval varchar(20), primary key(intval));\n" +
        "partition table foo on column intval;\n" +
        "create procedure MySpProc partition on table foo column intval as select strval from foo where intval = ?;\n" +
        "" +
        "create table bar (intval integer not null, strval varchar(20), primary key(intval));\n" +
        "partition table bar on column intval;\n" +
        "create procedure MyOtherSpProc partition on table bar column intval as select strval from bar where intval = ?;\n" +
        "" +
        "create table mumble (intval integer not null, strval1 varchar(20), strval2 varchar(20), primary key(intval));\n" +
        "partition table mumble on column intval;\n" +
        "create procedure MyLastProc partition on table mumble column intval as insert into mumble values (?, ?, ?);\n";

    final private static String POPULATE =
        "insert into foo values (1, 'something');\n" +
        "insert into foo values (3, 'another thing');\n" +
        "insert into bar values (2, 'something else');\n";

    private static void compile() throws Exception {
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(SCHEMA);
        assertTrue("bad schema", pb.compile(Configuration.getPathToCatalogForTest("more-nt.jar"), 5, 0));
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("more-nt.xml"));
    }

    private static void start() throws Exception {
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("more-nt.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("more-nt.xml");
        serverThread = new ServerThread(config);
        serverThread.start();
        serverThread.waitForInitialization();
    }

    private static void populate() throws Exception {
        System.out.println("populate tables");
        ClientResponse response = client.callProcedureSync("@AdHoc", POPULATE);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    // ==== the actual tests follow ===

    private ClientResponse execTest(String proc, int expect, Object... args) throws Exception {
        return execTest(null, proc, expect, args);
    }

    private ClientResponse execTest(Client2CallOptions opts, String proc, int expect, Object... args) throws Exception {
        ClientResponse response = null;
        try {
            response = client.callProcedureAsync(opts, "TestCompoundProcs$" + proc, args).get();
        }
        catch (Exception ex) {
            System.out.printf("Client exception: %s\n", ex);
            throw ex;
        }
        int status = response.getStatus();
        String mess = response.getStatusString();
        if (mess == null)
            System.out.printf("status %d\n", status);
        else
            System.out.printf("status %d, message \\\\%s\\\\\n", status, mess);
        assertEquals(expect, status);
        return response;
    }

    @Test
    public void testAACompoundProc() throws Exception {
        execTest("MyCompoundProc", ClientResponse.SUCCESS);
    }

    @Test
    public void testIllBehaved() throws Exception {
        execTest("IllBehavedProc", ClientResponse.UNEXPECTED_FAILURE);
    }

    @Test
    public void testAbortException() throws Exception {
        execTest("AbortExceptionProc", ClientResponse.COMPOUND_PROC_USER_ABORT);
    }

    @Test
    public void testPrematureCompletion() throws Exception {
        execTest("PrematureProc", ClientResponse.SUCCESS);
    }

    @Test
    public void testPartialFailure() throws Exception {
        execTest("PartialFailureProc", ClientResponse.COMPOUND_PROC_USER_ABORT);
    }

    @Test
    public void testIncomplete() throws Exception {
        execTest("IncompleteProc", ClientResponse.UNEXPECTED_FAILURE);
    }

    @Test
    public void testEarlyFail() throws Exception {
        execTest("EarlyFailProc", ClientResponse.UNEXPECTED_FAILURE);
    }

    @Test
    public void testNoSuchProc() throws Exception {
        execTest("BananaProc", ClientResponse.UNEXPECTED_FAILURE);
    }

    @Test
    public void testBadArgCount() throws Exception {
        execTest("ArgumentativeProc", ClientResponse.GRACEFUL_FAILURE, "arg1");
    }

    @Test
    public void testBadArgType() throws Exception {
        execTest("ArgumentativeProc", ClientResponse.GRACEFUL_FAILURE, "arg1", "arg2");
    }

    @Test
    public void testSwitchStages() throws Exception {
        execTest("SwitchProc", ClientResponse.UNEXPECTED_FAILURE);
    }

    @Test
    public void testExcessiveCalls() throws Exception {
        execTest("ExcessiveProc", ClientResponse.UNEXPECTED_FAILURE);
    }

    @Test
    public void testNestedCompoundCall() throws Exception {
        execTest("NastyNestyProc", ClientResponse.UNEXPECTED_FAILURE);
    }

    @Test
    public void testAppStatus() throws Exception {
        ClientResponse resp = execTest("AppStatusProc", ClientResponse.COMPOUND_PROC_USER_ABORT);
        int code = resp.getAppStatus();
        String text = resp.getAppStatusString();
        System.out.printf("app status %d, message \\\\%s\\\\\n", code, text);
        assertEquals("app status code not set", 53, code);
        assertEquals("app status string not set", "And my poor fool is hang'd!", text);
    }

    @Test
    public void testLongProc() throws Exception {
        Awaitility.setDefaultPollInterval(Durations.ONE_MILLISECOND);
        Awaitility.setDefaultTimeout(Duration.ofSeconds(3));

        // Test with default timeout succeeds
        execTest("LongProc", ClientResponse.SUCCESS);

        // Test with non-default timeout fails with client response timeout
        Client2CallOptions opts = new Client2CallOptions()
                .clientTimeout(1, TimeUnit.SECONDS);

        ClientResponse response = null;
        try {
            response = client.callProcedureAsync(opts, "TestCompoundProcs$LongProc").get();
        }
        catch (Exception ex) {
            System.out.printf("Client exception: %s\n", ex);
            throw ex;
        }
        int status = response.getStatus();
        if (status != ClientResponse.COMPOUND_PROC_TIMEOUT) {

            // The client must have timed out first
            assertEquals(ClientResponse.CLIENT_RESPONSE_TIMEOUT, status);

            // Verify we also get a late response with the compound procedure timeout
            await().until(() -> lateResponse.get().getStatus() == ClientResponse.COMPOUND_PROC_TIMEOUT);
        }
    }

    @Test
    public void testZZCompoundProc() throws Exception {
        execTest("MyCompoundProc", ClientResponse.SUCCESS);
    }
}
