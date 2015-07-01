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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.TestCase;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.voltdb.ParameterSet;
import org.voltdb.ServerThread;
import org.voltdb.TestJSONInterface;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class HTTPDBenchmark extends TestCase {

    public static class JettyHandler extends AbstractHandler {

        final int m_delay;
        final int m_responseSize;
        final String m_response;

        public JettyHandler(int delay, int responseSize) throws IOException {
            m_delay = delay;
            m_responseSize = responseSize;
            String responseTemp = "";
            for (int i = 0; i < responseSize; i++)
                responseTemp += "X";
            m_response = responseTemp;
        }

        @Override
        public void handle(String target,
                org.eclipse.jetty.server.Request baseRequest,
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException, ServletException {

            if (m_delay > 0)
                try {
                    Thread.sleep(m_delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new ServletException("Unable to sleep()");
                }
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            response.getWriter().print(m_response);
        }

    }

    public static String callHTTP(int port) throws Exception {
        String urlStr = String.format("http://localhost:%d/", port);
        URL url = new URL(urlStr);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        BufferedReader in = null;
        if(conn.getInputStream()!=null){
            in = new BufferedReader(new InputStreamReader(
                                    conn.getInputStream(), "UTF-8"));
        }
        if(in==null) {
            throw new Exception("Unable to read response from server");
        }

        StringBuffer decodedString = new StringBuffer();
        String line;
        while ((line = in.readLine()) != null) {
            decodedString.append(line);
        }
        in.close();
        // get result code
        int responseCode = conn.getResponseCode();

        String response = decodedString.toString();

        assert(200 == responseCode);
        return response;
    }

    static class HTTPClient extends Thread {
        final int m_iterations;
        final int m_port;

        public HTTPClient(int iterations, int port) {
            m_iterations = iterations;
            m_port = port;
        }

        @Override
        public void run() {
            for (int i = 0; i < m_iterations; i++) {
                try {
                    callHTTP(m_port);
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }

    void runJettyBenchmark(int port, int iterations, int clientCount, int delay, int responseSize) throws Exception {
        System.gc();
        Server s = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(port);
        s.addConnector(connector);

        s.setHandler(new JettyHandler(delay, responseSize));
        s.start();

        HTTPClient[] clients = new HTTPClient[clientCount];
        for (int i = 0; i < clientCount; i++) {
            clients[i] = new HTTPClient(iterations, port);
        }

        long start = System.nanoTime();
        for (int i = 0; i < clientCount; i++) {
            clients[i].start();
        }
        for (int i = 0; i < clientCount; i++) {
            clients[i].join();
        }
        long finish = System.nanoTime();

        double seconds = (finish - start) / (1000d * 1000d * 1000d);
        double rate = (iterations * clientCount) / seconds;
        System.out.printf("Simple bench did %.2f iterations / sec.\n", rate);

        s.stop();
        s.join();
        s = null;
    }

    public void testSimple() throws Exception {
        final int ITERATIONS = 1000;

        // benchmark trivial case
        //runBenchmark(8095, ITERATIONS, 10, 0, 100);
        //runBenchmark(8095, ITERATIONS, 10, 5, 100);

        //runNanoBenchmark(8095, ITERATIONS, 20, 0, 1000);
        runJettyBenchmark(8095, ITERATIONS, 20, 10, 1000);
    }

    static AtomicLong threadsOutstanding = new AtomicLong(0);

    /*public void testThreadCreation() {
        final int ITERATIONS = 1000;
        final int OUTSTANDING = 100;

        long start = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            while (threadsOutstanding.get() >= OUTSTANDING)
                Thread.yield();
            new dumbThread().start();
        }
        while (threadsOutstanding.get() > 0)
            Thread.yield();

        long finish = System.nanoTime();

        double seconds = (finish - start) / (1000d * 1000d * 1000d);
        double rate = ITERATIONS / seconds;
        System.out.printf("Simple bench did %.2f iterations / sec.\n", rate);
    }*/

    class dumbThread extends Thread {
        @Override
        public void run() {
            threadsOutstanding.incrementAndGet();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            threadsOutstanding.decrementAndGet();
        }
    }









    ServerThread startup() throws Exception {
        String simpleSchema =
            "create table dummy (" +
            "sval1 varchar(100) not null, " +
            "sval2 varchar(100) default 'foo', " +
            "sval3 varchar(100) default 'bar', " +
            "PRIMARY KEY(sval1));";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("dummy", "sval1");
        builder.addStmtProcedure("Insert", "insert into dummy values (?,?,?);");
        builder.addStmtProcedure("Select", "select * from dummy;");
        builder.setHTTPDPort(8095);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("jsonperf.jar"), 1, 1, 0);
        assert(success);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("jsonperf.jar");
        config.m_pathToDeployment = builder.getPathToDeployment();
        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponse response1;
        response1 = client.callProcedure("Insert", "FOO", "BAR", "BOO");
        assert(response1.getStatus() == ClientResponse.SUCCESS);

        return server;
    }

    class JSONClient extends Thread {
        final ParameterSet pset = ParameterSet.emptyParameterSet();
        final int m_iterations;
        public long totalExecTime = 0;
        final int m_id;

        public JSONClient(int clientId, int iterations) {
            m_id = clientId;
            m_iterations = iterations;
        }

        @Override
        public void run() {
            for (int i = 0; i < m_iterations; i++) {
                try {
                    long start = System.nanoTime();
                    /*String jsonResponse =*/ TestJSONInterface.callProcOverJSON("Select", pset, null, null, false);
                    long stop = System.nanoTime();
                    totalExecTime += stop - start;
                    //System.out.println(jsonResponse);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                if (i % 100 == 0) {
                    System.out.printf("Client %03d has done %7d/%7d and is %2d%% complete.\n", m_id, i, m_iterations, (int) (m_iterations / (i * 100.0)));
                    System.gc();
                }
            }
        }
    }

    public void JSONBench(int clientCount, int iterations) throws Exception {
        ServerThread server = startup();
        Thread.sleep(1000);

        JSONClient[] clients = new JSONClient[clientCount];
        for (int i = 0; i < clientCount; i++)
            clients[i] = new JSONClient(i, iterations);

        long execTime = 0;

        long start = System.nanoTime();
        for (JSONClient client : clients) {
            client.start();
        }
        for (JSONClient client : clients) {
            client.join();
            execTime += client.totalExecTime;
        }

        long finish = System.nanoTime();

        double seconds = (finish - start) / (1000d * 1000d * 1000d);
        double rate = (iterations * clientCount) / seconds;
        double latency = execTime / (double) (iterations * clientCount);
        latency /= 1000d * 1000d;

        System.out.printf("Simple bench did %.2f iterations / sec at %.2f ms latency per txn.\n", rate, latency);

        server.shutdown();
        server.join();
    }

    public void testJSON() {
        try {
            //b.testSimple();
            //b.testThreadCreation();
            JSONBench(2, 20000);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        HTTPDBenchmark b = new HTTPDBenchmark();

        try {
            //b.testSimple();
            //b.testThreadCreation();
            b.JSONBench(4, 8000);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
