/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package schemachange;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

public class SchemaChangeClient {

    static Client client = null;
    static AtomicLong nextKeyToInsert = new AtomicLong(0);
    static AtomicLong maxInsertedKey = new AtomicLong(0);
    static SchemaChangeConfig config = null;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class SchemaChangeConfig extends CLIConfig {
        @Option(desc = "Target RSS per server in MB.")
        long targetrssmb = 1024 * 4;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Path to the starting catalog.")
        String pathtocat1 = "schemachange.jar";

        @Option(desc = "Path to the starting catalog.")
        String pathtocat2 = "schemachange-withindex.jar";

        @Option(desc = "Path to the deployment.")
        String pathtodeployment = "deployment.xml";

        @Override
        public void validate() {
            if (targetrssmb < 512) exitWithMessageAndUsage("targetrssmb must be >= 512");
        }
    }

    /**
     * @return The max RSS across all servers.
     * @throws Exception
     */
    static long getRss() throws Exception {
        ClientResponse cr = client.callProcedure("@Statistics", "MEMORY", 0);
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        assert(cr.getResults().length == 1);
        VoltTable memoryStats = cr.getResults()[0];
        long maxRss = 0;
        while (memoryStats.advanceRow()) {
            long rss = memoryStats.getLong("RSS");
            if (rss > maxRss) {
                maxRss = rss;
            }
        }
        return maxRss;
    }

    /*static class ChurnThread extends Thread {

        AtomicBoolean shouldGo = new AtomicBoolean(true);

        public void shutdown() {
            shouldGo.set(false);
        }

        @Override
        public void run() {
            while (shouldGo.get()) {

            }
        }
    }*/

    static String randomString(int len) {
        Random r = new Random();
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) (r.nextInt(25) + 'a');
        }
        return new String(chars);
    }

    public static class GrowThread extends Thread {

        AtomicBoolean shouldGo = new AtomicBoolean(true);

        public void shutdown() {
            shouldGo.set(false);
        }

        @Override
        public void run() {
            while (shouldGo.get()) {
                long nextId = nextKeyToInsert.getAndIncrement();
                ClientResponse cr = null;
                try {
                    cr = client.callProcedure("BIG_TABLE.insert", nextId, randomString(60), randomString(60), "a", "a", "a", "a", "a", "a");
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                assert(cr.getStatus() == ClientResponse.SUCCESS);

                long maxDone = maxInsertedKey.get();
                while (maxDone < nextId) {
                    maxInsertedKey.compareAndSet(maxDone, nextId);
                    maxDone = maxInsertedKey.get();
                }
            }
        }
    }

    private static String readFileAsString(String filePath) throws java.io.IOException{
        StringBuilder fileData = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line = null;
        while((line = reader.readLine()) != null){
            fileData.append(line);
        }
        reader.close();
        return fileData.toString();
    }

    private static byte[] readFileAsBytes(String filePath) throws java.io.IOException{
        File inputFile = new File(filePath);
        int length = (int) inputFile.length();
        byte[] buf = new byte[length];
        InputStream is = new FileInputStream(inputFile);
        for (int i = 0; i < length; i++) {
            int next = is.read();
            assert(next != -1);
            buf[i] = (byte) next;
        }
        is.close();
        return buf;
    }

    public static void main(String[] args) throws Exception {
        config = new SchemaChangeConfig();
        config.parse("SchemaChangeClient", args);

        String deployment = readFileAsString(config.pathtodeployment);
        byte[] cat1 = readFileAsBytes(config.pathtocat1);
        byte[] cat2 = readFileAsBytes(config.pathtocat2);

        ClientConfig clientConfig = new ClientConfig();
        client = ClientFactory.createClient(clientConfig);
        String[] servers = config.servers.split(",");
        for (String server : servers) {
            server = server.trim();
            client.createConnection(server);
        }

        ClientResponse cr;

        //
        // ADD LOTS OF DATA
        //

        int growerCount = 20;
        GrowThread[] growers = new GrowThread[growerCount];
        for (int i = 0; i < growerCount; i++) {
            growers[i] = new GrowThread();
            growers[i].start();
        }

        // while rss in kb < target in kb
        while (getRss() < (config.targetrssmb * 1024)) {
            Thread.sleep(500);
        }

        for (int i = 0; i < growerCount; i++) {
            growers[i].shutdown();
        }
        for (int i = 0; i < growerCount; i++) {
            growers[i].join();
        }

        //
        // Add and remove the index
        //
        long start = System.currentTimeMillis();

        cr = client.callProcedure("@UpdateApplicationCatalog", cat2, deployment);
        assert(cr.getStatus() == ClientResponse.SUCCESS);

        long now1 = System.currentTimeMillis();
        System.out.printf("Took %f seconds to add the index\n", (now1 - start) / 1000.0);

        Thread.sleep(30 * 1000);

        now1 = System.currentTimeMillis();

        cr = client.callProcedure("@UpdateApplicationCatalog", cat1, deployment);
        assert(cr.getStatus() == ClientResponse.SUCCESS);

        long now2 = System.currentTimeMillis();
        System.out.printf("Took %f seconds to drop the index\n", (now2 - now1) / 1000.0);

        client.close();
    }
}
