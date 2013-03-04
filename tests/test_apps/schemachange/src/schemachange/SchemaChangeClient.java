/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.io.FileReader;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.CatalogBuilder;

public class SchemaChangeClient {

    static Client client = null;
    static AtomicLong nextKeyToInsert = new AtomicLong(0);
    static AtomicLong maxInsertedKey = new AtomicLong(0);
    static SchemaChangeConfig config = null;
    static String deploymentString = null;
    static Random rand = new Random(0);

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

        @Option(desc = "Path to the deployment.")
        String pathtodeployment = "deployment.xml";

        @Override
        public void validate() {
            if (targetrssmb < 512) exitWithMessageAndUsage("targetrssmb must be >= 512");
        }
    }

    /**
     * Perform a schema change to a mutated version of the current table (80%) or
     * to a new table entirely (20%, drops and adds the new table).
     */
    static VoltTable catalogChange(VoltTable t1) throws Exception {
        boolean newTable = (t1 == null) || (rand.nextInt(4) == 0); // 20% chance of a new table

        CatalogBuilder builder = new CatalogBuilder();
        VoltTable t2 = null;
        String currentName = t1 == null ? "B" : TableHelper.getTableName(t1);
        String newName = currentName;

        if (newTable) {
            newName = currentName.equals("A") ? "B" : "A";
            t2 = TableHelper.getTotallyRandomTable(newName, rand);
        }
        else {
            t2 = TableHelper.mutateTable(t1, rand);
        }

        System.out.printf("New Schema:\n%s\n", TableHelper.ddlForTable(t2));

        builder.addLiteralSchema(TableHelper.ddlForTable(t2));
        // make tables name A partitioned and tables named B replicated
        if (newName.equalsIgnoreCase("A")) {
            int pkeyIndex = TableHelper.getBigintPrimaryKeyIndexIfExists(t2);
            builder.addPartitionInfo(newName, t2.getColumnName(pkeyIndex));
        }
        byte[] catalogData = builder.compileToBytes();
        assert(catalogData != null);

        long count = tupleCount(t1);
        long start = System.nanoTime();

        if (newTable) {
            System.out.println("Starting catalog update to swap tables.");
        }
        else {
            System.out.println("Starting catalog update to change schema.");
        }

        ClientResponse cr = client.callProcedure("@UpdateApplicationCatalog", catalogData, deploymentString);
        assert(cr.getStatus() == ClientResponse.SUCCESS);

        long end = System.nanoTime();
        double seconds = (end - start) / 1000000000.0;

        if (newTable) {
            System.out.printf("Completed catalog update that swapped tables in %.4f seconds\n",
                    seconds);
        }
        else {
            System.out.printf("Completed catalog update of %d tuples in %.4f seconds (%d tuples/sec)\n",
                    count, seconds, (long)(count / seconds));
        }

        System.out.println("Sleeping for 5s");
        Thread.sleep(5000);

        return t2;
    }

    /**
     * Count the number of tuples in the table.
     */
    static long tupleCount(VoltTable t) throws Exception {
        if (t == null) {
            return 0;
        }
        VoltTable result = client.callProcedure("@AdHoc",
                String.format("select count(*) from %s;", TableHelper.getTableName(t))).getResults()[0];
        return result.asScalarLong();
    }

    /**
     * Find the largest pkey value in the table.
     */
    static long maxId(VoltTable t) throws Exception {
        if (t == null) {
            return 0;
        }
        VoltTable result = client.callProcedure("@AdHoc",
                String.format("select pkey from %s order by pkey desc limit 1;", TableHelper.getTableName(t))).getResults()[0];
        return result.getRowCount() > 0 ? result.asScalarLong() : 0;
    }

    /**
     * Add rows until RSS target met.
     * Delete all odd rows (triggers compaction).
     * Re-add odd rows until RSS target met (makes buffers out of order).
     */
    static void loadTable(VoltTable t) throws Exception {
        System.out.printf("loading table\n");
        long max = maxId(t);
        TableHelper.fillTableWithBigintPkey(t, 1200, client, rand, max + 1, 1);
        TableHelper.deleteOddRows(t, client);
        TableHelper.fillTableWithBigintPkey(t, 1200, client, rand, 1, 2);
    }

    /**
     * Grab some random rows that aren't on the first EE page for the table.
     */
    public static VoltTable sample(VoltTable t) throws Exception {
        VoltTable t2 = t.clone(4096 * 1024);

        ClientResponse cr = client.callProcedure("@AdHoc",
                String.format("select * from %s where pkey >= 100000 order by pkey limit 100;",
                        TableHelper.getTableName(t)));
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = cr.getResults()[0];
        result.resetRowPosition();
        while (result.advanceRow()) {
            t2.add(result);
        }

        return t2;
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

    public static void main(String[] args) throws Exception {
        VoltDB.setDefaultTimezone();

        config = new SchemaChangeConfig();
        config.parse("SchemaChangeClient", args);

        try {
            deploymentString = readFileAsString(config.pathtodeployment);
        }
        catch (Exception e) {
            deploymentString = "<?xml version=\"1.0\"?><deployment>" +
                    "<cluster hostcount=\"1\" sitesperhost=\"1\" kfactor=\"0\" />" +
                    "<httpd enabled=\"true\"><jsonapi enabled=\"true\" /></httpd>" +
                    "<snapshot prefix=\"schemachange\" frequency=\"1s\" retain=\"2\"/></deployment>";
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProcedureCallTimeout(30 * 60 * 1000); // 30 min
        client = ClientFactory.createClient(clientConfig);
        String[] servers = config.servers.split(",");
        for (String server : servers) {
            server = server.trim();
            client.createConnection(server);
        }

        // kick this off with a random schema
        VoltTable t = catalogChange(null);

        for (int i = 0; i < 50; i++) {
            // make sure the table is full and mess around with it
            loadTable(t);
            String tableName = TableHelper.getTableName(t);

            // deterministically sample some rows
            VoltTable preT = sample(t);
            //System.out.printf("First sample:\n%s\n", preT.toFormattedString());

            // move to an entirely new table or migrated schema
            t = catalogChange(t);

            // if the table has been migrated, check the data
            if (TableHelper.getTableName(t).equals(tableName)) {
                VoltTable guessT = t.clone(4096 * 1024);
                //System.out.printf("Empty clone:\n%s\n", guessT.toFormattedString());

                TableHelper.migrateTable(preT, guessT);
                //System.out.printf("Java migration:\n%s\n", guessT.toFormattedString());

                // deterministically sample the same rows
                VoltTable postT = sample(t);
                //System.out.printf("Second sample:\n%s\n", postT.toFormattedString());

                postT.resetRowPosition();
                preT.resetRowPosition();
                StringBuilder sb = new StringBuilder();
                if (!TableHelper.deepEqualsWithErrorMsg(postT, guessT, sb)) {
                    System.err.println(sb.toString());
                    assert(false);
                }
            }
        }

        client.close();
    }
}
