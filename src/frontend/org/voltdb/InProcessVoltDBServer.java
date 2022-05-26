/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.utils.SQLCommand;

/**
 * Class used to run a single VoltDB server in-process for debug and test
 * purposes.
 *
 */
public class InProcessVoltDBServer {
    ServerThread server = null;
    String pathToLicense = null;
    Client loaderClient = null;
    int sitesPerHost = 8; // default
    int httpdPort = 8080;

    List<Client> trackedClients = new ArrayList<>();

    /**
     * Create an instance ready to start.
     */
    public InProcessVoltDBServer() {}


    /**
     * Set the number of partitions the single VoltDB server will use.
     * The default is 8. Must be called before {@link #start()}.
     * @param partitionCount The number of partitions desired.
     * @return InProcessVoltDBServer instance for chaining.
     */
    public InProcessVoltDBServer configPartitionCount(int partitionCount) {
        sitesPerHost = partitionCount;
        return this;
    }

    /**
     * Override HTTP port.
     * @param port http port to use.
     * @return instance for chaining.
     */
    public InProcessVoltDBServer configureHttpPort(int port) {
        httpdPort = port;
        return this;
    }

    /**
     * When using enterprise or pro edition, specify a path to the license needed.
     * @param path Path to license. Must be called before {@link #start()}.
     * @return InProcessVoltDBServer instance for chaining.
     */
    public InProcessVoltDBServer configPathToLicense(String path) {
        pathToLicense = path;
        return this;
    }

    /**
     * Starts the in-process server and blocks until it is ready to accept
     * connections.
     * @return InProcessVoltDBServer instance for chaining.
     */
    public InProcessVoltDBServer start() {
        DeploymentBuilder depBuilder = new DeploymentBuilder(sitesPerHost, 1, 0);
        depBuilder.setEnableCommandLogging(false);
        depBuilder.setUseDDLSchema(true);
        depBuilder.setHTTPDPort(httpdPort);
        depBuilder.setJSONAPIEnabled(true);

        VoltDB.Configuration config = new VoltDB.Configuration();
        if (pathToLicense != null) {
            config.m_pathToLicense = pathToLicense;
        }
        else {
            config.m_pathToLicense = "./license.xml";
        }

        File tempDeployment = null;
        try {
            tempDeployment = File.createTempFile("volt_deployment_", ".xml");
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        depBuilder.writeXML(tempDeployment.getAbsolutePath());
        config.m_pathToDeployment = tempDeployment.getAbsolutePath();

        server = new ServerThread(config);

        server.start();
        server.waitForInitialization();

        return this;
    }

    /**
     * Run DDL from a file on disk (integrally uses in-process sqlcommand).
     * Must be called after {@link #start()}.
     * @param path Path to DDL file.
     * @return InProcessVoltDBServer instance for chaining.
     */
    public InProcessVoltDBServer runDDLFromPath(String path) {
        int ret = SQLCommand.mainWithReturnCode(new String[] { String.format("--ddl-file=%s", path) });
        assert(ret == 0);
        return this;
    }

    /**
     * Run DDL from a given string (integrally uses in-process sqlcommand).
     * Must be called after {@link #start()}.
     * @param ddl String containing DDL to run.
     * @return InProcessVoltDBServer instance for chaining.
     */
    public InProcessVoltDBServer runDDLFromString(String ddl) {
        try {
            File tempDDLFile = File.createTempFile("volt_ddl_", ".sql");
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempDDLFile));
            writer.write(ddl + "\n");
            writer.close();
            runDDLFromPath(tempDDLFile.getAbsolutePath());
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return this;
    }

    /**
     * Stop the in-process server and block until it has completely stopped.
     * Obviously must be called after {@link #start()}.
     */
    public void shutdown() {
        for (Client client : trackedClients) {
            // best effort closing -- ignores many problems
            try {
                client.drain();
                client.close();
            }
            catch (Exception e) {}
        }
        loaderClient = null;

        try {
            server.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            server.join(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create and connect a client to the in-process VoltDB server.
     * Note, client will be automatically closed when the in-process server
     * is shut down.
     * Must be called after {@link #start()}.
     * @return Connected client.
     * @throws Exception on failure to connect properly.
     */
    public Client getClient() throws Exception {
        ClientConfig config = new ClientConfig();
        // turn off the timeout for debugging
        config.setProcedureCallTimeout(0);
        Client client = ClientFactory.createClient(config);
        // track this client so it can be closed at shutdown
        trackedClients.add(client);
        client.createConnection("localhost");
        return client;
    }

    /**
     * Helper method for loading a row into a table.
     * Must be called after {@link #start()} and after {@link #runDDLFromPath(String)} or {@link #runDDLFromString(String)}.
     * @param tableName The case-insensitive name of the target table.
     * @param row An array of schema-compatible values comprising the row to load.
     * @throws Exception if the server is unable to complete a transaction of if the input doesn't match table schema.
     */
    public void loadRow(String tableName, Object... row) throws Exception {
        if (loaderClient == null) {
            loaderClient = getClient();
        }
        String procName = tableName.trim().toUpperCase() + ".insert";
        loaderClient.callProcedure(procName, row);
    }
}
