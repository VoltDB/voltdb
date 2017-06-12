/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importclient.kafka;

import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;

/*
 * Process command line arguments and do some validation.
 */
public class KafkaExternalLoaderCLIArguments extends CLIConfig {

    @Option(shortOpt = "c", desc = "Kafka consumer properties file (deprecated)")
    public String config = "";

    @Option(shortOpt = "p", desc = "Procedure name to insert the data into the database")
    public String procedure = "";

    // This is set to true when -p option is used.
    public boolean useSuppliedProcedure = false;

    // Input formatter properties
    public Properties m_formatterProperties = new Properties();

    @Option(shortOpt = "t", desc = "Kafka Topic to subscribe to")
    public String topic = "";

    @Option(shortOpt = "g", desc = "Kafka group-id")
    public String groupid = "";

    @Option(shortOpt = "m", desc = "Maximum errors allowed before terminating import")
    public int maxerrors = 100;

    @Option(desc = "Default port for VoltDB servers")
    public String port = "";

    @Option(desc = "Comma separated list of VoltDB servers (host[:port]) to connect to")
    public String host = "";

    @Option(shortOpt = "s", desc = "Comma separated list of VoltDB servers (host[:port]) to connect to. Deprecated; use 'host' instead.")
    public String servers = "";

    @Option(desc = "Username for connecting to VoltDB servers")
    public String user = "";

    @Option(desc = "Password for connecting to VoltDB servers")
    public String password = "";

    @Option(shortOpt = "z", desc = "Kafka Zookeeper to connect to (format: host:port)")
    public String zookeeper = ""; //No default here as default will clash with local voltdb cluster

    @Option(shortOpt = "b", desc = "Comma-separated list of Kafka brokers (host:port) to connect to")
    public String brokers = "";

    @Option(shortOpt = "f", desc = "Periodic flush interval in seconds. (default: 10)")
    public int flush = 10;

    @Option(desc = "Formatter configuration file. (Optional) .")
    public String formatter = "";

    @Option(desc = "Batch size for writing to VoltDB.")
    public int batchsize = 200;

    @AdditionalArgs(desc = "Insert the data into this table.")
    public String table = "";

    @Option(desc = "Use upsert instead of insert", hasArg = false)
    public boolean update = false;

    @Option(desc = "Enable SSL, optionally provide configuration file.")
    public String ssl = "";

    @Option(desc = "Kafka consumer buffer size (default 65536).")
    public int buffersize = 65536;

    @Option(desc = "Kafka consumer socket timeout, in milliseconds (default 30000, or thirty seconds)")
    public int timeout = 30000;

    @Option(desc = "Kafka time-based commit policy interval in milliseconds.  Default is to use manual offset commit.")
    public String commitPolicy = "";

    private PrintWriter warningWriter = null;

    public KafkaExternalLoaderCLIArguments(PrintWriter pw) {
        this.warningWriter = pw;
    }

    public KafkaExternalLoaderCLIArguments() {
        this.warningWriter = new PrintWriter(System.err, true); // Auto-flush
    }

    public List<String> getVoltHosts() throws Exception {

        ArrayList<String> hostPorts = new ArrayList<String>();
        int defaultVoltPort = port.trim().isEmpty() ? Client.VOLTDB_SERVER_PORT : Integer.parseInt(port.trim());

        // Normalize the host:port URIs for Volt. If nothing is specified, default to localhost. Otherwise, the 'host' argument takes
        // precedence over the deprecated (as of 7.5) 'server' argument
        String hostPortArg = "localhost" + ":" + defaultVoltPort;
        if (!host.trim().isEmpty()) {
            hostPortArg = host;
        }
        else if (!servers.trim().isEmpty()) {
            hostPortArg = servers;
        }

        // Look at each host, and if they don't include the port, add in the default:
        for (String h : hostPortArg.split(",")) {
            if (h.indexOf(':') < 0) {
                h = h + ":" + defaultVoltPort;
            }
            hostPorts.add(h);
        }

        return hostPorts;
    }

    public String getGroupId() throws Exception {
        String groupId;
        if (groupid == null || groupid.trim().length() == 0) {
            // Look into the (deprecated) config file, if present, and try to get it from there. This will help with
            // compatibility.
            if (!config.trim().isEmpty()) {
                try (FileReader fr = new FileReader(config.trim())) {
                    Properties props = new Properties();
                    props.load(fr);
                    groupid = props.getProperty("group.id", "");
                    if (!groupid.isEmpty()) {
                        warningWriter.println("Warning: Kafka group.id property extracted from properties file, which is deprecated.  Use --groupid argument instead.");
                    }
                }
            }
        }

        if (groupid == null || groupid.trim().length() == 0) {
            groupId = "voltdb-" + (useSuppliedProcedure ? procedure : table);
        }
        else {
            groupId = groupid.trim();
        }
        return groupId;
    }

    @Override
    public void validate() {

        if (batchsize < 0) {
            exitWithMessageAndUsage("batch size number must be >= 0");
        }
        if (flush <= 0) {
            exitWithMessageAndUsage("Periodic Flush Interval must be > 0");
        }
        if (topic.trim().isEmpty()) {
            exitWithMessageAndUsage("Topic must be specified.");
        }
        if (zookeeper.trim().isEmpty() && brokers.trim().isEmpty()) {
            exitWithMessageAndUsage("Either Kafka Zookeeper or list of brokers must be specified.");
        }
        if (!zookeeper.trim().isEmpty() && !brokers.trim().isEmpty()) {
            exitWithMessageAndUsage("Only one of Kafka Zookeeper or list of brokers can be specified.");
        }
        if (procedure.trim().isEmpty() && table.trim().isEmpty()) {
            exitWithMessageAndUsage("procedure name or a table name required");
        }
        if (!procedure.trim().isEmpty() && !table.trim().isEmpty()) {
            exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
        }
        if (!procedure.trim().isEmpty()) {
            useSuppliedProcedure = true;
        }
        if ((useSuppliedProcedure) && (update)){
            update = false;
            exitWithMessageAndUsage("update is not applicable when stored procedure specified");
        }
        if (commitPolicy.trim().isEmpty()) {
            commitPolicy = KafkaImporterCommitPolicy.NONE.name();
        }
        if (!servers.trim().isEmpty()) {
            warningWriter.println("Warning: --servers argument is deprecated; please use --host instead.");
        }
        if (!config.trim().isEmpty()) {
            warningWriter.println("Warning: --config argument is deprecated, please consult the documentation.");
        }
        if (!port.trim().isEmpty()) {
            warningWriter.println("Warning: --port argument is deprecated, please use --host with <host:port> URIs instead.");
        }
    }
}