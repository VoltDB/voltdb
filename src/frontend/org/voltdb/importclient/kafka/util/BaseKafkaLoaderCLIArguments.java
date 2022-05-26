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

package org.voltdb.importclient.kafka.util;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.voltdb.CLIConfig;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.Client;

/**
 * Common implementation for Kafka loader CLI arguments.
 * @author jcrump
 *
 */
public abstract class BaseKafkaLoaderCLIArguments extends CLIConfig {

    public static final int ZK_CONNECTION_TIMEOUT_MILLIS = 10*1000;

    // This is set to true when -p option is used.
    public boolean useSuppliedProcedure = false;

    // These values can be supplied in the properties file.
    public String groupid = "";
    public int buffersize = KafkaConstants.KAFKA_BUFFER_SIZE_DEFAULT;
    public int timeout = KafkaConstants.KAFKA_TIMEOUT_DEFAULT_MILLIS;
    public int zookeeperSessionTimeoutMillis = ZK_CONNECTION_TIMEOUT_MILLIS;

    @Option(shortOpt = "c", desc = "Kafka consumer properties file.")
    public String config = "";

    @Option(shortOpt = "p", desc = "Procedure name to insert the data into the database.")
    public String procedure = "";

    // Input formatter properties
    public Properties formatterProperties = new Properties();

    @Option(shortOpt = "t", desc = "Kafka Topic to subscribe to.")
    public String topic = "";

    @Option(shortOpt = "m", desc = "Maximum errors allowed before terminating import.")
    public int maxerrors = 100;

    @Option(desc = "Default port for VoltDB servers.")
    public String port = "";

    @Option(shortOpt = "H", desc = "Comma separated list of VoltDB servers (host[:port]) to connect to.")
    public String host = "";

    @Option(shortOpt = "s", desc = "Comma separated list of VoltDB servers (host[:port]) to connect to. Deprecated; use 'host' instead.")
    public String servers = "";

    @Option(desc = "Username for connecting to VoltDB servers.")
    public String user = "";

    @Option(desc = "Password for connecting to VoltDB servers.")
    public String password = "";

    @Option(desc = "Credentials that contains username and password information")
    public String credentials = "";

    @Option(shortOpt = "z", desc = "Kafka Zookeeper to connect to in the format (host:port).")
    public String zookeeper = ""; //No default here as default will clash with local voltdb cluster

    @Option(shortOpt = "b", desc = "Comma-separated list of Kafka brokers (host:port) to connect to.")
    public String brokers = "";

    @Option(shortOpt = "f", desc = "Periodic flush interval in seconds (default: 10).")
    public int flush = 10;

    @Option(desc = "Formatter configuration file (optional).")
    public String formatter = "";

    @Option(desc = "Batch size for writing to VoltDB.")
    public int batch = 200;

    @Option(desc = "Priority for writing to VoltDB.")
    public int priority = 0;

    @AdditionalArgs(desc = "Insert the data into this table.")
    public String table = "";

    @Option(desc = "Use upsert instead of insert.", hasArg = false)
    public boolean update = false;

    @Option(desc = "Enable SSL connection to Volt, optionally provide configuration file.")
    public String ssl = "";

    @Option(desc = "Kafka time-based commit policy interval in milliseconds.  Default is to use manual offset commit.")
    public String commitpolicy = "";

    @Option(desc = "Stop when all connections are lost", hasArg = false)
    public boolean stopondisconnect = false;

    protected PrintWriter warningWriter = null;

    protected BaseKafkaLoaderCLIArguments(PrintWriter pw) {
        this.warningWriter = pw;
    }

    protected BaseKafkaLoaderCLIArguments() {
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
        } else if (!servers.trim().isEmpty()) {
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

    public String getFormatter() {
        return formatterProperties.getProperty("formatter");
    }

    private void initializeCustomFormatter() throws Exception {
        // Load up any supplied formatter
        if (formatter.trim().isEmpty()) {
            return;
        }
        InputStream pfile = new FileInputStream(formatter);
        formatterProperties.load(pfile);
        String formatter = formatterProperties.getProperty("formatter");
        if (formatter == null || formatter.trim().isEmpty()) {
            throw new RuntimeException("Formatter class must be specified in formatter file as formatter=<class>: " + formatter);
        }
    }

    private void initializeDefaultsFromPropertiesFile() throws Exception {

        if (config.trim().isEmpty()) {
            return;
        }

        Properties props = new Properties();
        try (FileReader fr = new FileReader(config.trim())) {
            props.load(fr);
        }

        String prop = props.getProperty("group.id", "");
        if (prop.isEmpty()) {
            groupid = "voltdb-" + (useSuppliedProcedure ? procedure : table);
        }
        else {
            groupid = prop;
        }

        // socket.timeout.ms
        prop = props.getProperty("socket.timeout.ms", null);
        if (prop != null) {
            timeout = Integer.parseInt(prop);
        }

        // socket.receive.buffer.bytes
        prop = props.getProperty("socket.receive.buffer.bytes", null);
        if (prop != null) {
            buffersize = Integer.parseInt(prop);
        }

        // zookeeper.session.timeout.millis
        prop = props.getProperty("zookeeper.session.timeout.millis", null);
        if (prop != null) {
            zookeeperSessionTimeoutMillis = Integer.parseInt(prop);
        }
        // If following properties are set that means we are using commit policy of time this is for backward compatibility
        // "auto.commit.interval.ms"
        // "auto.commit.enable"
        prop = props.getProperty("auto.commit.enable", "false");
        if (Boolean.valueOf(prop)) {
            String ts = props.getProperty("auto.commit.interval.ms", "1000");
            commitpolicy = ts + "ms";
        }
    }

    @Override
    public void validate() {

        if (batch < 0) {
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
        if (commitpolicy.trim().isEmpty()) {
            commitpolicy = KafkaCommitPolicy.NONE.name();
        }
        if (!servers.trim().isEmpty()) {
            if (!host.trim().isEmpty()) {
                warningWriter.println("Warning: --servers argument is deprecated in favor of --host; value is ignored.");
            }
            else {
                warningWriter.println("Warning: --servers argument is deprecated; please use --host instead.");
            }
        }
        if (!port.trim().isEmpty()) {
            warningWriter.println("Warning: --port argument is deprecated, please use --host with <host:port> URIs instead.");
        }

        try {
            initializeCustomFormatter();
            initializeDefaultsFromPropertiesFile();
        }
        catch (Exception e) {
            System.err.println("Could not initialize formatters or properties:" + e.getMessage());
            System.exit(-1);
        }
    }
}
