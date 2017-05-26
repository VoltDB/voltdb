package org.voltdb.utils;

import java.util.Properties;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;

/**
 * Configuration options.
 */
public class KafkaConfig extends CLIConfig {

    @Option(shortOpt = "p", desc = "procedure name to insert the data into the database")
    String procedure = "";

    // This is set to true when -p option us used.
    boolean useSuppliedProcedure = false;

    @Option(shortOpt = "t", desc = "Kafka Topic to subscribe to")
    String topic = "";

    @Option(shortOpt = "m", desc = "maximum errors allowed")
    int maxerrors = 100;

    @Option(shortOpt = "s", desc = "list of VoltDB servers to connect to (default: localhost)")
    String servers = "localhost";

    @Option(desc = "port to use when connecting to database (default: 21212)")
    int port = Client.VOLTDB_SERVER_PORT;

    @Option(desc = "username when connecting to the VoltDB servers")
    String user = "";

    @Option(desc = "password to use when connecting to VoltDB servers")
    String password = "";

    @Option(shortOpt = "z", desc = "kafka zookeeper to connect to. (format: zkserver:port)")
    String zookeeper = ""; //No default here as default will clash with local voltdb cluster

    @Option(shortOpt = "f", desc = "Periodic Flush Interval in seconds. (default: 10)")
    int flush = 10;

    @Option(shortOpt = "k", desc = "Kafka Topic Partitions. (default: 10)")
    int kpartitions = 10; // NEEDSWORK: No longer necessary?

    @Option(shortOpt = "c", desc = "Kafka Consumer Configuration File")
    String config = ""; // NEEDSWORK: Can't use this anymore (at least in the advanced case)

    @Option(desc = "Formatter configuration file. (Optional) .")
    String formatter = "";

    /**
     * Batch size for processing batched operations.
     */
    @Option(desc = "Batch Size for processing.")
    public int batch = 200;

    /**
     * Table name to insert CSV data into.
     */
    @AdditionalArgs(desc = "insert the data into this table.")
    public String table = "";

    @Option(desc = "Use upsert instead of insert", hasArg = false)
    boolean update = false;

    @Option(desc = "Enable SSL, Optionally provide configuration file.")
    String ssl = "";

    //Read properties from formatter option and do basic validation.
    Properties m_formatterProperties = new Properties();
    /**
     * Validate command line options.
     */
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
        if (zookeeper.trim().isEmpty()) {
            exitWithMessageAndUsage("Kafka Zookeeper must be specified.");
        }
        if (port < 0) {
            exitWithMessageAndUsage("port number must be >= 0");
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
        //Try and load classes we need and not packaged.
        try {
            KafkaConfig.class.getClassLoader().loadClass("org.I0Itec.zkclient.IZkStateListener");
            KafkaConfig.class.getClassLoader().loadClass("org.apache.zookeeper.Watcher");
        } catch (ClassNotFoundException cnfex) {
            System.out.println("Cannot find the Zookeeper libraries, zkclient-0.3.jar and zookeeper-3.3.4.jar.");
            System.out.println("Use the ZKLIB environment variable to specify the path to the Zookeeper jars files.");
            System.exit(1);
        }
    }

    /**
     * Usage
     */
    @Override
    public void printUsage() {
        System.out.println("Usage: kafkaloader [args] -z kafka-zookeeper -t topic tablename");
        super.printUsage();
    }

    public String getProcedure() {
        return procedure;
    }

    public boolean isUseSuppliedProcedure() {
        return useSuppliedProcedure;
    }

    public String getTopic() {
        return topic;
    }

    public int getMaxerrors() {
        return maxerrors;
    }

    public String getServers() {
        return servers;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String s) {
        password = s;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public int getFlush() {
        return flush;
    }

    public int getPartitionCount() {
        return kpartitions;
    }

    public String getConfig() {
        return config;
    }

    public String getFormatter() {
        return formatter;
    }

    public int getBatch() {
        return batch;
    }

    public String getTable() {
        return table;
    }

    public boolean isUpdate() {
        return update;
    }

    public String getSSL() {
        return ssl;
    }

    public Properties getFormatterProperties() {
        return m_formatterProperties;
    }


}