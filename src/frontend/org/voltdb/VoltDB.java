/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.PortGenerator;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;

/**
 * VoltDB provides main() for the VoltDB server
 */
public class VoltDB {

    /** Global constants */
    public static final int DEFAULT_PORT = 21212;
    public static final int DEFAULT_ADMIN_PORT = 21211;
    public static final int DEFAULT_INTERNAL_PORT = 3021;
    public static final int DEFAULT_ZK_PORT = 2181;
    public static final String DEFAULT_EXTERNAL_INTERFACE = "";
    public static final String DEFAULT_INTERNAL_INTERFACE = "";
    public static final int DEFAULT_DR_PORT = 5555;
    public static final int BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;
    public static final int INITIATOR_SITE_ID = 0;
    public static final int SITES_TO_HOST_DIVISOR = 100;
    public static final int MAX_SITES_PER_HOST = 128;

    // The name of the SQLStmt implied by a statement procedure's sql statement.
    public static final String ANON_STMT_NAME = "sql";

    public enum START_ACTION {
        CREATE, RECOVER, START
    }

    public static Charset UTF8ENCODING = Charset.forName("UTF-8");

    // if VoltDB is running in your process, prepare to use UTC (GMT) timezone
    public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
    }
    static {
        setDefaultTimezone();
    }

    /** Encapsulates VoltDB configuration parameters */
    public static class Configuration {

        public List<Integer> m_ipcPorts = Collections.synchronizedList(new LinkedList<Integer>());

        protected static final VoltLogger hostLog = new VoltLogger("HOST");

        /** use normal JNI backend or optional IPC or HSQLDB backends */
        public BackendTarget m_backend = BackendTarget.NATIVE_EE_JNI;

        /** leader hostname */
        public String m_leader = null;

        /** name of the m_catalog JAR file */
        public String m_pathToCatalog = null;

        /** name of the deployment file */
        public String m_pathToDeployment = null;

        /** name of the license file, for commercial editions */
        public String m_pathToLicense = "license.xml";

        /** false if voltdb.so shouldn't be loaded (for example if JVM is
         *  started by voltrun).
         */
        public boolean m_noLoadLibVOLTDB = false;

        public String m_zkInterface = "127.0.0.1:" + VoltDB.DEFAULT_ZK_PORT;

        /** port number for the first client interface for each server */
        public int m_port = DEFAULT_PORT;

        /** override for the admin port number in the deployment file */
        public int m_adminPort = -1;

        /** port number to use to build intra-cluster mesh */
        public int m_internalPort = DEFAULT_INTERNAL_PORT;

        /** interface to listen to clients on (default: any) */
        public String m_externalInterface = DEFAULT_EXTERNAL_INTERFACE;

        /** interface to use for backchannel comm (default: any) */
        public String m_internalInterface = DEFAULT_INTERNAL_INTERFACE;

        /** port number to use for DR channel (override in the deployment file) */
        public int m_drAgentPortStart = -1;

        /** information used to rejoin this new node to a cluster */
        public String m_rejoinToHostAndPort = null;

        /** HTTP port can't be set here, but eventually value will be reflected here */
        public int m_httpPort = Integer.MAX_VALUE;

        /** running the enterprise version? */
        public final boolean m_isEnterprise = org.voltdb.utils.MiscUtils.isPro();

        public int m_deadHostTimeoutMS = 10000;

        /** start up action */
        public START_ACTION m_startAction = START_ACTION.START;

        /** start mode: normal, paused*/
        public OperationMode m_startMode = OperationMode.RUNNING;

        /** replication role. */
        public ReplicationRole m_replicationRole = ReplicationRole.NONE;

        /**
         * At rejoin time an interface will be selected. It will be the
         * internal interface specified on the command line. If none is specified
         * then the interface that the system selects for connecting to
         * the pre-existing node is used. It is then stored here
         * so it can be used for receiving connections by RecoverySiteDestinationProcessor
         */
        public String m_selectedRejoinInterface = null;

        /**
         * Whether or not adhoc queries should generate debugging output
         */
        public boolean m_quietAdhoc = false;

        public final File m_commitLogDir = new File("/tmp");

        /**
         * How much (ms) to skew the timestamp generation for
         * the TransactionIdManager. Should be ZERO except for tests.
         */
        public long m_timestampTestingSalt = 0;

        /** true if we're running the rejoin tests. Not used in production. */
        public boolean m_isRejoinTest = false;

        public Integer m_leaderPort = DEFAULT_INTERNAL_PORT;

        public int getZKPort() {
            return MiscUtils.getPortFromHostnameColonPort(m_zkInterface, VoltDB.DEFAULT_ZK_PORT);
        }

        public Configuration() { }

        public Configuration(PortGenerator ports) {
            m_port = ports.nextClient();
            m_adminPort = ports.nextAdmin();
            m_internalPort = ports.next();
            m_zkInterface = "127.0.0.1:" + ports.next();
        }

        public Configuration(String args[]) {
            String arg;

            // Arguments are accepted in any order.
            //
            // options:
            // [noloadlib] [hsqldb|jni|ipc] [catalog path_to_catalog] [deployment path_to_deployment]

            for (int i=0; i < args.length; ++i) {
                arg = args[i];
                // Some LocalCluster ProcessBuilder instances can result in an empty string
                // in the array args.  Ignore them.
                if (arg.equals(""))
                {
                    continue;
                }

                // Handle request for help/usage
                if (arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("--help")) {
                    usage(System.out);
                    System.exit(-1);
                }

                if (arg.equals("noloadlib")) {
                    m_noLoadLibVOLTDB = true;
                }
                else if (arg.equals("ipc")) {
                    m_backend = BackendTarget.NATIVE_EE_IPC;
                }
                else if (arg.equals("jni")) {
                    m_backend = BackendTarget.NATIVE_EE_JNI;
                }
                else if (arg.equals("hsqldb")) {
                    m_backend = BackendTarget.HSQLDB_BACKEND;
                }
                else if (arg.equals("valgrind")) {
                    m_backend = BackendTarget.NATIVE_EE_VALGRIND_IPC;
                }
                else if (arg.equals("quietadhoc"))
                {
                    m_quietAdhoc = true;
                }
                // handle from the command line as two strings <catalog> <filename>
                else if (arg.equals("port")) {
                    m_port = Integer.parseInt(args[++i]);
                }
                else if (arg.startsWith("port ")) {
                    m_port = Integer.parseInt(arg.substring("port ".length()));
                }
                else if (arg.equals("adminport")) {
                    m_adminPort = Integer.parseInt(args[++i]);
                }
                else if (arg.startsWith("adminport ")) {
                    m_adminPort = Integer.parseInt(arg.substring("adminport ".length()));
                }
                else if (arg.equals("internalport")) {
                    m_internalPort = Integer.parseInt(args[++i]);
                }
                else if (arg.startsWith("internalport ")) {
                    m_internalPort = Integer.parseInt(arg.substring("internalport ".length()));
                }
                else if (arg.equals("replicationport")) {
                    m_drAgentPortStart = Integer.parseInt(args[++i]);
                }
                else if (arg.startsWith("replicationport ")) {
                    m_drAgentPortStart = Integer.parseInt(arg.substring("replicationport ".length()));
                }
                else if (arg.startsWith("zkport")) {
                    m_zkInterface = "127.0.0.1:" + args[++i];
                }
                else if (arg.equals("externalinterface")) {
                    m_externalInterface = args[++i].trim();
                }
                else if (arg.startsWith("externalinterface ")) {
                    m_externalInterface = arg.substring("externalinterface ".length()).trim();
                }
                else if (arg.equals("internalinterface")) {
                    m_internalInterface = args[++i].trim();
                }
                else if (arg.startsWith("internalinterface ")) {
                    m_internalInterface = arg.substring("internalinterface ".length()).trim();
                }
                else if (arg.equals("leaderport")) {
                    m_leaderPort = Integer.valueOf(args[++i]);
                }
                else if (arg.equals("leader")) {
                    m_leader = args[++i].trim();
                } else if (arg.startsWith("leader")) {
                    m_leader = arg.substring("leader ".length()).trim();
                }
                else if (arg.equals("rejoinhost")) {
                    m_rejoinToHostAndPort = args[++i].trim();
                }
                else if (arg.startsWith("rejoinhost ")) {
                    m_rejoinToHostAndPort = arg.substring("rejoinhost ".length()).trim();
                }

                else if (arg.equals("create")) {
                    m_startAction = START_ACTION.CREATE;
                } else if (arg.equals("recover")) {
                    m_startAction = START_ACTION.RECOVER;
                } else if (arg.equals("start")) {
                    m_startAction = START_ACTION.START;
                }

                else if (arg.equals("replica")) {
                    m_replicationRole = ReplicationRole.REPLICA;
                }
                else if (arg.equals("dragentportstart")) {
                    m_drAgentPortStart = Integer.parseInt(args[++i]);
                }

                // handle timestampsalt
                else if (arg.equals("timestampsalt")) {
                    m_timestampTestingSalt = Long.parseLong(args[++i]);
                }
                else if (arg.startsWith("timestampsalt ")) {
                    m_timestampTestingSalt = Long.parseLong(arg.substring("timestampsalt ".length()));
                }

                else if (arg.equals("catalog")) {
                    m_pathToCatalog = args[++i];
                }
                // and from ant as a single string "m_catalog filename"
                else if (arg.startsWith("catalog ")) {
                    m_pathToCatalog = arg.substring("catalog ".length());
                }
                else if (arg.equals("deployment")) {
                    m_pathToDeployment = args[++i];
                } else if (arg.equals("license")) {
                    m_pathToLicense = args[++i];
                } else if (arg.equalsIgnoreCase("ipcports")) {
                    String portList = args[++i];
                    String ports[] = portList.split(",");
                    for (String port : ports) {
                        m_ipcPorts.add(Integer.valueOf(port));
                    }
                } else {
                    hostLog.fatal("Unrecognized option to VoltDB: " + arg);
                    usage();
                    System.exit(-1);
                }
            }
            // ENG-2815 If deployment is null (the user wants the default) and
            // leader is null, supply the only valid leader value ("localhost").
            if (m_leader == null && m_pathToDeployment == null) {
                m_leader = "localhost";
            }
        }

        /**
         * Validates configuration settings and logs errors to the host log.
         * You typically want to have the system exit when this fails, but
         * this functionality is left outside of the method so that it is testable.
         * @return Returns true if all required configuration settings are present.
         */
        public boolean validate() {
            boolean isValid = true;

            if (m_startAction != START_ACTION.START &&
                m_rejoinToHostAndPort != null &&
                m_pathToCatalog == null) {
                isValid = false;
                hostLog.fatal("The catalog location is missing.");
            }

            if (m_leader == null && m_rejoinToHostAndPort == null) {
                isValid = false;
                hostLog.fatal("The leader hostname is missing.");
            }

            if (m_backend.isIPC) {
                if (m_ipcPorts.isEmpty()) {
                    isValid = false;
                    hostLog.fatal("Specified an IPC backend but did not supply a , " +
                            " separated list of ports via ipcports param");
                }
            }

            // require deployment file location
            if (m_rejoinToHostAndPort == null) {
                // require deployment file location (null is allowed to receive default deployment)
                if (m_pathToDeployment != null && m_pathToDeployment.isEmpty()) {
                    isValid = false;
                    hostLog.fatal("The deployment file location is empty.");
                }

                if (m_replicationRole == ReplicationRole.REPLICA) {
                    if (m_startAction == START_ACTION.RECOVER) {
                        isValid = false;
                        hostLog.fatal("Replica cluster only supports create database");
                    } else {
                        m_startAction = START_ACTION.CREATE;
                    }
                }
            }

            return isValid;
        }

        /**
         * Prints a usage message on stderr.
         */
        public void usage() { usage(System.err); }

        /**
         * Prints a usage message on the designated output stream.
         */
        public void usage(PrintStream os) {
            // N.B: this text is user visible. It intentionally does NOT reveal options not interesting to, say, the
            // casual VoltDB operator. Please do not reveal options not documented in the VoltDB documentation set. (See
            // GettingStarted.pdf).
            String message = "";
            if (org.voltdb.utils.MiscUtils.isPro()) {
                message = "Usage: voltdb [create|recover|replica] [leader <hostname>] [deployment <deployment.xml>] license <license.xml> catalog <catalog.jar>";
                os.println(message);
                // Log it to log4j as well, which will capture the output to a file for (hopefully never) cases where VEM has issues (it generates command lines).
                hostLog.info(message);
            } else {
                message = "Usage: voltdb [create|recover] [leader <hostname>] [deployment <deployment.xml>] catalog <catalog.jar>";
                os.println(message);
                // Log it to log4j as well, which will capture the output to a file for (hopefully never) cases where VEM has issues (it generates command lines).
                hostLog.info(message);
            }
            // Don't bother logging these for log4j, only dump them to the designated stream.
            os.println("If action is not specified the default is to 'recover' the database if a snapshot is present otherwise 'create'.");
            os.println("If no deployment is specified, a default 1 node cluster deployment will be configured.");
        }

        /**
         * Helper to set the path for compiled jar files.
         *  Could also live in VoltProjectBuilder but any code that creates
         *  a catalog will probably start VoltDB with a Configuration
         *  object. Perhaps this is more convenient?
         * @return the path chosen for the catalog.
         */
        public String setPathToCatalogForTest(String jarname) {
            m_pathToCatalog = getPathToCatalogForTest(jarname);
            return m_pathToCatalog;
        }

        public static String getPathToCatalogForTest(String jarname) {
            String answer = jarname;

            // first try to get the "right" place to put the thing
            if (System.getenv("TEST_DIR") != null) {
                answer = System.getenv("TEST_DIR") + File.separator + jarname;
                // returns a full path, like a boss
                return new File(answer).getAbsolutePath();
            }

            // try to find an obj directory
            String userdir = System.getProperty("user.dir");
            String buildMode = System.getProperty("build");
            if (buildMode == null)
                buildMode = "release";
            assert(buildMode.length() > 0);
            if (userdir != null) {
                File userObjDir = new File(userdir + File.separator + "obj" + File.separator + buildMode);
                if (userObjDir.exists() && userObjDir.isDirectory() && userObjDir.canWrite()) {
                    File testobjectsDir = new File(userObjDir.getPath() + File.separator + "testobjects");
                    if (!testobjectsDir.exists()) {
                        boolean created = testobjectsDir.mkdir();
                        assert(created);
                    }
                    assert(testobjectsDir.isDirectory());
                    assert(testobjectsDir.canWrite());
                    return testobjectsDir.getAbsolutePath() + File.separator + jarname;
                }
            }

            // otherwise use a local dir
            File testObj = new File("testobjects");
            if (!testObj.exists()) {
                testObj.mkdir();
            }
            assert(testObj.isDirectory());
            assert(testObj.canWrite());
            return testObj.getAbsolutePath() + File.separator + jarname;
        }

    }

    /* helper functions to access current configuration values */
    public static boolean getLoadLibVOLTDB() {
        return !(m_config.m_noLoadLibVOLTDB);
    }

    public static BackendTarget getEEBackendType() {
        return m_config.m_backend;
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            return;
        }

        // Even if the logger is null, don't stop.  We want to log the stack trace and
        // any other pertinent information to a .dmp file for crash diagnosis
        StringBuilder stacktrace_sb = new StringBuilder("Stack trace from crashLocalVoltDB() method:\n");

        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        StackTraceElement[] myTrace = traces.get(Thread.currentThread());
        for (StackTraceElement ste : myTrace) {
            stacktrace_sb.append(ste.toString()).append("\n");
        }

        // Create a special dump file to hold the stack trace
        try
        {
            TimestampType ts = new TimestampType(new java.util.Date());
            CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
            String root = catalogContext != null ? catalogContext.cluster.getVoltroot() + File.separator : "";
            PrintWriter writer = new PrintWriter(root + "voltdb_crash" + ts.toString().replace(' ', '-') + ".txt");
            writer.println("Time: " + ts);
            writer.println("Message: " + errMsg);

            writer.println();
            writer.println("Platform Properties:");
            PlatformProperties pp = PlatformProperties.getPlatformProperties();
            String[] lines = pp.toLogLines().split("\n");
            for (String line : lines) {
                writer.println(line.trim());
            }

            writer.println();
            writer.println("****** Current Thread ****** ");
            writer.println(stacktrace_sb);
            writer.println("****** All Threads ******");
            Iterator<Thread> it = traces.keySet().iterator();
            while (it.hasNext())
            {
                Thread key = it.next();
                writer.println();
                StackTraceElement[] st = traces.get(key);
                writer.println("****** " + key + " ******");
                for (StackTraceElement ste : st)
                    writer.println(ste);
            }
            writer.close();
        }
        catch (Throwable err)
        {
            // shouldn't fail, but..
            err.printStackTrace();
        }

        VoltLogger log = null;
        try
        {
            log = new VoltLogger("HOST");
        }
        catch (RuntimeException rt_ex)
        { /* ignore */ }

        if (log != null)
        {
            if (t != null)
                log.fatal(errMsg, t);
            else
                log.fatal(errMsg);

            if (stackTrace)
                log.fatal(stacktrace_sb);
        }

        System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
        System.err.println("The log may contain additional information.");
        System.exit(-1);
    }

    /*
     * For tests that causes failures,
     * allow them stop the crash and inspect.
     */
    public static boolean ignoreCrash = false;

    public static boolean wasCrashCalled = false;

    public static String crashMessage;

    /**
     * Exit the process with an error message, optionally with a stack trace.
     * Also notify all connected peers that the node is going down.
     */
    public static void crashGlobalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            return;
        }
        try {
            instance().getHostMessenger().sendPoisonPill(errMsg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try { Thread.sleep(500); } catch (InterruptedException e) {}
        crashLocalVoltDB(errMsg, stackTrace, t);
    }

    /**
     * Entry point for the VoltDB server process.
     * @param args Requires catalog and deployment file locations.
     */
    public static void main(String[] args) {
        //Thread.setDefaultUncaughtExceptionHandler(new VoltUncaughtExceptionHandler());
        Configuration config = new Configuration(args);

        try {
            if (!config.validate()) {
                config.usage();
                System.exit(-1);
            } else {
                initialize(config);
                instance().run();
            }
        }
        catch (OutOfMemoryError e) {
            String errmsg = "VoltDB Main thread: ran out of Java memory. This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, false, e);
        }
    }

    /**
     * Initialize the VoltDB server.
     * @param config  The VoltDB.Configuration to use to initialize the server.
     */
    public static void initialize(VoltDB.Configuration config) {
        m_config = config;
        instance().initialize(config);
    }

    /**
     * Retrieve a reference to the object implementing VoltDBInterface.  When
     * running a real server (and not a test harness), this instance will only
     * be useful after calling VoltDB.initialize().
     *
     * @return A reference to the underlying VoltDBInterface object.
     */
    public static VoltDBInterface instance() {
        return singleton;
    }

    /**
     * Useful only for unit testing.
     *
     * Replace the default VoltDB server instance with an instance of
     * VoltDBInterface that is used for testing.
     *
     */
    public static void replaceVoltDBInstanceForTest(VoltDBInterface testInstance) {
        singleton = testInstance;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    private static VoltDB.Configuration m_config = new VoltDB.Configuration();
    private static VoltDBInterface singleton = new RealVoltDB();
}
