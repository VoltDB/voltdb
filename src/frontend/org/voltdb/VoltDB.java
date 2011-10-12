/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.utils.MiscUtils;

/**
 * <code>VoltDB</code> is the main class for VoltDB server.
 * It sets up global objects and then starts the individual threads
 * for the <code>ThreadManager</code>s.
 */
public class VoltDB {

    /** Global constants */
    public static final int DEFAULT_PORT = 21212;
    public static final int DEFAULT_ADMIN_PORT = 21211;
    public static final int DEFAULT_INTERNAL_PORT = 3021;
    public static final String DEFAULT_EXTERNAL_INTERFACE = "";
    public static final String DEFAULT_INTERNAL_INTERFACE = "";

    public static final int BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;

    static final int INITIATOR_SITE_ID = 0;
    public static final int DTXN_MAILBOX_ID = 0;
    public static final int AGREEMENT_MAILBOX_ID = 1;

    // temporary for single partition testing
    static final int FIRST_SITE_ID = 1;

    public static final int SITES_TO_HOST_DIVISOR = 100;
    public static final int MAX_SITES_PER_HOST = 128;

    // The name of the SQLStmt implied by a statement procedure's sql statement.
    public static final String ANON_STMT_NAME = "sql";

    public static enum START_ACTION {
        CREATE, RECOVER, START
    }

    public static Charset UTF8ENCODING = Charset.forName("UTF-8");

    // if VoltDB is running in your process, prepare to us GMT timezone
    public synchronized static void setDefaultTimezone() {
        //System.out.println(TimeZone.getTimeZone("GMT+0").getID());
          TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
    }
    static {
        setDefaultTimezone();
    }

    /** Encapsulates VoltDB configuration parameters */
    public static class Configuration {

        public List<Integer> m_ipcPorts = Collections.synchronizedList(new LinkedList<Integer>());

        private static final VoltLogger hostLog = new VoltLogger("HOST");

        /** Whether to enable watchdogs to check for possible deadlocks **/
        public boolean m_useWatchdogs = false;

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

        public String m_zkInterface = "127.0.0.1:2181";

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

        /** information used to rejoin this new node to a cluster */
        public String m_rejoinToHostAndPort = null;

        /** HTTP port can't be set here, but eventually value will be reflected here */
        public int m_httpPort = Integer.MAX_VALUE;

        /** running the enterprise version? */
        public final boolean m_isEnterprise =
                (null != MiscUtils.loadProClass("org.voltdb.CommandLogImpl", "Command logging", true));

        public int m_deadHostTimeoutMS = 10000;

        /** start up action */
        public START_ACTION m_startAction = START_ACTION.START;

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

        public Configuration() { }

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
                } else if (arg.startsWith("zkport")) {
                    m_zkInterface = "127.0.0.1:" + args[++i];
                } else if (arg.equals("externalinterface")) {
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

                else if (arg.equals("leader")) {
                    m_leader = args[++i].trim();
                    if (m_leader.compareTo("") == 0) {
                        m_leader = null;
                    }
                } else if (arg.startsWith("leader")) {
                    m_leader = arg.substring("leader ".length()).trim();
                    if (m_leader.compareTo("") == 0) {
                        m_leader = null;
                    }
                }

                else if (arg.equals("rejoinhost")) {
                    m_rejoinToHostAndPort = args[++i].trim();
                    if (m_rejoinToHostAndPort.compareTo("") == 0)
                        m_rejoinToHostAndPort = null;
                }
                else if (arg.startsWith("rejoinhost ")) {
                    m_rejoinToHostAndPort = arg.substring("rejoinhost ".length()).trim();
                    if (m_rejoinToHostAndPort.compareTo("") == 0)
                        m_rejoinToHostAndPort = null;
                }

                else if (arg.equals("create")) {
                    m_startAction = START_ACTION.CREATE;
                } else if (arg.equals("recover")) {
                    m_startAction = START_ACTION.RECOVER;
                } else if (arg.equals("start")) {
                    m_startAction = START_ACTION.START;
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
                } else if (arg.equalsIgnoreCase("useWatchdogs")) {
                    m_useWatchdogs = true;
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
        }

        /**
         * Validates configuration settings and logs errors to the host log. You typically want to have the system exit
         * when this fails, but this functionality is left outside of the method so that it is testable.
         * @return Returns true if all required configuration settings are present.
         */
        public boolean validate() {
            boolean isValid = true;

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
            if (m_pathToDeployment == null) {
                isValid = false;
                hostLog.fatal("The deployment file location is missing.");
            } else if (m_pathToDeployment.equals("")) {
                isValid = false;
                hostLog.fatal("The deployment file location is empty.");
            }

            return isValid;
        }

        /**
         * Prints a usage message as a fatal error.
         */
        public void usage() {
            // N.B: this text is user visible. It intentionally does NOT reveal options not interesting to, say, the
            // casual VoltDB operator. Please do not reveal options not documented in the VoltDB documentation set. (See
            // GettingStarted.pdf).
            hostLog.fatal("Usage: org.voltdb.VoltDB <action> leader <hostname> deployment <deployment.xml> license <license.xml> [catalog <catalog.jar>]");
            hostLog.fatal("The _Getting Started With VoltDB_ book explains how to run VoltDB from the command line.");
        }

        /** Helper to set the path for compiled jar files.
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

    private static VoltDB.Configuration m_config = new VoltDB.Configuration();

    /* helper functions to access current configuration values */
    public static boolean getLoadLibVOLTDB() {
        return !(m_config.m_noLoadLibVOLTDB);
    }

    public static BackendTarget getEEBackendType() {
        return m_config.m_backend;
    }

    public static boolean getUseWatchdogs() {
        return m_config.m_useWatchdogs;
    }

    public static boolean getQuietAdhoc()
    {
        return m_config.m_quietAdhoc;
    }

    /**
     * Wrapper for crashLocalVoltDB() to keep compatibility with >100 calls.
     */
    @Deprecated
    public static void crashVoltDB() {
        crashLocalVoltDB("Unexpected crash", true, null);
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     *
     * In the future it would be nice to notify any non-failed subsystems
     * that the node is going down. For now, just die.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
        if (instance().ignoreCrash()) {
            return;
        }

        VoltLogger log = new VoltLogger("HOST");

        if (t != null)
            log.fatal(errMsg, t);
        else
            log.fatal(errMsg);

        if (stackTrace) {
            StringBuilder sb = new StringBuilder("Stack trace from crashVoltDB() method:\n");

            Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
            StackTraceElement[] myTrace = traces.get(Thread.currentThread());
            for (StackTraceElement ste : myTrace) {
                sb.append(ste.toString()).append("\n");
            }

            log.fatal(sb);
        }

        System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
        System.err.println("The log may contain additional information.");
        System.exit(-1);
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     * Also notify all connected peers that the node is going down.
     *
     * In the future it would be nice to notify any non-failed subsystems
     * that the node is going down. For now, just die.
     */
    public static void crashGlobalVoltDB(String errMsg, boolean stackTrace, Throwable t) {
        if (instance().ignoreCrash()) {
            return;
        }
        try {
            ((HostMessenger) instance().getMessenger()).sendPoisonPill(errMsg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try { Thread.sleep(500); } catch (InterruptedException e) {}
        crashLocalVoltDB(errMsg, stackTrace, t);
    }

    /**
     * Entry point for the VoltDB server process.
     *
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
     *
     * @param config  The VoltDB.Configuration to use to initialize the server.
     */
    public static void initialize(VoltDB.Configuration config)
    {
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
    public static void replaceVoltDBInstanceForTest(VoltDBInterface testInstance)
    {
        singleton = testInstance;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
    private static VoltDBInterface singleton = new RealVoltDB();

}
