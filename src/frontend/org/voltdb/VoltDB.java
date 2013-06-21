/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TimeZone;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
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

    // Utility to calculate whether Iv2 is enabled or not for test cases.
    // There are several ways to enable Iv2, of course. Ideally, use a cluster
    // command line flag (enableiv2). Second best, use the VOLT_ENABLEIV2
    // environment variable.
    //
    // IMPORTANT: To determine if Iv2 is enabled at runtime,
    // call RealVoltDB.isIV2Enabled();
    public static boolean checkTestEnvForIv2()
    {
        String iv2 = System.getenv().get("VOLT_ENABLEIV2");
        if (iv2 == null) {
            iv2 = System.getProperty("VOLT_ENABLEIV2");
        }
        if (iv2 != null && iv2.equalsIgnoreCase("false")) {
            return false;
        }
        else {
            return true;
        }
    }

    // Utility to try to figure out if this is a test case.  Various junit targets in
    // build.xml set this environment variable to give us a hint
    public static boolean isThisATest()
    {
        String test = System.getenv().get("VOLT_JUSTATEST");
        if (test == null) {
            test = System.getProperty("VOLT_JUSTATEST");
        }
        if (test != null && test.equalsIgnoreCase("YESYESYES")) {
            return true;
        }
        else {
            return false;
        }
    }

    // The name of the SQLStmt implied by a statement procedure's sql statement.
    public static final String ANON_STMT_NAME = "sql";

    public static boolean createForRejoin(StartAction startAction)
    {
        return startAction.doesRejoin();
    }

    //The GMT time zone you know and love
    public static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT+0");

    //The time zone Volt is actually using, currently always GMT
    public static final TimeZone VOLT_TIMEZONE = GMT_TIMEZONE;

    //Whatever the default timezone was for this locale before we replaced it
    public static final TimeZone REAL_DEFAULT_TIMEZONE;

    // ODBC Datetime Format
    // if you need microseconds, you'll have to change this code or
    //  export a bigint representing microseconds since an epoch
    public static final String ODBC_DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";

    // if VoltDB is running in your process, prepare to use UTC (GMT) timezone
    public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(GMT_TIMEZONE);
    }

    static {
        REAL_DEFAULT_TIMEZONE = TimeZone.getDefault();
        setDefaultTimezone();
    }

    /** Encapsulates VoltDB configuration parameters */
    public static class Configuration {

        public List<Integer> m_ipcPorts = Collections.synchronizedList(new LinkedList<Integer>());

        protected static final VoltLogger hostLog = new VoltLogger("HOST");

        /** select normal JNI backend.
         *  IPC, Valgrind, and HSQLDB are the other options.
         */
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

        /** HTTP port can't be set here, but eventually value will be reflected here */
        public int m_httpPort = Integer.MAX_VALUE;

        /** running the enterprise version? */
        public final boolean m_isEnterprise = org.voltdb.utils.MiscUtils.isPro();

        public int m_deadHostTimeoutMS = 10000;

        /** start up action */
        public StartAction m_startAction = null;

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

        /** set to true to run with iv2 initiation. Good Luck! */
        public boolean m_enableIV2 = true;

        public final Queue<String> m_networkCoreBindings = new ArrayDeque<String>();
        public final Queue<String> m_computationCoreBindings = new ArrayDeque<String>();
        public final Queue<String> m_executionCoreBindings = new ArrayDeque<String>();
        public String m_commandLogBinding = null;

        public Configuration() {
            m_enableIV2 = VoltDB.checkTestEnvForIv2();
            // Set start action create.  The cmd line validates that an action is specified, however,
            // defaulting it to create for local cluster test scripts
            m_startAction = StartAction.CREATE;
        }

        /** Behavior-less arg used to differentiate command lines from "ps" */
        public String m_tag;

        public int getZKPort() {
            return MiscUtils.getPortFromHostnameColonPort(m_zkInterface, VoltDB.DEFAULT_ZK_PORT);
        }

        public Configuration(PortGenerator ports) {
            // Default iv2 configuration to the environment settings.
            // Let explicit command line override the environment.
            m_enableIV2 = VoltDB.checkTestEnvForIv2();
            m_port = ports.nextClient();
            m_adminPort = ports.nextAdmin();
            m_internalPort = ports.next();
            m_zkInterface = "127.0.0.1:" + ports.next();
            // Set start action create.  The cmd line validates that an action is specified, however,
            // defaulting it to create for local cluster test scripts
            m_startAction = StartAction.CREATE;
        }

        public Configuration(String args[]) {
            String arg;

            // let the command line override the environment setting for enable iv2.
            m_enableIV2 = VoltDB.checkTestEnvForIv2();

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
                } else if (arg.startsWith("networkbindings")) {
                    for (String core : args[++i].split(",")) {
                        m_networkCoreBindings.offer(core);
                    }
                    System.out.println("Network bindings are " + m_networkCoreBindings);
                }
                else if (arg.startsWith("computationbindings")) {
                    for (String core : args[++i].split(",")) {
                        m_computationCoreBindings.offer(core);
                    }
                    System.out.println("Computation bindings are " + m_computationCoreBindings);
                }
                else if (arg.startsWith("executionbindings")) {
                    for (String core : args[++i].split(",")) {
                        m_executionCoreBindings.offer(core);
                    }
                    System.out.println("Execution bindings are " + m_executionCoreBindings);
                } else if (arg.startsWith("commandlogbinding")) {
                    String binding = args[++i];
                    if (binding.split(",").length > 1) {
                        throw new RuntimeException("Command log only supports a single set of bindings");
                    }
                    m_commandLogBinding = binding;
                    System.out.println("Commanglog binding is " + m_commandLogBinding);
                }
                else if (arg.equals("host") || arg.equals("leader")) {
                    m_leader = args[++i].trim();
                } else if (arg.startsWith("host")) {
                    m_leader = arg.substring("host ".length()).trim();
                } else if (arg.startsWith("leader")) {
                    m_leader = arg.substring("leader ".length()).trim();
                }
                // synonym for "rejoin host" for backward compatibility
                else if (arg.equals("rejoinhost")) {
                    m_startAction = StartAction.REJOIN;
                    m_leader = args[++i].trim();
                }
                else if (arg.startsWith("rejoinhost ")) {
                    m_startAction = StartAction.REJOIN;
                    m_leader = arg.substring("rejoinhost ".length()).trim();
                }

                else if (arg.equals("create")) {
                    m_startAction = StartAction.CREATE;
                } else if (arg.equals("recover")) {
                    m_startAction = StartAction.RECOVER;
                    if (   args.length > i + 1
                        && args[i+1].trim().equals("safemode")) {
                        m_startAction = StartAction.SAFE_RECOVER;
                        i += 1;
                    }
                } else if (arg.equals("rejoin")) {
                    m_startAction = StartAction.REJOIN;
                } else if (arg.startsWith("live rejoin")) {
                    m_startAction = StartAction.LIVE_REJOIN;
                } else if (arg.equals("live") && args.length > i + 1 && args[++i].trim().equals("rejoin")) {
                    m_startAction = StartAction.LIVE_REJOIN;
                } else if (arg.startsWith("join")) {
                    m_startAction = StartAction.JOIN;
                }

                else if (arg.equals("replica")) {
                    // We're starting a replica, so we must create a new database.
                    m_startAction = StartAction.CREATE;
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

                // handle behaviorless tag field
                else if (arg.equals("tag")) {
                    m_tag = args[++i];
                }
                else if (arg.startsWith("tag ")) {
                    m_tag = arg.substring("tag ".length());
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
                } else if (arg.equals("enableiv2")) {
                    m_enableIV2 = true;
                } else {
                    hostLog.fatal("Unrecognized option to VoltDB: " + arg);
                    usage();
                    System.exit(-1);
                }
            }

            // If no action is specified, issue an error.
            if (null == m_startAction) {
                if (org.voltdb.utils.MiscUtils.isPro()) {
                    hostLog.fatal("You must specify a startup action, either create, recover, replica, rejoin, or compile.");
                } else
                {
                    hostLog.fatal("You must specify a startup action, either create, recover, rejoin, or compile.");
                }
                usage();
                System.exit(-1);
            }

            // ENG-3035 Warn if 'recover' action has a catalog since we won't
            // be using it. Only cover the 'recover' action since 'start' sometimes
            // acts as 'recover' and other times as 'create'.
            if (m_startAction.doesRecover() && m_pathToCatalog != null) {
                hostLog.warn("Catalog is ignored for 'recover' action.");
            }

            /*
             * ENG-2815 If deployment is null (the user wants the default) and
             * the start action is not rejoin and leader is null, supply the
             * only valid leader value ("localhost").
             */
            if (m_leader == null && m_pathToDeployment == null &&
                !m_startAction.doesRejoin()) {
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

            if (m_startAction == null) {
                    isValid = false;
                    hostLog.fatal("The startup action is missing (either create, recover, replica or rejoin).");
                }

            if (m_startAction == StartAction.CREATE &&
                m_pathToCatalog == null) {
                isValid = false;
                hostLog.fatal("The catalog location is missing.");
            }

            if (m_leader == null) {
                isValid = false;
                hostLog.fatal("The hostname is missing.");
            }

            if (m_backend.isIPC) {
                if (m_ipcPorts.isEmpty()) {
                    isValid = false;
                    hostLog.fatal("Specified an IPC backend but did not supply a , " +
                            " separated list of ports via ipcports param");
                }
            }

            // require deployment file location
            if (m_startAction != StartAction.REJOIN && m_startAction != StartAction.LIVE_REJOIN
                    && m_startAction != StartAction.JOIN) {
                // require deployment file location (null is allowed to receive default deployment)
                if (m_pathToDeployment != null && m_pathToDeployment.isEmpty()) {
                    isValid = false;
                    hostLog.fatal("The deployment file location is empty.");
                }

                if (m_replicationRole == ReplicationRole.REPLICA) {
                    if (m_startAction.doesRecover()) {
                        isValid = false;
                        hostLog.fatal("Replica cluster only supports create database");
                    } else {
                        m_startAction = StartAction.CREATE;
                    }
                }
            } else {
                if (!m_isEnterprise && m_startAction == StartAction.LIVE_REJOIN) {
                    // pauseless rejoin is only available in pro
                    isValid = false;
                    hostLog.fatal("Live rejoin is only available in the Enterprise Edition");
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
                message = "Usage: voltdb create catalog <catalog.jar> [host <hostname>] [deployment <deployment.xml>] license <license.xml>\n"
                        + "       voltdb replica catalog <catalog.jar> [host <hostname>] [deployment <deployment.xml>] license <license.xml> \n"
                        + "       voltdb recover [host <hostname>] [deployment <deployment.xml>] license <license.xml>\n"
                        + "       voltdb [live] rejoin host <hostname>\n";
            } else {
                message = "Usage: voltdb create  catalog <catalog.jar> [host <hostname>] [deployment <deployment.xml>]\n"
                        + "       voltdb recover [host <hostname>] [deployment <deployment.xml>]\n"
                        + "       voltdb rejoin host <hostname>\n";
            }
            message += "       voltdb compile [<option> ...] [<ddl-file> ...]  (run voltdb compile -h for more details)\n";
            os.print(message);
            // Log it to log4j as well, which will capture the output to a file for (hopefully never) cases where VEM has issues (it generates command lines).
            hostLog.info(message);
            // Don't bother logging these for log4j, only dump them to the designated stream.
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

    /*
     * Create a file that starts with the supplied message that contains
     * human readable stack traces for all java threads in the current process.
     */
    public static void dropStackTrace(String message) {
        if (VoltDB.isThisATest()) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn("Declining to drop a stack trace during a junit test.");
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSSZ");
        String dateString = sdf.format(new Date());
        CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
        HostMessenger hm = VoltDB.instance().getHostMessenger();
        int hostId = 0;
        if (hm != null) {
            hostId = hm.getHostId();
        }
        String root = catalogContext != null ? catalogContext.cluster.getVoltroot() + File.separator : "";
        try {
            PrintWriter writer = new PrintWriter(root + "host" + hostId + "-" + dateString + ".txt");
            writer.println(message);
            printStackTraces(writer);
        } catch (Exception e) {
            try
            {
                VoltLogger log = new VoltLogger("HOST");
                log.error("Error while dropping stack trace for \"" + message + "\"", e);
            }
            catch (RuntimeException rt_ex)
            {
                e.printStackTrace();
            }
        }
    }

    /*
     * Print stack traces for all java threads in the current process to the supplied writer
     */
    public static void printStackTraces(PrintWriter writer) {
        printStackTraces(writer, null);
    }

    /*
     * Print stack traces for all threads in the process to the supplied writer.
     * If a List is supplied then the stack frames for the current thread will be placed in it
     */
    public static void printStackTraces(PrintWriter writer, List<String> currentStacktrace) {
        if (currentStacktrace == null) {
            currentStacktrace = new ArrayList<String>();
        }

        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        StackTraceElement[] myTrace = traces.get(Thread.currentThread());
        for (StackTraceElement ste : myTrace) {
            currentStacktrace.add(ste.toString());
        }

        writer.println();
        writer.println("****** Current Thread ****** ");
        for (String currentStackElem : currentStacktrace) {
            writer.println(currentStackElem);
        }

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
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown) {
        /*
         * InvocationTargetException suppresses information about the cause, so unwrap until
         * we get to the root cause
         */
        while (thrown instanceof InvocationTargetException) {
            thrown = ((InvocationTargetException)thrown).getCause();
        }

        // for test code
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }
        if (VoltDB.isThisATest()) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn("Declining to drop a crash file during a junit test.");
        }
        // end test code

        // try/finally block does its best to ensure death, no matter what context this
        // is called in
        try {
            // slightly less important than death, this try/finally block protects code that
            // prints a message to stdout
            try {

                // Even if the logger is null, don't stop.  We want to log the stack trace and
                // any other pertinent information to a .dmp file for crash diagnosis
                List<String> currentStacktrace = new ArrayList<String>();
                currentStacktrace.add("Stack trace from crashLocalVoltDB() method:");

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

                    if (thrown != null) {
                        writer.println();
                        writer.println("****** Exception Thread ****** ");
                        thrown.printStackTrace(writer);
                    }

                    printStackTraces(writer, currentStacktrace);
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
                    log.fatal(errMsg);
                    if (thrown != null) {
                        if (stackTrace) {
                            log.fatal("Fatal exception", thrown);
                        } else {
                            log.fatal(thrown.toString());
                        }
                    } else {
                        if (stackTrace) {
                            for (String currentStackElem : currentStacktrace) {
                                log.fatal(currentStackElem);
                            }
                        }
                    }
                } else {
                    System.err.println(errMsg);
                    if (thrown != null) {
                        if (stackTrace) {
                            thrown.printStackTrace();
                        } else {
                            System.err.println(thrown.toString());
                        }
                    } else {
                        if (stackTrace) {
                            for (String currentStackElem : currentStacktrace) {
                                System.err.println(currentStackElem);
                            }
                        }
                    }
                }
            }
            finally {
                System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
                System.err.println("The log may contain additional information.");
            }
        }
        finally {
            System.exit(-1);
        }
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
        // for test code
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }
        // end test code

        try {
            // instruct the rest of the cluster to die
            instance().getHostMessenger().sendPoisonPill(errMsg);
            // give the pill a chance to make it through the network buffer
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            // sleep even on exception in case the pill got sent before the exception
            try { Thread.sleep(500); } catch (InterruptedException e2) {}
        }
        // finally block does its best to ensure death, no matter what context this
        // is called in
        finally {
            crashLocalVoltDB(errMsg, stackTrace, t);
        }
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
