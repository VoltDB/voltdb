/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TimeZone;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.OnDemandBinaryLogger;
import org.voltcore.utils.PortGenerator;
import org.voltcore.utils.ShutdownHooks;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;

import com.google_voltpatches.common.net.HostAndPort;

/**
 * VoltDB provides main() for the VoltDB server
 */
public class VoltDB {

    /** Global constants */
    public static final int DEFAULT_PORT = 21212;
    public static final int DEFAULT_ADMIN_PORT = 21211;
    public static final int DEFAULT_INTERNAL_PORT = 3021;
    public static final int DEFAULT_ZK_PORT = 2181;
    public static final int DEFAULT_IPC_PORT = 10000;
    public static final String DEFAULT_EXTERNAL_INTERFACE = "";
    public static final String DEFAULT_INTERNAL_INTERFACE = "";
    public static final int DEFAULT_DR_PORT = 5555;
    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final int BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;
    public static final int INITIATOR_SITE_ID = 0;
    public static final int SITES_TO_HOST_DIVISOR = 100;
    public static final int MAX_SITES_PER_HOST = 128;

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

    //The GMT time zone you know and love
    public static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT+0");

    //The time zone Volt is actually using, currently always GMT
    public static final TimeZone VOLT_TIMEZONE = GMT_TIMEZONE;

    //Whatever the default timezone was for this locale before we replaced it
    public static final TimeZone REAL_DEFAULT_TIMEZONE;

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

        public int m_ipcPort = DEFAULT_IPC_PORT;

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
        public boolean m_deploymentDefault = false;

        /** name of the license file, for commercial editions */
        public String m_pathToLicense = null;

        /** false if voltdb.so shouldn't be loaded (for example if JVM is
         *  started by voltrun).
         */
        public boolean m_noLoadLibVOLTDB = false;

        public String m_zkInterface = "127.0.0.1:" + VoltDB.DEFAULT_ZK_PORT;

        /** port number for the first client interface for each server */
        public int m_port = DEFAULT_PORT;
        public String m_clientInterface = "";

        /** override for the admin port number in the deployment file */
        public int m_adminPort = -1;
        public String m_adminInterface = "";

        /** port number to use to build intra-cluster mesh */
        public int m_internalPort = DEFAULT_INTERNAL_PORT;
        public String m_internalPortInterface = DEFAULT_INTERNAL_INTERFACE;

        /** interface to listen to clients on (default: any) */
        public String m_externalInterface = DEFAULT_EXTERNAL_INTERFACE;

        /** interface to use for backchannel comm (default: any) */
        public String m_internalInterface = DEFAULT_INTERNAL_INTERFACE;

        /** port number to use for DR channel (override in the deployment file) */
        public int m_drAgentPortStart = -1;
        public String m_drInterface = "";

        /** HTTP port can't be set here, but eventually value will be reflected here */
        public int m_httpPort = Integer.MAX_VALUE;
        public String m_httpPortInterface = "";

        public String m_publicInterface = "";

        /** running the enterprise version? */
        public final boolean m_isEnterprise = org.voltdb.utils.MiscUtils.isPro();

        public int m_deadHostTimeoutMS =
            org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS * 1000;

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

        public final Queue<String> m_networkCoreBindings = new ArrayDeque<String>();
        public final Queue<String> m_computationCoreBindings = new ArrayDeque<String>();
        public final Queue<String> m_executionCoreBindings = new ArrayDeque<String>();
        public String m_commandLogBinding = null;

        /**
         * Allow a secret CLI config option to test multiple versions of VoltDB running together.
         * This is used to test online upgrade (currently, for hotfixes).
         * Also used to test error conditons like incompatible versions running together.
         */
        public String m_versionStringOverrideForTest = null;
        public String m_versionCompatibilityRegexOverrideForTest = null;

        public Configuration() {
            // Set start action create.  The cmd line validates that an action is specified, however,
            // defaulting it to create for local cluster test scripts
            m_startAction = StartAction.CREATE;
        }

        /** Behavior-less arg used to differentiate command lines from "ps" */
        public String m_tag;

        /** Force catalog upgrade even if version matches. */
        public static boolean m_forceCatalogUpgrade = false;

        public int getZKPort() {
            return MiscUtils.getPortFromHostnameColonPort(m_zkInterface, VoltDB.DEFAULT_ZK_PORT);
        }

        public Configuration(PortGenerator ports) {
            // Default iv2 configuration to the environment settings.
            // Let explicit command line override the environment.
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
                    // We used to print usage here but now we have too many ways to start
                    // VoltDB to offer help that isn't possibly quite wrong.
                    // You can probably get here using the legacy voltdb3 script. The usage
                    // is now a comment in that file.
                    System.out.println("Please refer to VoltDB documentation for command line usage.");
                    System.out.flush();
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
                    String portStr = args[++i];
                    if (portStr.indexOf(':') != -1) {
                        HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, m_port);
                        m_clientInterface = hap.getHostText();
                        m_port = hap.getPort();
                    } else {
                        m_port = Integer.parseInt(portStr);
                    }
                } else if (arg.equals("adminport")) {
                    String portStr = args[++i];
                    if (portStr.indexOf(':') != -1) {
                        HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, VoltDB.DEFAULT_ADMIN_PORT);
                        m_adminInterface = hap.getHostText();
                        m_adminPort = hap.getPort();
                    } else {
                        m_adminPort = Integer.parseInt(portStr);
                    }
                } else if (arg.equals("internalport")) {
                    String portStr = args[++i];
                    if (portStr.indexOf(':') != -1) {
                        HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, m_internalPort);
                        m_internalPortInterface = hap.getHostText();
                        m_internalPort = hap.getPort();
                    } else {
                        m_internalPort = Integer.parseInt(portStr);
                    }
                } else if (arg.equals("replicationport")) {
                    String portStr = args[++i];
                    if (portStr.indexOf(':') != -1) {
                        HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, VoltDB.DEFAULT_DR_PORT);
                        m_drInterface = hap.getHostText();
                        m_drAgentPortStart = hap.getPort();
                    } else {
                        m_drAgentPortStart = Integer.parseInt(portStr);
                    }
                } else if (arg.equals("httpport")) {
                    String portStr = args[++i];
                    if (portStr.indexOf(':') != -1) {
                        HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, VoltDB.DEFAULT_HTTP_PORT);
                        m_httpPortInterface = hap.getHostText();
                        m_httpPort = hap.getPort();
                    } else {
                        m_httpPort = Integer.parseInt(portStr);
                    }
                } else if (arg.startsWith("zkport")) {
                    //zkport should be default to loopback but for openshift needs to be specified as loopback is unavalable.
                    String portStr = args[++i];
                    if (portStr.indexOf(':') != -1) {
                        HostAndPort hap = MiscUtils.getHostAndPortFromHostnameColonPort(portStr, VoltDB.DEFAULT_ZK_PORT);
                        m_zkInterface = hap.getHostText() + ":" + hap.getPort();
                    } else {
                        m_zkInterface = "127.0.0.1:" + portStr.trim();
                    }
                } else if (arg.equals("publicinterface")) {
                    m_publicInterface = args[++i].trim();
                } else if (arg.startsWith("publicinterface ")) {
                    m_publicInterface = arg.substring("publicinterface ".length()).trim();
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
                } else if (arg.startsWith("add")) {
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
                }
                else if (arg.equals("license")) {
                    m_pathToLicense = args[++i];
                }
                else if (arg.equalsIgnoreCase("ipcport")) {
                    String portStr = args[++i];
                    m_ipcPort = Integer.valueOf(portStr);
                }
                else if (arg.equals("forcecatalogupgrade")) {
                    hostLog.info("Forced catalog upgrade will occur due to command line option.");
                    m_forceCatalogUpgrade = true;
                }
                // version string override for testing online upgrade
                else if (arg.equalsIgnoreCase("versionoverride")) {
                    m_versionStringOverrideForTest = args[++i].trim();
                    m_versionCompatibilityRegexOverrideForTest = args[++i].trim();
                }
                else {
                    hostLog.fatal("Unrecognized option to VoltDB: " + arg);
                    System.out.println("Please refer to VoltDB documentation for command line usage.");
                    System.out.flush();
                    System.exit(-1);
                }
            }

            if (!m_publicInterface.isEmpty()) {
                m_httpPortInterface = m_publicInterface;
            }

            // If no action is specified, issue an error.
            if (null == m_startAction) {
                if (org.voltdb.utils.MiscUtils.isPro()) {
                    hostLog.fatal("You must specify a startup action, either create, recover, replica, rejoin, collect, or compile.");
                } else
                {
                    hostLog.fatal("You must specify a startup action, either create, recover, rejoin, collect, or compile.");
                }
                System.out.println("Please refer to VoltDB documentation for command line usage.");
                System.out.flush();
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

            if (m_leader == null) {
                isValid = false;
                hostLog.fatal("The hostname is missing.");
            }

            // check if start action is not valid in community
            if ((!m_isEnterprise) && (m_startAction.isEnterpriseOnly())) {
                isValid = false;
                hostLog.fatal("VoltDB Community Edition only supports the \"create\" start action.");
                String msg = m_startAction.featureNameForErrorString();
                msg += " is an Enterprise Edition feature. An evaluation edition is availibale at http://voltdb.com.";
                hostLog.fatal(msg);
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
            }

            return isValid;
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

            // first try to get the "right" place to put the thing
            if (System.getenv("TEST_DIR") != null) {
                File testDir = new File(System.getenv("TEST_DIR"));
                // Create the folder as needed so that "ant junitclass" works when run before
                // testobjects is created.
                if (!testDir.exists()) {
                    boolean created = testDir.mkdirs();
                    assert(created);
                }
                // returns a full path, like a boss
                return testDir.getAbsolutePath() + File.separator + jarname;
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
            writer.flush();
            writer.close();
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

    public static void crashLocalVoltDB(String errMsg) {
        crashLocalVoltDB(errMsg, false, null);
    }

    /**
     * Exit the process with an error message, optionally with a stack trace.
     */
    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown) {
        try {
            OnDemandBinaryLogger.flush();
        } catch (Throwable e) {}

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
            ShutdownHooks.useOnlyCrashHooks();
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
