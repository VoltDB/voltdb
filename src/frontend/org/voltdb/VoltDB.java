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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.voltcore.logging.VoltLog4jLogger;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.network.LoopbackAddress;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.EstTimeUpdater;
import org.voltcore.utils.OnDemandBinaryLogger;
import org.voltcore.utils.PortGenerator;
import org.voltcore.utils.ShutdownHooks;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;
import org.voltdb.export.E3ExecutorFactoryInterface;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExporterVersion;
import org.voltdb.importer.ImportManager;
import org.voltdb.probe.MeshProber;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.settings.Settings;
import org.voltdb.settings.SettingsException;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.VoltTrace;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;

import io.netty.handler.ssl.SslContext;

/**
 * VoltDB provides main() for the VoltDB server
 */
public class VoltDB {

    /** Global constants */
    public static final int DISABLED_PORT = Constants.UNDEFINED;
    public static final int UNDEFINED = Constants.UNDEFINED;
    public static final int DEFAULT_PORT = 21212;
    public static final int DEFAULT_ADMIN_PORT = 21211;
    public static final int DEFAULT_IPC_PORT = 10000;
    public static final String DEFAULT_EXTERNAL_INTERFACE = "";
    public static final int DEFAULT_DR_PORT = 5555;
    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final int DEFAULT_HTTPS_PORT = 8443;
    public static final int DEFAULT_STATUS_PORT = 11780;
    public static final int DEFAULT_TOPICS_PORT = 9092;
    public static final int DEFAULT_ZK_PORT = org.voltcore.common.Constants.DEFAULT_ZK_PORT;
    public static final int BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;

    // Staged filenames for advanced deployments
    public static final String INITIALIZED_MARKER = ".initialized";
    public static final String TERMINUS_MARKER = ".shutdown_snapshot";
    public static final String TERMINUS_NONCE_START = "SHUTDOWN_";
    public static final String INITIALIZED_PATHS = ".paths";
    public static final String STAGED_MESH = "_MESH";
    public static final String DEFAULT_CLUSTER_NAME = "database";
    public static final String DBROOT = Constants.DBROOT;
    public static final String MODULE_CACHE = ".bundles-cache";

    // The name of the SQLStmt implied by a statement procedure's sql statement.
    public static final String ANON_STMT_NAME = "sql";

    //The GMT time zone you know and love
    public static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT+0");

    //The time zone Volt is actually using, currently always GMT
    public static final TimeZone VOLT_TIMEZONE = GMT_TIMEZONE;

    //Whatever the default timezone was for this locale before we replaced it
    public static final TimeZone REAL_DEFAULT_TIMEZONE;

    public static final String DISABLE_PLACEMENT_RESTORE = "DISABLE_PLACEMENT_RESTORE";

    // TODO: if VoltDB is running in your process, prepare to use UTC (GMT) timezone
    public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(GMT_TIMEZONE);
    }

    static {
        REAL_DEFAULT_TIMEZONE = TimeZone.getDefault();
        setDefaultTimezone();
    }

    /** Encapsulates VoltDB configuration parameters */
    public static class Configuration {
        protected static final VoltLogger hostLog = new VoltLogger("HOST");

        private boolean m_validateSuccess;

        public int m_ipcPort = DEFAULT_IPC_PORT;

        /** select normal JNI backend.
         *  IPC, Valgrind, HSQLDB, and PostgreSQL are the other options.
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

        /** ZooKeeper interface */
        public String m_zkInterface = LoopbackAddress.get();
        public int m_zkPort = DEFAULT_ZK_PORT;

        /** port number for the first client interface for each server */
        public int m_port = DEFAULT_PORT;
        public String m_clientInterface = "";

        /** override for the admin port number in the deployment file */
        public int m_adminPort = DISABLED_PORT;
        public String m_adminInterface = "";

        /** ssl context factory */
        public SslContextFactory m_sslContextFactory = null;

        /** ssl context for client and admin ports */
        public SslContext m_sslServerContext = null;
        public SslContext m_sslClientContext = null;

        /** specifies which version of exporter the system will use */
        public ExporterVersion m_exporterVersion = ExporterVersion.UNDEFINED;

        /** enable ssl */
        public boolean m_sslEnable = System.getenv("ENABLE_SSL") == null ?
                Boolean.getBoolean("ENABLE_SSL") :
                Boolean.parseBoolean(System.getenv("ENABLE_SSL"));

        /** enable ssl for external (https, client and admin port*/
        public boolean m_sslExternal = System.getenv("ENABLE_SSL") == null ?
                Boolean.getBoolean("ENABLE_SSL") :
                Boolean.parseBoolean(System.getenv("ENABLE_SSL"));

        public boolean m_sslDR = System.getenv("ENABLE_DR_SSL") == null ?
                Boolean.getBoolean("ENABLE_DR_SSL") :
                Boolean.parseBoolean(System.getenv("ENABLE_DR_SSL"));

        public boolean m_sslInternal = System.getenv("ENABLE_INTERNAL_SSL") == null ?
                Boolean.getBoolean("ENABLE_INTERNAL_SSL") :
                Boolean.parseBoolean(System.getenv("ENABLE_INTERNAL_SSL"));

        /** port number to use to build intra-cluster mesh */
        public int m_internalPort = org.voltcore.common.Constants.DEFAULT_INTERNAL_PORT;

        /** interface to listen to clients on (default: any) */
        public String m_externalInterface = DEFAULT_EXTERNAL_INTERFACE;

        /** interface to use for backchannel comm (default: any) */
        public String m_internalInterface = org.voltcore.common.Constants.DEFAULT_INTERNAL_INTERFACE;

        /** port number to use for DR channel (override in the deployment file) */
        public int m_drAgentPortStart = DISABLED_PORT;
        public String m_drInterface = "";

        /** interface and port used for consumers to connect to DR on this cluster. Used in hosted env primarily **/
        public String m_drPublicHost;
        public int m_drPublicPort = DISABLED_PORT;

        /** HTTP port can't be set here, but eventually value will be reflected here */
        public int m_httpPort = Constants.HTTP_PORT_DISABLED;
        public String m_httpPortInterface = "";

        public String m_publicInterface = "";

        /** Status monitoring interface and port */
        public int m_statusPort = DISABLED_PORT;
        public String m_statusInterface = "";

        /** running the enterprise version? */
        public final boolean m_isEnterprise = org.voltdb.utils.MiscUtils.isPro();

        public int m_deadHostTimeoutMS =
            org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS * 1000;

        public boolean m_partitionDetectionEnabled = true;

        /** start up action */
        public StartAction m_startAction = null;

        /** start mode: normal, paused*/
        public OperationMode m_startMode = OperationMode.RUNNING;

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

        public final Queue<String> m_networkCoreBindings = new ArrayDeque<>();
        public final Queue<String> m_computationCoreBindings = new ArrayDeque<>();
        public final Queue<String> m_executionCoreBindings = new ArrayDeque<>();
        public String m_commandLogBinding = null;

        /**
         * Allow a secret CLI config option to test multiple versions of VoltDB running together.
         * This is used to test online upgrade (currently, for hotfixes).
         * Also used to test error conditions like incompatible versions running together.
         */
        public String m_versionStringOverrideForTest = null;
        public String m_versionCompatibilityRegexOverrideForTest = null;
        public String m_buildStringOverrideForTest = null;

        /** Placement group */
        public String m_placementGroup = null;

        public boolean m_isPaused = false;

        /** GET option */
        public GetActionArgument m_getOption = null;
        /**
         * Name of output file in which get command will store it's result
         */
        public String m_getOutput = null;
        /**
         * Flag to indicate whether to force store the result even if there is already an existing
         * file with same name
         */
        public boolean m_forceGetCreate = false;

        private final static void referToDocAndExit(String format, Object... args) {
            String message = args.length != 0 ? String.format(format, args) : format;
            System.err.println("FATAL: " + message);
            referToDocAndExit();
        }

        private final static void referToDocAndExit() {
            System.out.println("Please refer to VoltDB documentation for command line usage.");
            System.out.flush();
            exit(-1);
        }

        /** Behavior-less arg used to differentiate command lines from "ps" */
        public String m_tag;

        public int m_queryTimeout = 0;

        /** Force catalog upgrade even if version matches. */
        public static boolean m_forceCatalogUpgrade = false;

        /** Allow starting voltdb with non-empty managed directories. */
        public boolean m_forceVoltdbCreate = false;

        /** Number of archived snapshot directories to retain if m_forceVoltdbCreate is true */
        public int m_snapArchiveRetainCount = Integer.getInteger("SNAPSHOT_DIRECTORY_ARCHIVE_LIMIT", 2);

        /** cluster name designation */
        public String m_clusterName = DEFAULT_CLUSTER_NAME;

        /** command line provided voltdbroot */
        public File m_voltdbRoot = new File(DBROOT);

        /** configuration UUID */
        public final UUID m_configUUID = UUID.randomUUID();

        /** holds a list of comma separated mesh formation coordinators */
        public String m_meshBrokers = null;

        /** holds a set of mesh formation coordinators */
        public NavigableSet<String> m_coordinators = ImmutableSortedSet.of();

        /** number of hosts that participate in a VoltDB cluster */
        public int m_hostCount = UNDEFINED;

        /** number of hosts that will be missing when the cluster is started up */
        public int m_missingHostCount = 0;

        /** not sites per host actually, number of local sites in this node */
        public int m_sitesperhost = UNDEFINED;

        /** allow elastic joins */
        public boolean m_enableAdd = false;

        /** apply safe mode strategy when recovering */
        public boolean m_safeMode = false;

        /** location of user supplied schema */
        public List<File> m_userSchemas = null;

        /** location of user supplied classes and resources jar file */
        public List<File> m_stagedClassesPaths = null;

        /** Best effort to recover previous partition layout*/
        public final boolean m_restorePlacement = !Boolean.parseBoolean(
                System.getProperty("DISABLE_PLACEMENT_RESTORE", System.getenv("DISABLE_PLACEMENT_RESTORE")));

        public String m_recoveredPartitions = "";

        public HostAndPort m_topicsHostPort = null;

        public HostAndPort m_topicsPublicHostPort = null;

        /** This is set to 'kubernetes' iff running under kubernetes, and may
            affect some operational behaviour. Other values reserved for future. */
        private final String m_voltdbContainer = System.getenv("VOLTDB_CONTAINER");
        private final String m_voltdbContainerDeployment = System.getenv("VOLTDB_K8S_CLUSTER_DEPLOYMENT");

        public boolean runningUnderKubernetes() {
            return "kubernetes".equals(m_voltdbContainer);
        }

        public int getZKPort() {
            return m_zkPort;
        }

        // Constructor used by unit tests. Attributes are
        // filled in one by one.
        public Configuration() {
            m_startAction = StartAction.CREATE; // TODO: remove legacy ops
        }

        // Constructor used by unit tests, mostly for
        // LocalCluster testing.
        public Configuration(PortGenerator ports) {
            // Default iv2 configuration to the environment settings.
            // Let explicit command line override the environment.
            m_port = ports.nextClient();
            m_adminPort = ports.nextAdmin();
            m_internalPort = ports.next();
            m_zkInterface = LoopbackAddress.get();
            m_zkPort = ports.next();
            m_coordinators = MeshProber.hosts(m_internalPort);
            m_startAction = StartAction.CREATE; // TODO: remove legacy ops
        }

        // The standard constructor: parses options from
        // a command line.
        public Configuration(String args[]) {
            /*
             *  !!! D O  N O T  U S E  hostLog  T O  L O G ,  U S E  System.[out|err]  I N S T E A D
             */
            for (int n=0; n<args.length;) {
                String arg = args[n++];

                // Some LocalCluster ProcessBuilder instances can result in an empty string
                // in the array args. Ignore them.
                if (arg.isEmpty()) {
                    continue;
                }

                // Obsolete commands. We dispose of these first so as to be able to give
                // a better error message. These are never used by the 'voltdb' CLI.
                int obsolete = 0;
                switch (arg.toLowerCase()) {
                case "create": // still used by some unit tests
                    obsolete = CoreUtils.isJunitTest() ? -1 : 1;
                    break;
                case "add":
                case "live": // live rejoin
                case "recover":
                case "rejoin":
                case "rejoinhost":
                case "replica":
                    obsolete = 1;
                    break;
                }
                if (obsolete != 0) {
                    String msg = String.format("Obsolete command option '%s', %s a unit test%n", arg,
                                               CoreUtils.isJunitTest() ? "in" : "not in");
                    if (obsolete > 0) {
                        System.err.println("FATAL: " + msg);
                        exit(-1);
                    }
                    System.out.println(msg);
                }

                // Options without values.
                // Alphabetical order please!
                boolean handled = true; // assumed
                switch (arg.toLowerCase()) {
                case "-h":
                case "--help":
                    referToDocAndExit();
                    break;
                case "create": // obsolete but used in some unit tests
                    m_startAction = StartAction.CREATE;
                    break;
                case "drssl":
                    m_sslDR = true;
                    break;
                case "enableadd":
                    m_enableAdd = true;
                    break;
                case "enablessl":
                    m_sslEnable = true;
                    break;
                case "externalssl":
                    m_sslExternal = true;
                    break;
                case "e2":
                    m_exporterVersion = ExporterVersion.E2;
                    break;
                case "e3":
                    m_exporterVersion = ExporterVersion.E3;
                    break;
                case "force":
                    m_forceVoltdbCreate = true;
                    break;
                case "forcecatalogupgrade":
                    m_forceCatalogUpgrade = true;
                    break;
                case "forceget":
                    m_forceGetCreate = true;
                    break;
                case "hsqldb":
                    m_backend = BackendTarget.HSQLDB_BACKEND;
                    break;
                case "initialize":
                    m_startAction = StartAction.INITIALIZE;
                    break;
                case "internalssl":
                    m_sslInternal = true;
                    break;
                case "ipc":
                    m_backend = BackendTarget.NATIVE_EE_IPC;
                    break;
                case "jni":
                    m_backend = BackendTarget.NATIVE_EE_JNI;
                    break;
                case "noadd":
                    m_enableAdd = false;
                    break;
                case "noloadlib":
                    m_noLoadLibVOLTDB = true;
                    break;
                case "paused":
                    m_isPaused = true;
                    break;
                case "postgis":
                    m_backend = BackendTarget.POSTGIS_BACKEND;
                    break;
                case "postgresql":
                    m_backend = BackendTarget.POSTGRESQL_BACKEND;
                    break;
                case "probe":
                    m_startAction = StartAction.PROBE;
                    break;
                case "quietadhoc":
                    m_quietAdhoc = true;
                    break;
                case "safemode":
                    m_safeMode = true;
                    break;
                case "valgrind":
                    m_backend = BackendTarget.NATIVE_EE_VALGRIND_IPC;
                    break;
                default:
                    handled = false;
                    break;
                }
                if (handled) {
                    continue;
                }

                // Options with at least one value, generally only one value, but
                // there are a couple of options that may have more than one value.
                if (n >= args.length) {
                    referToDocAndExit("The \"%s\" option must be followed by a value.", arg);
                }
                String val = args[n++];
                handled = true; // assumed
                HostAndPort hap = null; // scratch

                // Alphabetical order please!
                switch (arg.toLowerCase()) {
                case "adminport":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, m_adminInterface, DEFAULT_ADMIN_PORT);
                    m_adminInterface = hap.getHost();
                    m_adminPort = hap.getPort();
                    break;
                case "buildstringoverride":
                    m_buildStringOverrideForTest = val;
                    break;
                case "catalog":
                    m_pathToCatalog = val;
                    break;
                case "classes":
                    m_stagedClassesPaths = parseFiles(val, m_stagedClassesPaths, "classes jar");
                    break;
                case "commandlogbinding":
                    if (val.split(",").length > 1) {
                        throw new RuntimeException("Command log only supports a single set of bindings");
                    }
                    m_commandLogBinding = val;
                    System.out.println("Commandlog binding is " + m_commandLogBinding);
                    break;
                case "computationbindings":
                    parseBindings(val, m_computationCoreBindings, "Computation");
                    break;
                case "deployment":
                    m_pathToDeployment = val;
                    break;
                case "dragentportstart":
                    m_drAgentPortStart = Integer.parseInt(val);
                    break;
                case "drpublic":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, m_drPublicHost, DEFAULT_DR_PORT);
                    m_drPublicHost = hap.getHost();
                    m_drPublicPort = hap.getPort();
                    break;
                case "executionbindings":
                    parseBindings(val,  m_executionCoreBindings, "Execution");
                    break;
                case "externalinterface":
                    m_externalInterface = MiscUtils.getAddressOfInterface(val);
                    break;
                case "file":
                    m_getOutput = val;
                    break;
                case "get":
                    m_startAction = StartAction.GET;
                    if (val.isEmpty()) {
                        referToDocAndExit("Supply a valid non-null argument for \"get\" command."
                                          + " Supported arguments for get are: %s", GetActionArgument.supportedVerbs());
                    }
                    try {
                        m_getOption = GetActionArgument.valueOf(val.toUpperCase());
                    } catch (IllegalArgumentException ex) {
                        referToDocAndExit("%s is not a valid \"get\" command argument."
                                          + " Valid arguments for get command are: %s", val, GetActionArgument.supportedVerbs());
                    }
                    m_getOutput = m_getOption.getDefaultOutput();
                    break;
                case "getvoltdbroot":
                    m_voltdbRoot = new File(val);
                    if (!DBROOT.equals(m_voltdbRoot.getName())) {
                        m_voltdbRoot = new File(m_voltdbRoot, DBROOT);
                    }
                    if (!m_voltdbRoot.exists()) {
                        referToDocAndExit("%s does not contain a  valid database root directory."
                                          + " Use the --dir option to specify the path to the root.",
                                          m_voltdbRoot.getParentFile().getAbsolutePath());
                    }
                    break;
                case "host":
                    m_leader = val;
                    break;
                case "hostcount":
                    m_hostCount = Integer.parseInt(val);
                    break;
                case "httpport":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, m_httpPortInterface, DEFAULT_HTTP_PORT);
                    m_httpPortInterface = hap.getHost();
                    m_httpPort = hap.getPort();
                    break;
                case "internalinterface":
                    m_internalInterface = MiscUtils.getAddressOfInterface(val);
                    break;
                case "internalport":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, m_internalInterface, m_internalPort);
                    m_internalInterface = hap.getHost();
                    m_internalPort = hap.getPort();
                    break;
                case "ipcport":
                    m_ipcPort = Integer.valueOf(val);
                    break;
                case "leader":
                    m_leader = val;
                    break;
                case "license":
                    m_pathToLicense = val;
                    break;
                case "mesh":
                    StringBuilder sbld = new StringBuilder(64);
                    sbld.append(val); // may be empty
                    while (!val.isEmpty() && n < args.length && (val.endsWith(",") || args[n].startsWith(","))) {
                        val = args[n++];
                        sbld.append(val);
                    }
                    m_meshBrokers = sbld.toString();
                    break;
                 case "missing":
                    m_missingHostCount = Integer.parseInt(val);
                    break;
                case "networkbindings":
                    parseBindings(val, m_networkCoreBindings, "Network");
                    break;
                case "placementgroup":
                    m_placementGroup = val;
                    break;
                case "port":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, m_clientInterface, m_port);
                    m_clientInterface = hap.getHost();
                    m_port = hap.getPort();
                    break;
                case "publicinterface":
                    m_publicInterface = MiscUtils.getAddressOfInterface(val);
                    break;
                case "replicationport":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, m_drInterface, DEFAULT_DR_PORT);
                    m_drInterface = hap.getHost();
                    m_drAgentPortStart = hap.getPort();
                    break;
                case "retain":
                    m_snapArchiveRetainCount = Integer.parseInt(val);
                    break;
                case "schema":
                    m_userSchemas = parseFiles(val, m_userSchemas, "schema");
                    break;
                case "statusport":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, m_statusInterface, DEFAULT_STATUS_PORT);
                    m_statusInterface = hap.getHost();
                    m_statusPort = hap.getPort();
                    break;
                case "tag": // behaviorless tag field
                    m_tag = val;
                    break;
                case "timestampsalt":
                    m_timestampTestingSalt = Long.parseLong(val);
                    break;
                case "topicshostport":
                    // Listener names and port numbers must be unique.
                    m_topicsHostPort = MiscUtils.getHostAndPortFromInterfaceSpec(val, "", DEFAULT_TOPICS_PORT);
                    break;
                case "topicspublic":
                    m_topicsPublicHostPort = MiscUtils.getHostAndPortFromHostnameColonPort(val, DEFAULT_TOPICS_PORT);
                    break;
                case "versionoverride": // version string override for testing online upgrade
                    m_versionStringOverrideForTest = val;
                    m_versionCompatibilityRegexOverrideForTest = (n < args.length ? args[n++] : val);
                    break;
                case "voltdbroot":
                    m_voltdbRoot = new File(val);
                    if (!DBROOT.equals(m_voltdbRoot.getName())) {
                        m_voltdbRoot = new File(m_voltdbRoot, DBROOT);
                    }
                    if (!m_voltdbRoot.exists() && !m_voltdbRoot.mkdirs()) {  // TODO: really create this here?
                        referToDocAndExit("Could not create directory \"%s\"", m_voltdbRoot.getPath());
                    }
                    try {
                        CatalogUtil.validateDirectory(DBROOT, m_voltdbRoot);
                    } catch (RuntimeException e) {
                        referToDocAndExit(e.getMessage());
                    }
                    break;
                case "zkport":
                    hap = MiscUtils.getHostAndPortFromInterfaceSpec(val, LoopbackAddress.get(), DEFAULT_ZK_PORT);
                    m_zkInterface = hap.getHost();
                    m_zkPort = hap.getPort();
                    break;
                default:
                    handled = false;
                }

                if (handled) {
                    continue;
                }

                if (arg.indexOf(' ') > 0) { // hint that combining args is no longer allowed
                    System.err.printf("Option \"%s\" should be split into separate arguments.%n", arg);
                }
                referToDocAndExit("Unrecognized option to VoltDB: %s", arg);
            }

            // If no action is specified, issue an error. This can only happen with
            // test code, since the supported CLI will always supply a valid action.
            if (m_startAction == null) {
                referToDocAndExit("You must specify a startup action, one of initialize, probe, get");
            }

            // The 'get' command gets out of the way early
            if (m_startAction == StartAction.GET) {
                VoltDB.exitAfterMessage = true;
                inspectGetCommand();
                return;
            }

            /*
             *  !!! F R O M  T H I S  P O I N T  O N  Y O U  M A Y  U S E  hostLog  T O  L O G
             */
            VoltLog4jLogger.setFileLoggerRoot(m_voltdbRoot);
            if (m_forceCatalogUpgrade) {
                String msg = "Forced catalog upgrade will occur due to command line option.";
                System.out.println(msg);
                hostLog.info(msg);
            }

            // If the leader is null and the deployment is null (the user wants
            // the default), supply the only valid leader value ("localhost").
            if (m_leader == null && m_pathToDeployment == null) {
                m_leader = "localhost";
            }

            if (m_startAction == StartAction.PROBE) {
                checkInitializationMarker();
            } else if (m_startAction == StartAction.INITIALIZE) {
                if (isInitialized() && !m_forceVoltdbCreate) {
                    hostLog.fatalFmt("%s is already initialized.", m_voltdbRoot);
                    referToDocAndExit("Use the start command to start the initialized database,\n" +
                                      "or use init --force to overwrite existing files.");
                }
            } else if (m_startAction == StartAction.CREATE) {
                if (m_meshBrokers == null || m_meshBrokers.trim().isEmpty()) {
                    if (m_leader != null) {
                        m_meshBrokers = m_leader;
                    }
                }
            }
            else {
                hostLog.fatalFmt("Internal error, unexpected start action %s", m_startAction);
                referToDocAndExit();
            }

            if (m_meshBrokers != null) {
                m_coordinators = MeshProber.hosts(m_meshBrokers);
                if (m_leader == null) {
                    m_leader = m_coordinators.first();
                }
            }
            if (m_startAction == StartAction.PROBE && m_hostCount == UNDEFINED && m_coordinators.size() > 1) {
                m_hostCount = m_coordinators.size();
            }
        }

        private List<File> parseFiles(String pathList, List<File> fileList, String what) {
            for (String path : Splitter.on(',').trimResults().omitEmptyStrings().split(pathList)) {
                File file = checkFile(path, what);
                if (fileList == null) {
                    fileList = new ArrayList<>();
                }
                fileList.add(file);
            }
            return fileList;
        }

        private File checkFile(String path, String what) {
            File file = new File(path);
            if (!file.exists()) {
                referToDocAndExit("Supplied %s file %s does not exist", what, path);
            }
            if (!file.canRead()) {
                referToDocAndExit("Supplied %s file %s can't be read.", what, path);
            }
            if (!file.isFile()) {
                referToDocAndExit("Supplied %s file %s is not an ordinary file.", what, path);
            }
            return file;
        }

        private void parseBindings(String bindingList, Queue<String> outQueue, String what) {
            for (String binding : Splitter.on(',').trimResults().omitEmptyStrings().split(bindingList)) {
                outQueue.offer(binding);
            }
            System.out.printf("%s bindings are %s %n", what, outQueue);
        }

        private boolean isInitialized() {
            File inzFH = new File(m_voltdbRoot, VoltDB.INITIALIZED_MARKER);
            return inzFH.exists() && inzFH.isFile() && inzFH.canRead();
        }

        private void inspectGetCommand() {
            String parentPath = m_voltdbRoot.getParent();
            // check voltdbroot
            if (!m_voltdbRoot.exists()) {
                try {
                    parentPath = m_voltdbRoot.getCanonicalFile().getParent();
                } catch (IOException ignored) {
                }
                referToDocAndExit("%s does not contain a valid database root directory."
                                  + " Use the --dir option to specify the path to the root.",
                                  parentPath);
            }
            File configInfoDir = new File(m_voltdbRoot, Constants.CONFIG_DIR);
            switch (m_getOption) {
                case DEPLOYMENT: {
                    File depFH = new File(configInfoDir, "deployment.xml");
                    if (!depFH.exists()) {
                        referToDocAndExit("Deployment file \"%s\" not found", depFH.getAbsolutePath());
                    }
                    m_pathToDeployment = depFH.getAbsolutePath();
                    return;
                }
                case SCHEMA:
                case CLASSES: {
                    // catalog.jar contains DDL and proc classes with which the database was
                    // compiled. Check if catalog.jar exists as it is needed to fetch ddl (get
                    // schema) as well as procedures (get classes)
                    File catalogFH = new File(configInfoDir, CatalogUtil.CATALOG_FILE_NAME);
                    if (!catalogFH.exists()) {
                        System.err.printf("Catalog file \"%s\" not found.%n", catalogFH.getAbsolutePath());
                        try {
                            parentPath = m_voltdbRoot.getCanonicalFile().getParent();
                        } catch (IOException ignored) {
                        }
                        referToDocAndExit("%s not found in the provided database directory %s."
                                          + " Make sure the database has been started.",
                                          m_getOption.name().toUpperCase(), parentPath);
                    }
                    m_pathToCatalog = catalogFH.getAbsolutePath();
                    return;
                }
                case LICENSE: {
                    if(!m_isEnterprise) {
                        referToDocAndExit("Community Edition of VoltDB does not have license files");
                    }
                    File licFH = new File(m_voltdbRoot, "license.xml");
                    m_pathToLicense = licFH.getAbsolutePath();
                    if (!licFH.exists()) {
                        referToDocAndExit("License file \"%s\" not found.", m_pathToLicense);
                    }
                    return;
                }
            }
        }

        public Map<String,String> asClusterSettingsMap() {
            Settings.initialize(m_voltdbRoot);
            return ImmutableMap.<String, String>builder()
                    .put(ClusterSettings.HOST_COUNT, Integer.toString(m_hostCount))
                    .put(ClusterSettings.PARTITITON_IDS, m_recoveredPartitions)
                    .build();
        }

        public Map<String,String> asPathSettingsMap() {
            Settings.initialize(m_voltdbRoot);
            return ImmutableMap.<String, String>builder()
                    .put(NodeSettings.VOLTDBROOT_PATH_KEY, m_voltdbRoot.getPath())
                    .build();
        }

        public Map<String,String> asRelativePathSettingsMap() {
            Settings.initialize(m_voltdbRoot);
            File currDir;
            File voltdbroot;
            try {
                currDir = new File("").getCanonicalFile();
                voltdbroot = m_voltdbRoot.getCanonicalFile();
            } catch (IOException e) {
                throw new SettingsException("Failed to relativize voltdbroot " +
                        m_voltdbRoot.getPath() + ". Reason: " + e.getMessage());
            }
            String relativePath = currDir.toPath().relativize(voltdbroot.toPath()).toString();
            return ImmutableMap.<String, String>builder()
                    .put(NodeSettings.VOLTDBROOT_PATH_KEY, relativePath)
                    .build();
        }

        public Map<String, String> asNodeSettingsMap() {
            return ImmutableMap.<String, String>builder()
                    .put(NodeSettings.LOCAL_SITES_COUNT_KEY, Integer.toString(m_sitesperhost))
                    .put(NodeSettings.LOCAL_ACTIVE_SITES_COUNT_KEY, Integer.toString(m_sitesperhost))
                    .build();
        }

        public ClusterSettings asClusterSettings() {
            return ClusterSettings.create(asClusterSettingsMap());
        }

        List<File> getInitMarkers() {
            return ImmutableList.<File>builder()
                    .add(new File(m_voltdbRoot, VoltDB.INITIALIZED_MARKER))
                    .add(new File(m_voltdbRoot, VoltDB.INITIALIZED_PATHS))
                    .add(new File(m_voltdbRoot, Constants.CONFIG_DIR))
                    .add(new File(m_voltdbRoot, VoltDB.STAGED_MESH))
                    .add(new File(m_voltdbRoot, VoltDB.TERMINUS_MARKER))
                    .build();
        }

        /**
         * Checks for the initialization marker on initialized voltdbroot directory
         */
        private void checkInitializationMarker() {
            File inzFH = new File(m_voltdbRoot, VoltDB.INITIALIZED_MARKER);
            File deploymentFH;
            if (runningUnderKubernetes() && StringUtils.isNotEmpty(m_voltdbContainerDeployment)) {
                hostLog.info("Running in kubernetes environment, using deployment from: " + m_voltdbContainerDeployment);
                deploymentFH = new File(m_voltdbContainerDeployment);
            } else {
                deploymentFH = new File(new File(m_voltdbRoot, Constants.CONFIG_DIR), "deployment.xml");
            }
            File configCFH = null;
            File optCFH = null;

            if (m_pathToDeployment != null && !m_pathToDeployment.trim().isEmpty()) {
                try {
                    configCFH = deploymentFH.getCanonicalFile();
                } catch (IOException e) {
                    hostLog.fatal("Could not resolve file location " + deploymentFH, e);
                    referToDocAndExit();
                }
                try {
                    optCFH = new File(m_pathToDeployment).getCanonicalFile();
                } catch (IOException e) {
                    hostLog.fatal("Could not resolve file location " + optCFH, e);
                    referToDocAndExit();
                }
                if (!configCFH.equals(optCFH)) {
                    hostLog.fatal("In startup mode you may only specify " + deploymentFH + " for deployment, you specified: " + optCFH);
                    referToDocAndExit();
                }
            } else {
                m_pathToDeployment = deploymentFH.getPath();
            }

            if (!inzFH.exists() || !inzFH.isFile() || !inzFH.canRead()) {
                hostLog.fatalFmt("Specified directory '%s' is not a VoltDB initialized root",
                                 m_voltdbRoot.getAbsolutePath());
                referToDocAndExit();
            }

            String stagedName = null;
            try (BufferedReader br = new BufferedReader(new FileReader(inzFH))) {
                stagedName = br.readLine();
            } catch (IOException e) {
                hostLog.fatal("Unable to access initialization marker at " + inzFH, e);
                referToDocAndExit();
            }

            if (m_clusterName != null && !m_clusterName.equals(stagedName)) {
                hostLog.fatal("The database root directory has changed. Either initialization did not complete properly or the directory has been corrupted. You must reinitialize the database directory before using it.");
                referToDocAndExit();
            } else {
                m_clusterName = stagedName;
            }
            try {
                if (m_meshBrokers == null || m_meshBrokers.trim().isEmpty()) {
                    File meshFH = new File(m_voltdbRoot, VoltDB.STAGED_MESH);
                    if (meshFH.exists() && meshFH.isFile() && meshFH.canRead()) {
                        try (BufferedReader br = new BufferedReader(new FileReader(meshFH))) {
                            m_meshBrokers = br.readLine();
                        } catch (IOException e) {
                            hostLog.fatal("Unable to read cluster name given at initialization from " + inzFH, e);
                            referToDocAndExit();
                        }
                    }
                }
            } catch (IllegalArgumentException e) {
                hostLog.fatal("Unable to validate mesh argument \"" + m_meshBrokers + "\"", e);
                referToDocAndExit();
            }
        }

        private void generateFatalLog(String fatalMsg) {
            if (m_validateSuccess) {
                m_validateSuccess = false;
                StringBuilder sb = new StringBuilder(2048).append("Command line arguments: ");
                sb.append(System.getProperty("sun.java.command", "[not available]"));
                hostLog.info(sb.toString());
            }
            hostLog.fatal(fatalMsg);
        }


        /**
         * Validates configuration settings and logs errors to the host log.
         * You typically want to have the system exit when this fails, but
         * this functionality is left outside of the method so that it is testable.
         *
         * Validation is partial. Mostly we just ignore options that make
         * no sense with the specified startup action.
         *
         * May be called from test code without having executed the command-
         * line parser, so there are some duplicated checks.
         *
         * Where the official CLI (the 'voltdb' program) has been used, the only
         * possible start actions are INITIALIZE, PROBE, and GET.
         *
         * @return Returns true if all required configuration settings are present.
         */
        public boolean validate() {
            m_validateSuccess = true; // this must be set before calling generateFatalLog

            if (m_startAction == null) {
                generateFatalLog("The startup action is missing (one of INITIALIZE, PROBE, GET)");
                return m_validateSuccess; // no point in carrying on
            }

            if (m_startAction.isLegacy()) {
                switch (m_startAction) {
                case CREATE:
                case RECOVER:
                case SAFE_RECOVER:
                    break;
                default:
                    generateFatalLog("Unsupported legacy start action " + m_startAction);
                    return m_validateSuccess; // further checks irrelevant
                }
            }

            if (!m_isEnterprise && m_startAction.isEnterpriseOnly()) {
                generateFatalLog("VoltDB Community Edition does not support startup action " + m_startAction);
            }

            EnumSet<StartAction> hostNotRequired = EnumSet.of(StartAction.INITIALIZE, StartAction.GET);
            if (!hostNotRequired.contains(m_startAction)) {
                if (m_leader == null) {
                    generateFatalLog("The hostname is missing");
                }
                if (m_coordinators.isEmpty()) {
                    generateFatalLog("List of hosts is missing");
                }
            }

            EnumSet<StartAction> deploymentNotRequired = EnumSet.of(StartAction.INITIALIZE, StartAction.PROBE);
            if (!deploymentNotRequired.contains(m_startAction) && m_pathToDeployment != null && m_pathToDeployment.trim().isEmpty()) {
                generateFatalLog("The deployment file location is empty");
            }

            if (m_hostCount != UNDEFINED) {
                if (m_startAction == StartAction.PROBE) {
                    if (m_hostCount < 0) {
                        generateFatalLog("\"--count\" may not be specified with negative values");
                    }
                    if (m_hostCount < m_coordinators.size()) {
                        generateFatalLog("List of hosts is greater than option \"--count\"");
                    }
                }
                else {
                    generateFatalLog("Option \"--count\" may only be specified when using the \"start\" command");
                }
            }

            return m_validateSuccess;
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
            if (jarname == null) {
                return null; // NewCLI tests that init with schema do not want a pre-compiled catalog
            }

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
            if (buildMode == null) {
                buildMode = "release";
            }
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

        public int getQueryTimeout() {
           return singleton.s_config.m_queryTimeout;
        }
    }

    /* helper functions to access current configuration values */
    public static boolean getLoadLibVOLTDB() {
        return ! singleton.s_config.m_noLoadLibVOLTDB;
    }

    public static BackendTarget getEEBackendType() {
        return singleton.s_config.m_backend;
    }

    /*
     * Create a file that starts with the supplied message that contains
     * human readable stack traces for all java threads in the current process.
     */
    public static void dropStackTrace(String message) {
        if (CoreUtils.isJunitTest()) {
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
        String root = catalogContext != null ? VoltDB.instance().getVoltDBRootPath() + File.separator : "";
        try {
            PrintWriter writer = new PrintWriter(root + "host" + hostId + "-" + dateString + "-log.txt");
            writer.println(message);
            printStackTraces(writer, null);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            try {
                VoltLogger log = new VoltLogger("HOST");
                log.error("Error while dropping stack trace for \"" + message + "\"", e);
            } catch (RuntimeException rt_ex) {
                e.printStackTrace();
            }
        }
    }

    /*
     * Print stack traces for all threads in the process to the supplied writer.
     * If a List is supplied then the stack frames for the current thread will be placed
     * in it, as well as written to the writer.
     */
    private static void printStackTraces(PrintWriter writer, List<String> currentStackTrace) {
        if (currentStackTrace == null) {
            currentStackTrace = new ArrayList<>();
        }
        getStackTraceAsList(currentStackTrace);

        writer.println();
        writer.println("****** Current Thread ****** ");
        for (String currentStackElem : currentStackTrace) {
            writer.println(currentStackElem);
        }

        writer.println();
        writer.println("****** All Threads ******");
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        for (ThreadInfo t : threadInfos) {
            writer.println(t);
        }
    }

    /*
     * Collect stack trace for current thread and append to supplied list
     */
    private static void getStackTraceAsList(List<String> currentStackTrace) {
        StackTraceElement[] myTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement ste : myTrace) {
            currentStackTrace.add(ste.toString());
        }
    }

    /*
     * turn off client interface as fast as possible
     */
    private static boolean turnOffClientInterface() {
        // we don't expect this to ever fail, but if it does, skip to dying immediately
        VoltDBInterface vdbInstance = instance();
        if (vdbInstance != null) {
            ClientInterface ci = vdbInstance.getClientInterface();
            if (ci != null) {
                if (!ci.ceaseAllPublicFacingTrafficImmediately()) {
                    return false;
                }
            }
        }
        return true;
    }

    /*
     * send a SNMP trap crash notification
     */
    private static void sendCrashSNMPTrap(String msg) {
        if (msg == null || msg.trim().isEmpty()) {
            return;
        }
        VoltDBInterface vdbInstance = instance();
        if (vdbInstance == null) {
            return;
        }
        SnmpTrapSender snmp = vdbInstance.getSnmpTrapSender();
        if (snmp == null) {
            return;
        }
        snmp.crash(msg);
    }

    // Set non-null (as reason) to decline crash file
    public static String declineCrashFile;

    /**
     * Exit the process with an error message, generating a file containing
     * crash details (unless declineCrashFile set) and optionally logs the crash message.
     *
     * See comments for exitAfterMessage and ignoreCrash for modifiers;
     * but these are not applicable to the majority of cases in VoltDB.
     * Similarly, there is an implicit flag set when we are running a
     * junit test. Subject to none of those flags being set, a crash
     * file is always created by crashLocalVoltDB().
     *
     * At the time of writing, there is only one case (outside this file)
     * of calling the four-argument overload of this method.
     *
     * @param errMsg message to print, log, and/or include in trap message
     * @param stackTrace if true, stack traces logged as well as in crash file
     * @param thrown optional cause of crash, details added to crash file
     * @param logFatal if true, errMsg written to host log as well as crash file
     */
    public static void crashLocalVoltDB(String errMsg) {
        crashLocalVoltDB(errMsg, false/*no stack*/, null, true/*log fatal msg*/);
    }

    public static void crashLocalVoltDB(String errMsg, boolean stackTrace) {
        crashLocalVoltDB(errMsg, stackTrace, null, true/*log fatal msg*/);
    }

    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown) {
        crashLocalVoltDB(errMsg, stackTrace, thrown, true/*log fatal msg*/);
    }

    public static void crashLocalVoltDB(String errMsg, boolean stackTrace, Throwable thrown, boolean logFatal) {
        if (singleton != null) {
            singleton.s_voltdb.notifyOfShutdown();
        }

        if (exitAfterMessage) {
            System.err.println(errMsg);
            VoltDB.exit(-1);
        }

        try {
            OnDemandBinaryLogger.flush();
        } catch (Throwable ignored) {}

        // InvocationTargetException suppresses information about the cause,
        // so unwrap until we get to the root cause
        while (thrown instanceof InvocationTargetException) {
            thrown = thrown.getCause();
        }

        // for test code
        wasCrashCalled = true;
        crashMessage = errMsg;
        if (ignoreCrash) {
            throw new AssertionError("Faux crash of VoltDB successful.");
        }
        if (CoreUtils.isJunitTest()) {
            declineCrashFile = "Declining to drop a crash file during a junit test.";
        }
        // end test code

        // send a snmp trap crash notification
        sendCrashSNMPTrap(errMsg);

        // try/finally block does its best to ensure death, no matter what context this
        // is called in
        try {
            // slightly less important than death, this try/finally block protects code that
            // prints a message to stdout
            try {
                // turn off client interface as fast as possible
                // we don't expect this to ever fail, but if it does, skip to dying immediately
                if (!turnOffClientInterface()) {
                    return; // this will jump to the finally block and die faster
                }

                // Flush trace files
                try {
                    VoltTrace.closeAllAndShutdown(new File(instance().getVoltDBRootPath(), "trace_logs").getAbsolutePath(),
                                                  TimeUnit.SECONDS.toMillis(10));
                } catch (Exception ignored) {}

                // Try for a logger; but we can live without it
                VoltLogger log = null;
                try {
                    log = new VoltLogger("HOST");
                } catch (RuntimeException ignored) { }

                // Write out a crash file (voltdb_crash_...txt)
                // The content is not affected by the 'stackTrace' flag input argument to the
                // crashLocalVoltDB() method. which only controls what is written to the host log.
                List<String> currentStackTrace = new ArrayList<>();
                if (declineCrashFile == null) {
                    currentStackTrace.add("Stack trace from crashLocalVoltDB() method:");
                    writeCrashFile(errMsg, thrown, currentStackTrace);
                } else {
                    if (stackTrace && thrown == null) {
                        getStackTraceAsList(currentStackTrace);
                    }
                    if (log != null) {
                        log.info(declineCrashFile);
                    }
               }

                // Attempt writing to host log.
                // - Logs fatal error message if requested via logFatal argument
                // - If stackTrace requested:
                //   Logs exception cause and exception stack, if 'thrown' non-null
                //   Else logs the current thread stack
                // - If stackTrace not requested:
                // - Logs only the exception cause if given
                // If writing to host log is not possible, the same information
                // will be written to standard error instead.
                if (log != null) {
                    if (logFatal) {
                        log.fatal(errMsg);
                    }
                    if (thrown != null) {
                        if (stackTrace) {
                            log.fatal("Fatal exception", thrown);
                        } else {
                            log.fatal(thrown.toString());
                        }
                    } else if (stackTrace) {
                        for (String currentStackElem : currentStackTrace) {
                            log.fatal(currentStackElem);
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
                    } else if (stackTrace) {
                        for (String currentStackElem : currentStackTrace) {
                            System.err.println(currentStackElem);
                        }
                    }
                }
            } finally {
                System.err.println("VoltDB has encountered an unrecoverable error and is exiting.");
                System.err.println("The log may contain additional information.");
            }
        } finally {
            ShutdownHooks.useOnlyCrashHooks();
            System.exit(-1);
        }
    }

    /*
     * Create a crash file to record crash details, including exception stack trace
     * if 'thrown' was given to us as a cause, and stack traces for all threads.
     *
     * The current stack trace is appended to the 'currentStackTrace' argument
     * by the call to printStackTraces().
     */
    private static void writeCrashFile(String errMsg, Throwable thrown, List<String> currentStackTrace) {
        try {
            TimestampType ts = new TimestampType(new java.util.Date());
            CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
            String root = catalogContext != null ? VoltDB.instance().getVoltDBRootPath() + File.separator : "";
            PrintWriter writer = new PrintWriter(root + "voltdb_crash" + ts.toString().replace(' ', '-') + ".txt");
            writer.println("Time: " + ts);
            writer.println("Message: " + errMsg);

            writer.println();
            writer.println("Platform Properties:");
            PlatformProperties pp = PlatformProperties.getPlatformProperties();
            String[] lines = pp.toLogLines(instance().getVersionString()).split("\n");
            for (String line : lines) {
                writer.println(line.trim());
            }

            if (thrown != null) {
                writer.println();
                writer.println("****** Exception Thread ****** ");
                thrown.printStackTrace(writer);
            }

            printStackTraces(writer, currentStackTrace);
            writer.close();
        } catch (Throwable err) {
            // shouldn't fail, but..
            err.printStackTrace();
        }
    }

    /*
     * For testing only: if set, throws an assertion error
     * instead of crashing. The local crash routine first
     * notifies the VoltDB instance of shutdown, and flushes
     * binary logging. The global crash routine does nothing.
     * In both cases the 'crash record' variables are then
     * set and the error is thrown.
     */
    public static boolean ignoreCrash = false;

    /*
     * If true, then what happens depends on whether we're doing
     * a local crash or a global crash.
     * - Local: notify the VoltDB instance of shutdown, print
     *   a message on stderr, and exit. The 'crash record' below
     *   will not be set, nor will ignoreCrash be used.
     * - Global: if ignoreCrash is set, the 'crash record' is set,
     *   and an assertion error is thrown. Otherwise (normal case)
     *   cluster crash is triggered, and then we execute the local
     *   node crash routine as described in previous paragraph.
     * This is really just intended for local CLI operations, in
     * which case the global-crash scenario is not applicable.
     */
    public static boolean exitAfterMessage = false;

    /*
     * Crash record: true when crash call, and the (most recent)
     * crash message text. Mostly of use for test code.
     */
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

        // send a snmp trap crash notification
        sendCrashSNMPTrap(errMsg);
        try {
            // turn off client interface as fast as possible
            // we don't expect this to ever fail, but if it does, skip to dying immediately
            if (!turnOffClientInterface()) {
                return; // this will jump to the finally block and die faster
            }
            // instruct the rest of the cluster to die
            instance().getHostMessenger().sendPoisonPill(errMsg);
            // give the pill a chance to make it through the network buffer
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            // sleep even on exception in case the pill got sent before the exception
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {}
        }
        // finally block does its best to ensure death, no matter what context this
        // is called in
        finally {
            crashLocalVoltDB(errMsg, stackTrace, t);
        }
    }

    /**
     * Entry point for the VoltDB server process. Command line
     * specifies a startup action (like 'initialize' or 'probe')
     * and numerous parameters for the server.
     *
     * @param args Requires catalog and deployment file locations.
     */
    public static void main(String[] args) {

        try {
            Configuration config = new Configuration(args);
            if (!config.validate()) {
                System.exit(-1);
            } else if (config.m_startAction == StartAction.GET) {
                cli(config);
            } else {
                if (config.m_startAction != StartAction.INITIALIZE) {
                    // we dont need Est time updater and DNS cache for INITIALIZE
                    ClientFactory.preserveResources();
                }
                initialize(config, false);
                instance().run();
            }
        } catch (OutOfMemoryError e) {
            String errmsg = "VoltDB Main thread: ran out of Java memory. This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, false, e);
        }
    }

    /**
     * Initialize the VoltDB server.
     * @param config  The VoltDB.Configuration to use to initialize the server.
     */
    public static void initialize(VoltDB.Configuration config, boolean fromServerThread) {
        singleton.s_config = config;
        singleton.s_fromServerThread = fromServerThread;
        instance().initialize(config);
        singleton.s_siteCountBarrier.setPartyCount(
                instance().getCatalogContext().getNodeSettings().getLocalSitesCount());
    }

    /**
     * Run CLI operations
     * @param config  The VoltDB.Configuration to use for getting configuration via CLI
     */
    public static void cli(VoltDB.Configuration config) {
        singleton.s_config = config;
        instance().cli(config);
    }

    /**
     * Retrieve a reference to the object implementing VoltDBInterface.  When
     * running a real server (and not a test harness), this instance will only
     * be useful after calling VoltDB.initialize().
     *
     * @return A reference to the underlying VoltDBInterface object.
     */
    public static VoltDBInterface instance() {
        return singleton.s_voltdb;
    }

    public static boolean instanceOnServerThread() {
        return singleton.s_fromServerThread;
    }
    /**
     * Useful only for unit testing.
     *
     * Replace the default VoltDB server instance with an instance of
     * VoltDBInterface that is used for testing.
     *
     */
    public static void replaceVoltDBInstanceForTest(VoltDBInterface testInstance) {
        singleton.s_voltdb = testInstance;
    }

    public static String getPublicReplicationInterface() {
        return singleton.s_config.m_drPublicHost == null || singleton.s_config.m_drPublicHost.isEmpty() ?
                "" : singleton.s_config.m_drPublicHost;
    }

    public static int getPublicReplicationPort() {
        return singleton.s_config.m_drPublicPort;
    }

    public static String getPublicTopicsInterface() {
        if (singleton.s_config.m_topicsPublicHostPort != null) {
            return singleton.s_config.m_topicsPublicHostPort.getHost();
        }
        return "";
    }

    public static int getPublicTopicsPort() {
        if (singleton.s_config.m_topicsPublicHostPort != null  && singleton.s_config.m_topicsPublicHostPort.hasPort()) {
            return singleton.s_config.m_topicsPublicHostPort.getPort();
        }
        return DISABLED_PORT;
    }

    public static int getTopicsPort(int deploymentPort) {
        if (singleton.s_config.m_topicsHostPort != null  && singleton.s_config.m_topicsHostPort.hasPort()) {
            return singleton.s_config.m_topicsHostPort.getPort();
        }
        return deploymentPort;
    }
    /**
     * Selects the a specified m_drInterface over a specified m_externalInterface from m_config
     * @return an empty string when neither are specified
     */
    public static String getDefaultReplicationInterface() {
        if (singleton.s_config.m_drInterface == null || singleton.s_config.m_drInterface.isEmpty()) {
            if (singleton.s_config.m_externalInterface == null) {
                return "";
            } else {
                return singleton.s_config.m_externalInterface;
            }
        } else {
            return singleton.s_config.m_drInterface;
        }
    }

    public static int getReplicationPort(int deploymentFilePort) {
        if (singleton.s_config.m_drAgentPortStart != -1) {
            return singleton.s_config.m_drAgentPortStart;
        } else {
            return deploymentFilePort;
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    public static class SimulatedExitException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final int status;
        public SimulatedExitException(int status) {
            this.status = status;
        }
        public int getStatus() {
            return status;
        }
    }

    public static void exit(int status) {
        if (CoreUtils.isJunitTest() || ignoreCrash) {
            throw new SimulatedExitException(status);
        }
        System.exit(status);
    }

    public static String generateThreadDump() {
        StringBuilder threadDumps = new StringBuilder();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        for (ThreadInfo t : threadInfos) {
            threadDumps.append(t);
        }
        return threadDumps.toString();
    }

    public static boolean dumpThreadTraceToFile(String dumpDir, String fileName) {
        final File dir = new File(dumpDir);
        if (!dir.getParentFile().canWrite() || !dir.getParentFile().canExecute()) {
            System.err.println("Parent directory " + dir.getParentFile().getAbsolutePath() +
                    " is not writable");
            return false;
        }
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                System.err.println("Failed to create directory " + dir.getAbsolutePath());
                return false;
            }
        }
        File file = new File(dumpDir, fileName);
        try (FileWriter writer = new FileWriter(file); PrintWriter out = new PrintWriter(writer)) {
            out.println(generateThreadDump());
        } catch (IOException e) {
            System.err.println("Failed to write to file " + file.getAbsolutePath());
            return false;
        }
        return true;
    }

    /**
     * Small wrapper class around a {@link CyclicBarrier}. This is used so that operations can synchronize on this
     * instance and still be able to change the participant count in the barrier when the site count changes (decommission)
     */
    public static final class UpdatableSiteCoordinationBarrier {
        private CyclicBarrier m_barrier;

        UpdatableSiteCoordinationBarrier() {}

        synchronized void setPartyCount(int parties) {
            if (m_barrier != null && m_barrier.getNumberWaiting() != 0) {
                throw new IllegalStateException("Cannot change participant count while parties are waiting");
            }
            CyclicBarrier oldBarrier = m_barrier;
            m_barrier = new CyclicBarrier(parties);
            if (oldBarrier != null) {
                oldBarrier.reset();
            }
        }

        public void await() throws InterruptedException, BrokenBarrierException {
            m_barrier.await();
        }

        public void reset() {
            m_barrier.reset();
        }
    }

    private static class VoltDBInstance {
        // NOTE: an explicit exception to the pattern is the Hashinator (which is a static map)
        //       because the initialize method explicitly resets the map.
        boolean s_fromServerThread;
        VoltDB.Configuration s_config;
        VoltDBInterface s_voltdb;
        ImportManager s_importManager;
        volatile ExportManagerInterface s_exportManager;
        E3ExecutorFactoryInterface s_e3ExecutorFactory;
        ShutdownHooks s_shutdownHooks;
        volatile TTLManager s_ttlManager;
        UpdatableSiteCoordinationBarrier s_siteCountBarrier;

        VoltDBInstance(VoltDB.Configuration emptyConfig) {
            s_fromServerThread = false;
            s_config = emptyConfig;
            s_voltdb = new RealVoltDB();
            s_importManager = null;
            s_exportManager = null;
            s_e3ExecutorFactory = null;
            s_shutdownHooks = new ShutdownHooks();
            s_ttlManager = null;
            s_siteCountBarrier = new UpdatableSiteCoordinationBarrier();
        }
    }

    public static void setImportManagerInstance(ImportManager mgr) {
        singleton.s_importManager = mgr;
    }

    /**
     * Get the global instance of the ImportManager.
     * @return The global single instance of the ImportManager.
     */
    public static ImportManager getImportManager() {
        return singleton.s_importManager;
    }

    public static ExportManagerInterface getExportManager() {
        return singleton.s_exportManager;
    }

    public static void setExportManagerInstance(ExportManagerInterface self) {
        singleton.s_exportManager = self;
    }

    public static E3ExecutorFactoryInterface getE3ExecutorFactory() {
        return singleton.s_e3ExecutorFactory;
    }

    public static void setE3ExecutorFactoryInstance(E3ExecutorFactoryInterface self) {
        singleton.s_e3ExecutorFactory = self;
    }

    public static ShutdownHooks getShutdownHooks() {
        return singleton.s_shutdownHooks;
    }


    public static void setTTLManagerInstance(TTLManager self) {
        singleton.s_ttlManager = self;
    }

    public static TTLManager getTTLManager() {
        if (singleton.s_ttlManager == null) {
            synchronized (TTLManager.class) {
                if (singleton.s_ttlManager == null) {
                    singleton.s_ttlManager = new TTLManager();
                }
            }
        }
        return singleton.s_ttlManager;
    }

    public static UpdatableSiteCoordinationBarrier getSiteCountBarrier() {
        return singleton.s_siteCountBarrier;
    }

    public static void resetSingletonsForTest() {
        singleton = new VoltDBInstance(s_emptyConfig);
        EstTimeUpdater.s_pause = false;
        DBBPool.clear();
    }

    // The Configuration class initializer also initializes the Logger ... so yeah.
    private static VoltDB.Configuration s_emptyConfig = new VoltDB.Configuration();
    private static VoltDBInstance singleton = new VoltDBInstance(s_emptyConfig);
}
