package org.voltdb.config;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Queue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.PortGenerator;
import org.voltdb.BackendTarget;
import org.voltdb.OperationMode;
import org.voltdb.ReplicationRole;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.net.HostAndPort;

/** Encapsulates VoltDB configuration parameters */
public class Configuration {

    public int m_ipcPort = VoltDB.DEFAULT_IPC_PORT;

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
    public int m_port = VoltDB.DEFAULT_PORT;
    public String m_clientInterface = "";

    /** override for the admin port number in the deployment file */
    public int m_adminPort = -1;
    public String m_adminInterface = "";

    /** port number to use to build intra-cluster mesh */
    public int m_internalPort = VoltDB.DEFAULT_INTERNAL_PORT;
    public String m_internalPortInterface = VoltDB.DEFAULT_INTERNAL_INTERFACE;

    /** interface to listen to clients on (default: any) */
    public String m_externalInterface = VoltDB.DEFAULT_EXTERNAL_INTERFACE;

    /** interface to use for backchannel comm (default: any) */
    public String m_internalInterface = VoltDB.DEFAULT_INTERNAL_INTERFACE;

    /** port number to use for DR channel (override in the deployment file) */
    public int m_drAgentPortStart = VoltDB.DEFAULT_DR_PORT;
    public String m_drInterface = "";

    /** HTTP port can't be set here, but eventually value will be reflected here */
    public int m_httpPort = Constants.HTTP_PORT_DISABLED;
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
            hostLog.fatal("You must specify a startup action, either create, recover, rejoin, collect, or compile.");
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
                hostLog.fatal("The startup action is missing (either create, recover or rejoin).");
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