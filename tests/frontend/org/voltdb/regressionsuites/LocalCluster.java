/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.regressionsuites;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.voltcore.logging.VoltLogger;
import org.voltdb.BackendTarget;
import org.voltdb.EELibraryLoader;
import org.voltdb.ServerThread;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CommandLine;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.collect.Maps;

/**
 * Implementation of a VoltServerConfig for a multi-process
 * cluster. All cluster processes run locally (keep this in
 * mind if building memory or load intensive tests.)
 */
public class LocalCluster extends VoltServerConfig {

    public enum FailureState {
        ALL_RUNNING,
        ONE_FAILURE,
        ONE_RECOVERING
    }

    // Used to provide out-of-band HostId determination.
    // NOTE: This mechanism can't be used when m_hasLocalServer is enabled
    public static final String clusterHostIdProperty = "__VOLTDB_CLUSTER_HOSTID__";

    private VoltLogger log = new VoltLogger("HOST");

    // the timestamp salt for the TransactionIdManager
    // will vary between -3 and 3 uniformly
    static final int TIMESTAMP_SALT_VARIANCE = 3;

    int getRandomTimestampSalt() {
        Random r = new Random();
        // if variance is 3, get a range between 0 and 6 inclusive
        int retval = r.nextInt(TIMESTAMP_SALT_VARIANCE * 2 + 1);
        // shift that range so it goes from -3 to 3 inclusive
        retval -= TIMESTAMP_SALT_VARIANCE;
        return retval;
    }

    // how long to wait for startup of external procs
    static final long PIPE_WAIT_MAX_TIMEOUT = 60 * 1000 *2; //*2 == slow machine allowance

    String m_callingClassName = "";
    String m_callingMethodName = "";
    boolean m_compiled = false;
    protected int m_siteCount;
    int m_hostCount;
    int m_missingHostCount = 0;
    int m_kfactor = 0;
    int m_clusterId;
    protected String m_jarFileName;
    boolean m_running = false;
    private final boolean m_debug;
    FailureState m_failureState;
    int m_nextIPCPort = 10000;
    ArrayList<Process> m_cluster = new ArrayList<>();
    int perLocalClusterExtProcessIndex = 0;
    VoltProjectBuilder m_builder;
    private boolean m_expectedToCrash = false;
    private boolean m_expectedToInitialize = true;
    int m_replicationPort = -1;

    // log message pattern match results by host
    private Map<Integer, Set<String>> m_logMessageMatchResults = new ConcurrentHashMap<>();
    // log message patterns
    private Map<String, Pattern> m_logMessageMatchPatterns = new ConcurrentHashMap<>();

    Map<String, String> m_hostRoots = new HashMap<>();
    /** Gets the dedicated paths in the filesystem used as a root for each process.
     * Used with NewCLI.
     */
    public Map<String, String> getHostRoots() {
        return m_hostRoots;
    }

    // Dedicated paths in the filesystem to be used as a root for each process
    ArrayList<File> m_subRoots = new ArrayList<>();
    public ArrayList<File> getSubRoots() {
        return m_subRoots;
    }

    // This should be set to true by default, otherwise the client interface
    // may throw different types of exception in other tests, which results
    // to failure
    boolean m_hasLocalServer = true;
    public void setHasLocalServer(boolean hasLocalServer) {
        m_hasLocalServer = hasLocalServer;
    }

    ArrayList<PipeToFile> m_pipes = null;
    ArrayList<CommandLine> m_cmdLines = null;
    ServerThread m_localServer = null;
    ProcessBuilder m_procBuilder;

    //wait before next node is started up in millisecond
    //to help matching the host id on the real cluster with the host id on the local
    //cluster
    private long m_deplayBetweenNodeStartupMS = 0;
    private boolean m_httpPortEnabled = false;
    private final ArrayList<EEProcess> m_eeProcs = new ArrayList<>();
    //This is additional process invironment variables that can be passed.
    // This is used to pass JMX port. Any additional use cases can use this too.
    private Map<String, String> m_additionalProcessEnv = null;
    protected final Map<String, String> getAdditionalProcessEnv() {
        return m_additionalProcessEnv;
    }

    // Produce a (presumably) available IP port number.
    public static final PortGeneratorForTest portGenerator = new PortGeneratorForTest();
    private InternalPortGeneratorForTest internalPortGenerator;
    private int numberOfCoordinators = 1;
    private String m_voltdbroot = "";
    private VoltFile m_filePrefix;

    private String[] m_versionOverrides = null;
    private String[] m_versionCheckRegexOverrides = null;
    private String[] m_buildStringOverrides = null;

    private String[] m_modeOverrides = null;
    private Map<Integer, Integer> m_sitesperhostOverrides = null;
    private String[] m_placementGroups = null;
    // The base command line - each process copies and customizes this.
    // Each local cluster process has a CommandLine instance configured
    // with the port numbers and command line parameter value specific to that
    // instance.
    private final CommandLine templateCmdLine = new CommandLine(StartAction.CREATE);
    //NEW_CLI can be picked up from env var or -D to JVM.
    private boolean isNewCli = Boolean.valueOf(System.getenv("NEW_CLI") == null ? System.getProperty("NEW_CLI", "true") : System.getenv("NEW_CLI"));
    public boolean isNewCli() { return isNewCli; };
    public void setNewCli(boolean flag) {
        isNewCli = flag;
        templateCmdLine.setNewCli(flag);
        templateCmdLine.startCommand("create");
    };

    private boolean isEnableSSL = Boolean.valueOf(System.getenv("ENABLE_SSL") == null ? Boolean.toString(Boolean.getBoolean("ENABLE_SSL")) : System.getenv("ENABLE_SSL"));
    public boolean isEnableSSL() { return isEnableSSL; };
    public void setEnableSSL(boolean flag) {
        isEnableSSL = flag;
        templateCmdLine.m_sslEnable = flag;
        templateCmdLine.m_sslExternal = flag;
        templateCmdLine.m_sslInternal = flag;
    };

    private String m_prefix = null;
    private boolean m_isPaused = false;
    private boolean m_usesStagedSchema;

    private int m_httpOverridePort = -1;

    /** Schema to use on the mismatched node, or null to initialize a bare node. */
    private String m_mismatchSchema = null;
    /** Node to initialize with a different schema, or null to use the same schema on all nodes. */
    private Integer m_mismatchNode = null;

    public void setHttpOverridePort(int port) {
        m_httpOverridePort = port;
    }
    public int getHttpOverridePort() { return m_httpOverridePort; };

    /*
     * Enable pre-compiled regex search in logs
     */
    public void setLogSearchPatterns(List<String> regexes) {
        for (int i = 0; i < regexes.size(); i++) {
            String s = regexes.get(i);
            Pattern p = Pattern.compile(s);
            m_logMessageMatchPatterns.put(s, p);
        }
    }

    public LocalCluster(String jarFileName,
                        int siteCount,
                        int hostCount,
                        int kfactor,
                        BackendTarget target)
    {
        this(jarFileName, siteCount, hostCount, kfactor, target, null);
    }

    public LocalCluster(String jarFileName,
            int siteCount,
            int hostCount,
            int kfactor,
            BackendTarget target,
            int inactiveCount)
{
       this(jarFileName, siteCount, hostCount, kfactor, target, null);
       this.m_missingHostCount = inactiveCount;
}

    public LocalCluster(String jarFileName,
                        int siteCount,
                        int hostCount,
                        int kfactor,
                        BackendTarget target,
                        Map<String, String> env)
    {
        this(jarFileName, siteCount, hostCount, kfactor, target,
                FailureState.ALL_RUNNING, false, false, env);

    }

    public LocalCluster(String jarFileName,
                        int siteCount,
                        int hostCount,
                        int kfactor,
                        BackendTarget target,
                        boolean isRejoinTest)
    {
        this(jarFileName, siteCount, hostCount, kfactor, target,
                FailureState.ALL_RUNNING, false, isRejoinTest, null);
    }

    public LocalCluster(String jarFileName,
            int siteCount,
            int hostCount,
            int kfactor,
            int clusterId,
            BackendTarget target,
            boolean isRejoinTest)
    {
        this(jarFileName, siteCount, hostCount, kfactor, clusterId, target,
            FailureState.ALL_RUNNING, false, isRejoinTest, null);
    }

    public LocalCluster(String jarFileName,
                        int siteCount,
                        int hostCount,
                        int kfactor,
                        BackendTarget target,
                        FailureState failureState,
                        boolean debug)
    {
        this(jarFileName, siteCount, hostCount, kfactor, target,
                failureState, debug, false, null);
    }

    public LocalCluster(String jarFileName,
                        int siteCount,
                        int hostCount,
                        int kfactor,
                        BackendTarget target,
                        FailureState failureState,
                        boolean debug,
                        boolean isRejoinTest,
                        Map<String, String> env) {
        this(jarFileName, siteCount, hostCount, kfactor, 0, target,
                failureState, debug, isRejoinTest, env);
    }

    public LocalCluster(String jarFileName,
            int siteCount,
            int hostCount,
            int kfactor,
            int clusterId,
            BackendTarget target,
            FailureState failureState,
            boolean debug,
            boolean isRejoinTest,
            Map<String, String> env) {
        this(null, null, jarFileName, siteCount, hostCount, kfactor, clusterId, target, failureState, debug, isRejoinTest, env);
    }

    public LocalCluster(String schemaToStage,
                        String classesJarToStage,
                        String catalogJarFileName,
                        int siteCount,
                        int hostCount,
                        int kfactor,
                        int clusterId,
                        BackendTarget target,
                        FailureState failureState,
                        boolean debug,
                        boolean isRejoinTest,
                        Map<String, String> env)
    {
        m_usesStagedSchema = schemaToStage != null || classesJarToStage != null;
        setNewCli(isNewCli() || m_usesStagedSchema);

        assert siteCount > 0 : "site count is less than 1";
        assert hostCount > 0 : "host count is less than 1";

        numberOfCoordinators = hostCount <= 2 ? hostCount : hostCount <= 4 ? 2 : 3;
        internalPortGenerator = new InternalPortGeneratorForTest(portGenerator, numberOfCoordinators);

        m_additionalProcessEnv = env==null ? new HashMap<>() : env;
        if (Boolean.getBoolean(EELibraryLoader.USE_JAVA_LIBRARY_PATH)) {
            // set use.javalib for LocalCluster so that Eclipse runs will be OK.
            m_additionalProcessEnv.put(EELibraryLoader.USE_JAVA_LIBRARY_PATH, "true");
        }
        // get the name of the calling class
        StackTraceElement[] traces = Thread.currentThread().getStackTrace();
        m_callingClassName = "UnknownClass";
        m_callingMethodName = "unknownMethod";
        //ArrayUtils.reverse(traces);
        int i;
        // skip all stack frames below this method
        for (i = 0; ! traces[i].getClassName().equals(getClass().getName()); i++);
        // skip all stack frames from localcluster itself
        for (;      traces[i].getClassName().equals(getClass().getName()); i++);
        // skip the package name
        int dot = traces[i].getClassName().lastIndexOf('.');
        m_callingClassName = traces[i].getClassName().substring(dot + 1);
        m_callingMethodName = traces[i].getMethodName();

        if (catalogJarFileName == null) {
            if (m_usesStagedSchema == false) {
                log.info("Instantiating empty LocalCluster with class.method: " +
                        m_callingClassName + "." + m_callingMethodName);
            } else {
                log.info("Instantiating LocalCluster with schema and class.method: " +
                        m_callingClassName + "." + m_callingMethodName);
            }
        } else {
            assert m_usesStagedSchema == false : "Cannot use OldCLI catalog with staged schema and/or classes";
            log.info("Instantiating LocalCluster for " + catalogJarFileName + " with class.method: " +
                    m_callingClassName + "." + m_callingMethodName);
        }
        log.info("ClusterId: " + clusterId + " Sites: " + siteCount + " Hosts: " + hostCount + " ReplicationFactor: " + kfactor);

        m_cluster.ensureCapacity(hostCount);

        m_siteCount = siteCount;
        m_hostCount = hostCount;
        m_sitesperhostOverrides = Maps.newHashMap();
        for (int hostId = 0; hostId < hostCount; hostId++) {
            m_sitesperhostOverrides.put(hostId, m_siteCount);
        }
        templateCmdLine.hostCount(hostCount);
        templateCmdLine.setMissingHostCount(m_missingHostCount);
        setEnableSSL(isEnableSSL);
        m_kfactor = kfactor;
        m_clusterId = clusterId;
        m_debug = debug;
        m_jarFileName = catalogJarFileName;
        m_failureState = m_kfactor < 1 ? FailureState.ALL_RUNNING : failureState;
        m_pipes = new ArrayList<>();
        m_cmdLines = new ArrayList<>();
        // m_userSchema and m_stagedClassesPath are only used by init.
        // start ignores them silently if they are set.
        if (schemaToStage != null) {
            try {
                templateCmdLine.m_userSchema = VoltProjectBuilder.createFileForSchema(schemaToStage);
                log.info("LocalCluster staged schema as \"" + templateCmdLine.m_userSchema + "\"");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (classesJarToStage != null) {
            templateCmdLine.m_stagedClassesPath = new VoltFile(classesJarToStage);
            log.info("LocalCluster staged classes as \"" + templateCmdLine.m_stagedClassesPath + "\"");
        }

        // if the user wants valgrind and it makes sense, give it to 'em
        // For now only one host works.
        if (isMemcheckDefined() && (target == BackendTarget.NATIVE_EE_JNI) && m_hostCount == 1) {
            m_target = BackendTarget.NATIVE_EE_VALGRIND_IPC;
        }
        else {
            m_target = target;
        }

        String buildDir = System.getenv("VOLTDB_BUILD_DIR");  // via build.xml
        if (buildDir == null) {
            buildDir = System.getProperty("user.dir") + "/obj/release";
        }
        // Allow importer tests to find their bundles
        String defaultBundleDir = System.getProperty("user.dir") + "/bundles";
        m_additionalProcessEnv.putIfAbsent(
                CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME,
                System.getProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, defaultBundleDir));

        String classPath = System.getProperty("java.class.path");
        if (m_jarFileName != null) {
            classPath += ":" + buildDir + File.separator + m_jarFileName;
        }
        classPath += ":" + buildDir + File.separator + "prod";
        // Remove the stored procedures from the classpath.  Out-of-process nodes will
        // only be able to find procedures and dependent classes in the catalog, as intended
        classPath = classPath.replace(buildDir + File.separator + "testprocs:", "");

        // set the java lib path to the one for this process - Add obj/release/nativelibs
        String javaLibraryPath = System.getProperty("java.library.path");
        if (javaLibraryPath == null || javaLibraryPath.trim().length() == 0) {
            javaLibraryPath = buildDir + "/nativelibs";
        }
        else {
            javaLibraryPath += ":" + buildDir + "/nativelibs";
        }

        // First try 'ant' syntax and then 'eclipse' syntax...
        String log4j = System.getProperty("log4j.configuration");
        if (log4j == null) {
            log4j = "file://" + System.getProperty("user.dir") + "/tests/log4j-allconsole.xml";
        }

        m_procBuilder = new ProcessBuilder();

        // set the working directory to obj/release/prod
        //m_procBuilder.directory(new File(m_buildDir + File.separator + "prod"));
        m_procBuilder.redirectErrorStream(true);

        Thread shutdownThread = new Thread(new ShutDownHookThread());
        java.lang.Runtime.getRuntime().addShutdownHook(shutdownThread);
        this.templateCmdLine.
            addTestOptions(true).
            leader("").
            target(m_target).
            startCommand(isNewCli() ? "probe" : "create").
            jarFileName(VoltDB.Configuration.getPathToCatalogForTest(m_jarFileName)).
            buildDir(buildDir).
            classPath(classPath).
            pathToLicense(ServerThread.getTestLicensePath()).
            log4j(log4j).
            setForceVoltdbCreate(true);
        if (javaLibraryPath!=null) {
            templateCmdLine.javaLibraryPath(javaLibraryPath);
        }
        this.templateCmdLine.m_noLoadLibVOLTDB = m_target == BackendTarget.HSQLDB_BACKEND;
        // "tag" this command line so it's clear which test started it
        this.templateCmdLine.m_tag = m_callingClassName + ":" + m_callingMethodName;
    }

    /** Directs this LocalCluster to initialize one of its nodes with a different schema.
     * Only use this with NewCLI clusters that have an initialized schema.
     *
     * @pre the node specified is not running
     * @param mismatchSchema schema to put on one node, or null if node should be empty
     * @param nodeID node to put the mismatch, or null to re-enable matched schemas
     */
    public void setMismatchSchemaForInit( String mismatchSchema, Integer nodeID ){
        assert isNewCli();
        assert m_usesStagedSchema;
        m_mismatchSchema = mismatchSchema;
        m_mismatchNode = nodeID;
    }

    public CommandLine getTemplateCommandLine() {
        return templateCmdLine;
    }

    public void setToStartPaused() {
       m_isPaused = true;
    }
    /**
     * Override the Valgrind backend with a JNI backend.
     * Called after a constructor but before startup.
     */
    public void overrideAnyRequestForValgrind() {
        if (templateCmdLine.m_backend == BackendTarget.NATIVE_EE_VALGRIND_IPC) {
            m_target = BackendTarget.NATIVE_EE_JNI;
            templateCmdLine.m_backend = BackendTarget.NATIVE_EE_JNI;
        }
    }

    public void overrideStartCommandVerb(String verb) {
        if (verb == null || verb.trim().isEmpty()) return;
        this.templateCmdLine.startCommand(verb);
    }

    public void setCustomCmdLn(String customCmdLn) {
        templateCmdLine.customCmdLn(customCmdLn);
    }

    public void setJavaProperty(String property, String value) {
        templateCmdLine.setJavaProperty(property, value);
    }

    @Override
    public void setCallingMethodName(String name) {
        m_callingMethodName = name;
    }

    public boolean compile(VoltProjectBuilder builder, final String voltRootPath) {
        if (! m_compiled) {
            m_initialCatalog = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor, voltRootPath, m_clusterId);
            m_compiled = m_initialCatalog != null;
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
            m_voltdbroot = builder.getPathToVoltRoot().getAbsolutePath();
        }
        return m_compiled;
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        if (!m_compiled) {
            m_initialCatalog = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor, null, m_clusterId);
            m_compiled = m_initialCatalog != null;
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
            m_voltdbroot = builder.getPathToVoltRoot().getAbsolutePath();
        }
        return m_compiled;
    }

    @Override
    public boolean compileWithPartitionDetection(VoltProjectBuilder builder, String snapshotPath, String ppdPrefix) {
        if (!m_compiled) {
            m_compiled = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor,
                    null, m_clusterId, true, snapshotPath, ppdPrefix);
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
            m_voltdbroot = builder.getPathToVoltRoot().getAbsolutePath();
        }
        return m_compiled;
    }

    @Override
    public boolean compileWithAdminMode(VoltProjectBuilder builder, int adminPort, boolean adminOnStartup)
    {
        if (adminOnStartup) {
            setToStartPaused();
        }

        if (!m_compiled) {
            m_initialCatalog = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor, m_clusterId);
            m_compiled = m_initialCatalog != null;
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
            m_voltdbroot = builder.getPathToVoltRoot().getAbsolutePath();
        }
        return m_compiled;
    }

    @Override
    public void startUp() {
        startUp(true);
    }

    @Override
    public void startUp(boolean clearLocalDataDirectories) {
        //if cleardirectory is true we dont skip init.
        startUp(clearLocalDataDirectories, ! clearLocalDataDirectories);
    }

    public void setForceVoltdbCreate(boolean newVoltdb) {
        templateCmdLine.setForceVoltdbCreate(newVoltdb);
    }

    public void setDeploymentAndVoltDBRoot(String pathToDeployment, String pathToVoltDBRoot) {
        templateCmdLine.pathToDeployment(pathToDeployment);
        m_voltdbroot = pathToVoltDBRoot;
        m_compiled = true;
    }

    public void setFilePrefix(VoltFile filePrefix) {
        m_filePrefix = filePrefix;
    }

    public void setHostCount(int hostCount)
    {
        m_hostCount = hostCount;
        if (hostCount < numberOfCoordinators) {
            numberOfCoordinators = hostCount;
        }
        // Force recompilation
        m_compiled = false;
    }

    public void setHttpPortEnabled(boolean enabled) {
        m_httpPortEnabled = enabled;
    }

    public void setReplicationPort(int port) {
        m_replicationPort = port;
    }

    private void startLocalServer(int hostId, boolean clearLocalDataDirectories) throws IOException {
        startLocalServer(hostId, clearLocalDataDirectories, templateCmdLine.m_startAction);
    }

    private void startLocalServer(int hostId, boolean clearLocalDataDirectories, StartAction action) throws IOException {
        // Generate a new root for the in-process server if clearing directories.
        File subroot = null;
        if (!isNewCli) {
            try {
                if (m_filePrefix != null) {
                    subroot = m_filePrefix;
                    m_subRoots.add(subroot);
                }
                else if (clearLocalDataDirectories) {
                    subroot = VoltFile.initNewSubrootForThisProcess();
                    m_subRoots.add(subroot);
                }
                else {
                    if (m_subRoots.size() <= hostId) {
                        m_subRoots.add(VoltFile.initNewSubrootForThisProcess());
                    }
                    subroot = m_subRoots.get(hostId);
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Make the local Configuration object...
        CommandLine cmdln = (templateCmdLine.makeCopy());
        cmdln.startCommand(action);
        cmdln.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));
        if (this.m_additionalProcessEnv != null) {
            for (String name : this.m_additionalProcessEnv.keySet()) {
                cmdln.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
            }
        }
        if (!isNewCli) {
            cmdln.voltFilePrefix(subroot.getPath());
        }
        cmdln.internalPort(internalPortGenerator.nextInternalPort(hostId));
        cmdln.coordinators(internalPortGenerator.getCoordinators());
        cmdln.port(portGenerator.nextClient());
        cmdln.adminPort(portGenerator.nextAdmin());
        cmdln.zkport(portGenerator.nextZkPort());
        cmdln.httpPort(portGenerator.nextHttp());
        // replication port and its two automatic followers.
        cmdln.drAgentStartPort(m_replicationPort != -1 ? m_replicationPort : portGenerator.nextReplicationPort());
        portGenerator.nextReplicationPort();
        portGenerator.nextReplicationPort();
        if (m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC) {
            EEProcess proc = m_eeProcs.get(hostId);
            assert(proc != null);
            cmdln.m_ipcPort = proc.port();
        }
        if (m_target == BackendTarget.NATIVE_EE_IPC) {
            cmdln.m_ipcPort = portGenerator.next();
        }
        if ((m_versionOverrides != null) && (m_versionOverrides.length > hostId)) {
            assert(m_versionOverrides[hostId] != null);
            assert(m_versionCheckRegexOverrides[hostId] != null);
            cmdln.m_versionStringOverrideForTest = m_versionOverrides[hostId];
            cmdln.m_versionCompatibilityRegexOverrideForTest = m_versionCheckRegexOverrides[hostId];
            if ((m_buildStringOverrides != null) && (m_buildStringOverrides.length > hostId)) {
                assert(m_buildStringOverrides[hostId] != null);
                cmdln.m_buildStringOverrideForTest = m_buildStringOverrides[hostId];
            }
        }
        if ((m_modeOverrides != null) && (m_modeOverrides.length > hostId)) {
            assert(m_modeOverrides[hostId] != null);
            cmdln.m_modeOverrideForTest = m_modeOverrides[hostId];
            cmdln.m_isPaused = true;
        }

        if ((m_sitesperhostOverrides != null) && (m_sitesperhostOverrides.size() > hostId)) {
            assert(m_sitesperhostOverrides.containsKey(hostId));
            cmdln.m_sitesperhost = m_sitesperhostOverrides.get(hostId);
        }

        // for debug, dump the command line to a unique file.
        // cmdln.dumpToFile("/Users/rbetts/cmd_" + Integer.toString(portGenerator.next()));

        m_cluster.add(null);
        m_pipes.add(null);
        m_cmdLines.add(cmdln);
        if (isNewCli) {
            cmdln.m_startAction = StartAction.PROBE;
            cmdln.enableAdd(action == StartAction.JOIN);
            cmdln.m_hostCount = m_hostCount;
            String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
            String root = m_hostRoots.get(hostIdStr);
            //For new CLI dont pass deployment for probe.
            cmdln.pathToDeployment(null);
            cmdln.voltdbRoot(root + File.separator + Constants.DBROOT);
        }
        m_localServer = new ServerThread(cmdln);
        if (m_usesStagedSchema) {
            // ServerThread sets this to true, always - override with our desired behavior.
            // Only do this for staged schema tests - preserve old behavior for others.
            cmdln.setForceVoltdbCreate(clearLocalDataDirectories);
        }
        m_localServer.start();
    }

    /**
     * Gets the voltdbroot directory for the specified host.
     * WARNING: behavior is inconsistent with {@link VoltFile#getServerSpecificRoot(String, boolean)},
     * which returns the parent directory of voltdbroot.
     * @param hostId
     * @return  The location of voltdbroot
     */
    public String getServerSpecificRoot(String hostId) {
        if (isNewCli()) {
            if (!m_hostRoots.containsKey(hostId)) {
                throw new IllegalArgumentException("getServerSpecificRoot possibly called before cluster has started.");
            }
            assert( new File(m_hostRoots.get(hostId)).getName().equals(Constants.DBROOT) == false ) : m_hostRoots.get(hostId);
            return m_hostRoots.get(hostId) + File.separator + Constants.DBROOT;
        }
        else {
            for (CommandLine cl : m_cmdLines) {
                if (cl.getJavaProperty(clusterHostIdProperty).equals(hostId)) {
                    return cl.voltRoot().toString();
                }
            }
            throw new IllegalArgumentException("getServerSpecificRoot could not find specified host (old CLI)");
        }
    }

    void initLocalServer(int hostId, boolean clearLocalDataDirectories) throws IOException {
        // Make the local Configuration object...
        CommandLine cmdln = (templateCmdLine.makeCopy());
        cmdln.startCommand(StartAction.INITIALIZE);
        cmdln.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));
        if (this.m_additionalProcessEnv != null) {
            for (String name : this.m_additionalProcessEnv.keySet()) {
                cmdln.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
            }
        }
        if (new Integer(hostId).equals(m_mismatchNode)) {
            assert m_usesStagedSchema;
            cmdln.m_userSchema = m_mismatchSchema == null ? null : VoltProjectBuilder.createFileForSchema(m_mismatchSchema);
        }
        cmdln.setForceVoltdbCreate(clearLocalDataDirectories);

        //If we are initializing lets wait for it to finish.
        ServerThread th = new ServerThread(cmdln);
        File root = VoltFile.getServerSpecificRoot(String.valueOf(hostId), clearLocalDataDirectories);
        assert( root.getName().equals(Constants.DBROOT) == false ) : root.getAbsolutePath();
        cmdln.voltdbRoot(new File(root, Constants.DBROOT));
        try {
            th.initialize();
        }
        catch (VoltDB.SimulatedExitException expected) {
            //All ok
        }
        catch (Exception ex) {
            log.error("Failed to initialize cluster process:" + ex.getMessage(), ex);
            assert (false);
        }
        //Keep track by hostid the voltdbroot
        String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
        m_hostRoots.put(hostIdStr, root.getAbsolutePath());
    }

    private boolean waitForAllReady()
    {
        if (!m_expectedToInitialize) {
            return true;
        }
        long startOfPipeWait = System.currentTimeMillis();
        boolean allReady = false;
        while ( ! allReady) {
            if ((System.currentTimeMillis() - startOfPipeWait) > PIPE_WAIT_MAX_TIMEOUT) {
                return false;
            }

            allReady = true;
            for (PipeToFile pipeToFile : m_pipes) {
                if (pipeToFile == null) {
                    continue;
                }
                synchronized(pipeToFile) {
                    // if prtests/frontend/org/voltdb/regressionsuites/LocalCluster.javaocess is dead, no point in waiting around
                    if (isProcessDead(pipeToFile.getProcess())) {
                        // dead process means the other pipes won't start,
                        // so bail here
                        return false;
                    }

                    // if eof, then no point in waiting around
                    if (pipeToFile.m_eof.get()) {
                        continue;
                    }

                    // if not eof, then wait for statement of readiness
                    if ( ! pipeToFile.m_witnessedReady.get()) {
                        try {
                            // use a timeout to prevent a forever hang
                            pipeToFile.wait(250);
                        }
                        catch (InterruptedException ex) {
                            log.error(ex.toString(), ex);
                        }
                        allReady = false;
                    }
                }
            }
        }
        return true;
    }

    private void printTiming(boolean logtime, String msg) {
        if (logtime) {
            System.out.println("************ " + msg);
        }
    }

    public void startUp(boolean clearLocalDataDirectories, boolean skipInit) {
        if (m_running) {
            return;
        }
        VoltServerConfig.addInstance(this);

        // needs to be called before any call to pick a filename
        VoltDB.setDefaultTimezone();

        if (m_isPaused) {
            // Set paused mode
            templateCmdLine.startPaused();
        }
        System.out.println("New CLI options set to: " + isNewCli);
        // set to true to spew startup timing data
        boolean logtime = false;
        long startTime = 0;
        printTiming(logtime, "Starting cluster at: " + System.currentTimeMillis());

        // clear any logs, export or snapshot data for this run
        if (clearLocalDataDirectories && !isNewCli) {
            try {
                VoltFile.deleteAllSubRoots();
                m_subRoots.clear();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // reset the port generator. RegressionSuite always expects
        // to find ClientInterface and Admin mode on known ports.
        portGenerator.reset();
        internalPortGenerator = new InternalPortGeneratorForTest(portGenerator, numberOfCoordinators);

        templateCmdLine.leaderPort(portGenerator.nextInternalPort());
        templateCmdLine.coordinators(internalPortGenerator.getCoordinators());
        if (m_httpPortEnabled) {
            templateCmdLine.httpPort(0); // Set this value to 0 would enable http port assignment
        }

        m_eeProcs.clear();
        int hostCount = m_hostCount - m_missingHostCount;
        for (int ii = 0; ii < hostCount; ii++) {
            String logfile = "LocalCluster_host_" + ii + ".log";
            m_eeProcs.add(new EEProcess(templateCmdLine.target(), m_siteCount, logfile));
        }

        m_pipes.clear();
        m_cluster.clear();
        m_cmdLines.clear();
        if (m_logMessageMatchPatterns != null) {
            resetLogMessageMatchResults();
        }
        int oopStartIndex = 0;

        // create the in-process server instance.
        if (m_hasLocalServer) {
            try {
                //Init
                if (isNewCli && !skipInit) {
                    initLocalServer(oopStartIndex, clearLocalDataDirectories);
                }
                startLocalServer(oopStartIndex, clearLocalDataDirectories);
            }
            catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            ++oopStartIndex;
        }

        // create all the out-of-process servers
        for (int i = oopStartIndex; i < hostCount; i++) {
            try {
                if (isNewCli && !skipInit) {
                    initOne(i, clearLocalDataDirectories);
                }
                String placementGroup = null;
                if (m_placementGroups != null && m_placementGroups.length == m_hostCount) {
                    placementGroup = m_placementGroups[i];
                }

                startOne(i, clearLocalDataDirectories, StartAction.CREATE, true, placementGroup);
                //wait before next one
                if (m_deplayBetweenNodeStartupMS > 0) {
                    try {
                        Thread.sleep(m_deplayBetweenNodeStartupMS);
                    } catch (InterruptedException e) {
                    }
                }
            }
            catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        printTiming(logtime, "Pre-witness: " + (System.currentTimeMillis() - startTime) + "ms");
        boolean allReady = false;
        allReady = waitForAllReady();
        printTiming(logtime, "Post-witness: " + (System.currentTimeMillis() - startTime) + "ms");

        // verify all processes started up and count failures
        int downProcesses = 0;
        for (Process proc : m_cluster) {
            if ((proc != null) && (isProcessDead(proc))) {
                downProcesses++;
            }
        }

        // throw an exception if there were failures starting up
        if ((downProcesses > 0) || ! allReady) {
            // poke all the external processes to die (no guarantees)
            for (Process proc : m_cluster) {
                if (proc != null) {
                    try { proc.destroy(); } catch (Exception e) {}
                }
            }

            if (downProcesses > 0) {
                int expectedProcesses = m_hostCount - (m_hasLocalServer ? 1 : 0);
                if (!m_expectedToCrash) {
                    throw new RuntimeException(
                            String.format("%d/%d external processes failed to start",
                            downProcesses, expectedProcesses));
                }
            }
            // this error case should only be from a timeout
            else if (!allReady) {
                throw new RuntimeException(
                        "One or more external processes failed to complete initialization.");
            }
        }

        // Finally, make sure the local server thread is running and wait if it is not.
        if (m_hasLocalServer) {
            m_localServer.waitForInitialization();
        }

        printTiming(logtime, "DONE: " + (System.currentTimeMillis() - startTime) + " ms");
        m_running = true;

        // if supposed to kill a server, it's go time
        if (m_failureState != FailureState.ALL_RUNNING) {
            killOne();
        }

        // after killing a server, bring it back in recovery mode
        if (m_failureState == FailureState.ONE_RECOVERING) {
            int hostId = m_hasLocalServer ? 1 : 0;
            recoverOne(logtime, startTime, hostId);
        }
    }

    private void killOne() {
        log.info("Killing one cluster member.");
        int procIndex = 0;
        if (m_hasLocalServer) {
            procIndex = 1;
        }

        Process proc = m_cluster.get(procIndex);
        proc.destroy();
        int retval = 0;
        File valgrindOutputFile = null;
        try {
            retval = proc.waitFor();
            EEProcess eeProc = m_eeProcs.get(procIndex);
            valgrindOutputFile = eeProc.waitForShutdown();
        }
        catch (InterruptedException e) {
            log.info("External VoltDB process is acting crazy.");
        }
        finally {
            m_cluster.set(procIndex, null);
        }

        // exit code 143 is the forcible shutdown code from .destroy()
        if (retval != 0 && retval != 143) {
            log.info("killOne: External VoltDB process terminated abnormally with return: " + retval);
        }

        failIfValgrindErrors(valgrindOutputFile);
    }

    private void initOne(int hostId, boolean clearLocalDataDirectories) throws IOException {
        PipeToFile ptf = null;
        CommandLine cmdln = (templateCmdLine.makeCopy());
        cmdln.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));
        if (this.m_additionalProcessEnv != null) {
            for (String name : this.m_additionalProcessEnv.keySet()) {
                cmdln.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
            }
        }
        File root = null;
        try {
            //If clear clean VoltFile.getServerSpecificRoot(String.valueOf(hostId))
            root = VoltFile.getServerSpecificRoot(String.valueOf(hostId), clearLocalDataDirectories);
            assert( root.getName().equals(Constants.DBROOT) == false ) : root.getAbsolutePath();
            cmdln = cmdln.voltdbRoot(new File(root, Constants.DBROOT));
            cmdln = cmdln.startCommand(StartAction.INITIALIZE);
            if (clearLocalDataDirectories) {
                cmdln.setForceVoltdbCreate(true);
            } else {
                cmdln.setForceVoltdbCreate(false);
            }
            if (new Integer(hostId).equals(m_mismatchNode)) {
                assert m_usesStagedSchema;
                cmdln.m_userSchema = m_mismatchSchema == null ? null : VoltProjectBuilder.createFileForSchema(m_mismatchSchema);
            }
            m_procBuilder.command().clear();
            List<String> cmdlnList = cmdln.createCommandLine();
            String cmdLineFull = "Init cmd host=" + String.valueOf(hostId) + " :";
            for (String element : cmdlnList) {
                assert(element != null);
                cmdLineFull += " " + element;
            }
            log.info(cmdLineFull);
            m_procBuilder.command().addAll(cmdlnList);

            // write output to obj/release/testoutput/<test name>-n.txt
            // this may need to be more unique? Also very useful to just
            // set this to a hardcoded path and use "tail -f" to debug.
            String testoutputdir = cmdln.buildDir() + File.separator + "testoutput";
            System.out.println("Process output will be redirected to: " + testoutputdir);
            // make sure the directory exists
            File dir = new File(testoutputdir);
            if (dir.exists()) {
                assert (dir.isDirectory());
            } else {
                boolean status = dir.mkdirs();
                assert (status);
            }

            File dirFile = new VoltFile(testoutputdir);
            if (dirFile.listFiles() != null) {
                for (File f : dirFile.listFiles()) {
                    if (f.getName().startsWith(getName() + "-" + hostId)) {
                        f.delete();
                    }
                }
            }

            Process proc = m_procBuilder.start();
            //Make init process output file begin with init so easy to vi LC*
            String fileName = testoutputdir
                    + File.separator
                    + "init-LC-"
                    + getFileName() + "-"
                    + m_clusterId + "-"
                    + hostId + "-"
                    + "idx" + String.valueOf(perLocalClusterExtProcessIndex++)
                    + ".txt";
            System.out.println("Process output can be found in: " + fileName);

            if (m_logMessageMatchPatterns == null) {
                ptf = new PipeToFile(
                        fileName,
                        proc.getInputStream(),
                        String.valueOf(hostId),
                        false,
                        proc);
            } else {
                if (m_logMessageMatchResults.get(hostId) == null) {
                    m_logMessageMatchResults.put(hostId, new ConcurrentHashSet<>());
                }
                ptf = new PipeToFile(
                        fileName,
                        proc.getInputStream(),
                        String.valueOf(hostId),
                        false,
                        proc,
                        m_logMessageMatchPatterns,
                        m_logMessageMatchResults.get(hostId));
                ptf.setHostId(hostId);
            }
            ptf.setName("ClusterPipe:" + String.valueOf(hostId));
            ptf.start();
            proc.waitFor(); // Wait for the server initialization to finish ?
        }
        catch (IOException ex) {
            log.error("Failed to start cluster process:" + ex.getMessage(), ex);
            assert (false);
        }
        catch (InterruptedException ex) {
            log.error("Failed to start cluster process:" + ex.getMessage(), ex);
            assert (false);
        }
        if ( root != null ) {
            String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
            m_hostRoots.put(hostIdStr, root.getPath());
        }
    }

    void startOne(int hostId, boolean clearLocalDataDirectories,
            StartAction startAction, boolean waitForReady, String placementGroup) throws IOException
    {
        startOne(hostId, clearLocalDataDirectories, startAction, waitForReady, placementGroup, false);
    }

    void startOne(int hostId, boolean clearLocalDataDirectories,
            StartAction startAction, boolean waitForReady, String placementGroup,
            boolean resetLogMessageMatchResults) throws IOException
    {
        PipeToFile ptf = null;
        CommandLine cmdln = (templateCmdLine.makeCopy());
        cmdln.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));
        if (isNewCli) {
            cmdln.m_startAction = StartAction.PROBE;
            cmdln.enableAdd(startAction == StartAction.JOIN);
            cmdln.hostCount(m_hostCount);
            String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
            String root = m_hostRoots.get(hostIdStr);
            //For new CLI dont pass deployment for probe.
            cmdln.voltdbRoot(root);
            cmdln.pathToDeployment(null);
            cmdln.setForceVoltdbCreate(clearLocalDataDirectories);
        }

        if (this.m_additionalProcessEnv != null) {
            for (String name : this.m_additionalProcessEnv.keySet()) {
                cmdln.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
            }
        }
        try {
            cmdln.internalPort(internalPortGenerator.nextInternalPort(hostId));
            cmdln.coordinators(internalPortGenerator.getCoordinators());
            if (m_replicationPort != -1) {
                int index = m_hasLocalServer ? hostId + 1 : hostId;
                cmdln.drAgentStartPort(m_replicationPort + index);
            } else {
                // set the dragent port. it uses the start value and
                // the next two sequential port numbers - so burn those two.
                cmdln.drAgentStartPort(portGenerator.nextReplicationPort());
                portGenerator.next();
                portGenerator.next();
            }

            // add the ipc ports
            if (m_target == BackendTarget.NATIVE_EE_IPC) {
                // set 1 port for the EE process
                cmdln.ipcPort(portGenerator.next());
            }
            if (m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC) {
                EEProcess proc = m_eeProcs.get(hostId);
                assert(proc != null);
                cmdln.m_ipcPort = proc.port();
            }

            cmdln.port(portGenerator.nextClient());
            cmdln.adminPort(portGenerator.nextAdmin());
            if (cmdln.m_httpPort != Constants.HTTP_PORT_DISABLED)
                cmdln.httpPort(portGenerator.nextHttp());
            cmdln.timestampSalt(getRandomTimestampSalt());
            cmdln.setPlacementGroup(placementGroup);
            if (m_debug) {
                cmdln.debugPort(portGenerator.next());
            }

            cmdln.zkport(portGenerator.nextZkPort());
            if (!isNewCli && startAction == StartAction.JOIN) {
                cmdln.startCommand(startAction);
                int portNoToRejoin = m_cmdLines.get(0).internalPort();
                cmdln.leader(":" + portNoToRejoin);
                cmdln.enableAdd(true);
            }

            // If local directories are being cleared
            // generate a new subroot, otherwise reuse the existing directory
            File subroot = null;
            if (!isNewCli) {
                if (m_filePrefix != null) {
                    subroot = m_filePrefix;
                    m_subRoots.add(subroot);
                } else if (clearLocalDataDirectories) {
                    subroot = VoltFile.getNewSubroot();
                    m_subRoots.add(subroot);
                } else {
                    if (m_subRoots.size() <= hostId) {
                        m_subRoots.add(VoltFile.getNewSubroot());
                    }
                    subroot = m_subRoots.get(hostId);
                }
                cmdln.voltFilePrefix(subroot.getPath());
                cmdln.voltRoot(subroot.getPath() + File.separator + m_voltdbroot);
            }

            if ((m_versionOverrides != null) && (m_versionOverrides.length > hostId)) {
                assert(m_versionOverrides[hostId] != null);
                assert(m_versionCheckRegexOverrides[hostId] != null);
                cmdln.m_versionStringOverrideForTest = m_versionOverrides[hostId];
                cmdln.m_versionCompatibilityRegexOverrideForTest = m_versionCheckRegexOverrides[hostId];
                if ((m_buildStringOverrides != null) && (m_buildStringOverrides.length > hostId)) {
                    assert(m_buildStringOverrides[hostId] != null);
                    cmdln.m_buildStringOverrideForTest = m_buildStringOverrides[hostId];
                }
            }

            if ((m_modeOverrides != null) && (m_modeOverrides.length > hostId)) {
                assert(m_modeOverrides[hostId] != null);
                cmdln.m_modeOverrideForTest = m_modeOverrides[hostId];
            }

            if ((m_sitesperhostOverrides != null) && (m_sitesperhostOverrides.size() > hostId)) {
                assert(m_sitesperhostOverrides.containsKey(hostId));
                cmdln.m_sitesperhost = m_sitesperhostOverrides.get(hostId);
            }

            cmdln.setMissingHostCount(m_missingHostCount);
            m_cmdLines.add(cmdln);
            m_procBuilder.command().clear();
            List<String> cmdlnList = cmdln.createCommandLine();
            String cmdLineFull = "Start cmd host=" + String.valueOf(hostId) + " :";
            for (String element : cmdlnList) {
                assert(element != null);
                cmdLineFull += " " + element;
            }
            log.info(cmdLineFull);
            System.out.println(cmdLineFull);
            m_procBuilder.command().addAll(cmdlnList);

            // write output to obj/release/testoutput/<test name>-n.txt
            // this may need to be more unique? Also very useful to just
            // set this to a hardcoded path and use "tail -f" to debug.
            String testoutputdir = cmdln.buildDir() + File.separator + "testoutput";
            System.out.println("Process output will be redirected to: " + testoutputdir);
            // make sure the directory exists
            File dir = new File(testoutputdir);
            if (dir.exists()) {
                assert (dir.isDirectory());
            }
            else {
                boolean status = dir.mkdirs();
                assert (status);
            }

            File dirFile = new VoltFile(testoutputdir);
            if (dirFile.listFiles() != null) {
                for (File f : dirFile.listFiles()) {
                    if (f.getName().startsWith(getName() + "-" + hostId)) {
                        f.delete();
                    }
                }
            }

            Process proc = m_procBuilder.start();
            m_cluster.add(proc);
            String fileName = testoutputdir
                    + File.separator
                    + "LC-"
                    + getFileName() + "-"
                    + m_clusterId + "-"
                    + hostId + "-"
                    + "idx" + String.valueOf(perLocalClusterExtProcessIndex++)
                    + ".txt";
            System.out.println("Process output can be found in: " + fileName);

            if (m_logMessageMatchPatterns == null) {
                ptf = new PipeToFile(
                        fileName,
                        proc.getInputStream(),
                        PipeToFile.m_initToken,
                        false,
                        proc);
            } else {
                if (m_logMessageMatchResults.containsKey(hostId)) {
                    if (resetLogMessageMatchResults) {
                        resetLogMessageMatchResults(hostId);
                    }
                } else {
                    m_logMessageMatchResults.put(hostId, new ConcurrentHashSet<>());
                }
                ptf = new PipeToFile(
                        fileName,
                        proc.getInputStream(),
                        PipeToFile.m_initToken,
                        false,
                        proc,
                        m_logMessageMatchPatterns,
                        m_logMessageMatchResults.get(hostId));
                ptf.setHostId(hostId);
            }

            m_pipes.add(ptf);
            ptf.setName("ClusterPipe:" + String.valueOf(hostId));
            ptf.start();
        }
        catch (IOException ex) {
            log.error("Failed to start cluster process:" + ex.getMessage(), ex);
            assert (false);
        }

        if (waitForReady && (startAction == StartAction.JOIN || startAction == StartAction.PROBE || startAction == StartAction.REJOIN)) {
            waitOnPTFReady(ptf, true, System.currentTimeMillis(), System.currentTimeMillis(), hostId);
        }

        if (hostId > (m_hostCount - 1)) {
            m_hostCount++;
            this.m_compiled = false; //Host count changed, should recompile
        }
    }

    public void setNumberOfCoordinators(int i) {
        checkArgument(i > 0 && i <= m_hostCount,
                "coordinators count %s must be greater than 0, and less or equal to host count %s",
                i, m_hostCount);
        numberOfCoordinators = i;
    }

    /**
     * Use the weird portable java way to figure out if a cluster is alive
     */
    private boolean isProcessDead(Process p) {
        try {
            p.exitValue();
            return true; // if no exception, process died
        }
        catch (IllegalThreadStateException e) {
            return false; // process is still alive
        }
    }

    public boolean recoverOne(int hostId, Integer portOffset, String rejoinHost, boolean liveRejoin) {
        StartAction startAction = isNewCli ? StartAction.PROBE : (liveRejoin ? StartAction.LIVE_REJOIN : StartAction.REJOIN);
        return recoverOne(
                false,
                0,
                hostId,
                portOffset,
                rejoinHost,
                startAction);
    }

    public void joinOne(int hostId) {
        try {
            if (isNewCli && !m_hostRoots.containsKey(Integer.toString(hostId))) {
                initLocalServer(hostId, true);
            }
            startOne(hostId, true, StartAction.JOIN, true, null);
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void joinOne(int hostId, String placementGroup) {
        try {
            if (isNewCli && !m_hostRoots.containsKey(Integer.toString(hostId))) {
                initLocalServer(hostId, true);
            }
            startOne(hostId, true, StartAction.JOIN, true, placementGroup);
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    //create a new node and join to the cluster via rejoin
    public void rejoinOne(int hostId) {
        try {
            if (isNewCli && !m_hostRoots.containsKey(Integer.toString(hostId))) {
                initLocalServer(hostId, true);
            }
            startOne(hostId, true, StartAction.REJOIN, true, null);
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /**
     * join multiple nodes to the cluster
     * @param hostIds a set of new host ids
     */
    public void join(Set<Integer> hostIds) {
        for (int hostId : hostIds) {
            try {
                if (isNewCli && !m_hostRoots.containsKey(Integer.toString(hostId))) {
                    initLocalServer(hostId, true);
                }
                startOne(hostId, true, StartAction.JOIN, false, null);
            }
            catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        waitForAllReady();
    }

    /**
     * join multiple nodes to the cluster under their placement groups
     * @param hostIds a set of new host ids and their placement groups
     */
    public void join(Map<Integer, String> hostIdByPlacementGroup) {
        for (Map.Entry<Integer, String> entry : hostIdByPlacementGroup.entrySet()) {
            try {
                if (isNewCli && !m_hostRoots.containsKey(Integer.toString(entry.getKey()))) {
                    initLocalServer(entry.getKey(), true);
                }
                startOne(entry.getKey(), true, StartAction.JOIN, false, entry.getValue());
                if (m_deplayBetweenNodeStartupMS > 0) {
                    try {
                        Thread.sleep(m_deplayBetweenNodeStartupMS);
                    } catch (InterruptedException e) {
                    }
                }
            }
            catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        waitForAllReady();
    }

    public boolean recoverOne(int hostId, Integer portOffset, String rejoinHost) {
        return recoverOne(false, 0, hostId, portOffset, rejoinHost, StartAction.REJOIN);
    }

    private boolean recoverOne(boolean logtime, long startTime, int hostId) {
        return recoverOne( logtime, startTime, hostId, null, "", StartAction.REJOIN);
    }

    // Re-start a (dead) process. HostId is the enumeration of the host
    // in the cluster (0, 1, ... hostCount-1) -- not an hsid, for example.
    private boolean recoverOne(boolean logtime, long startTime, int hostId, Integer rejoinHostId,
                               String rejoinHost, StartAction startAction) {
        // Lookup the client interface port of the rejoin host
        // I have no idea why this code ignores the user's input
        // based on other state in this class except to say that whoever wrote
        // it this way originally probably eats kittens and hates cake.
        if (rejoinHostId == null || (m_hasLocalServer && hostId != 0)) {
            rejoinHostId = 0;
        }
        if (isNewCli) {
            //If this is new CLI we use probe
            startAction = StartAction.PROBE;
        }
        int portNoToRejoin = m_cmdLines.get(rejoinHostId).internalPort();

        if (hostId == 0 && m_hasLocalServer) {
            templateCmdLine.leaderPort(portNoToRejoin);
            try {
                startLocalServer(rejoinHostId, false, startAction);
                m_localServer.waitForRejoin();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            return true;
        }

        // For some mythical reason rejoinHostId is not actually used for the newly created host,
        // hostNum is used by default (in fact hostNum should equal to hostId, otherwise some tests
        // may fail)
        log.info("Rejoining " + hostId + " to hostID: " + rejoinHostId);

        // rebuild the EE proc set.
        if (templateCmdLine.target().isIPC && m_eeProcs.contains(hostId)) {
            EEProcess eeProc = m_eeProcs.get(hostId);
            File valgrindOutputFile = null;
            try {
                valgrindOutputFile = eeProc.waitForShutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            failIfValgrindErrors(valgrindOutputFile);
        }
        if (templateCmdLine.target().isIPC) {
            String logfile = "LocalCluster_host_" + hostId + ".log";
            m_eeProcs.set(hostId, new EEProcess(templateCmdLine.target(), m_siteCount, logfile));
        }

        PipeToFile ptf = null;
        long start = 0;
        try {
            CommandLine rejoinCmdLn = m_cmdLines.get(hostId);
            rejoinCmdLn.setForceVoltdbCreate(false);
            // some tests need this
            rejoinCmdLn.javaProperties = templateCmdLine.javaProperties;
            rejoinCmdLn.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));

            rejoinCmdLn.startCommand(startAction);
            rejoinCmdLn.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));

            // This shouldn't collide but apparently it sucks.
            // Bump it to avoid collisions on rejoin.
            if (m_debug) {
                rejoinCmdLn.debugPort(portGenerator.next());
            }
            rejoinCmdLn.leader(rejoinHost + ":" + String.valueOf(portNoToRejoin));

            rejoinCmdLn.m_port = portGenerator.nextClient();
            rejoinCmdLn.m_adminPort = portGenerator.nextAdmin();
            rejoinCmdLn.m_httpPort = portGenerator.nextHttp();
            rejoinCmdLn.m_zkInterface = "127.0.0.1:" + portGenerator.next();
            rejoinCmdLn.m_internalPort = internalPortGenerator.nextInternalPort(hostId);
            rejoinCmdLn.m_coordinators = internalPortGenerator.getCoordinators();
            setPortsFromConfig(hostId, rejoinCmdLn);
            if (this.m_additionalProcessEnv != null) {
                for (String name : this.m_additionalProcessEnv.keySet()) {
                    rejoinCmdLn.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
                }
            }

            //rejoin can hotfix
            if ((m_versionOverrides != null) && (m_versionOverrides.length > hostId)) {
                assert(m_versionOverrides[hostId] != null);
                assert(m_versionCheckRegexOverrides[hostId] != null);
                rejoinCmdLn.m_versionStringOverrideForTest = m_versionOverrides[hostId];
                rejoinCmdLn.m_versionCompatibilityRegexOverrideForTest = m_versionCheckRegexOverrides[hostId];
                if ((m_buildStringOverrides != null) && (m_buildStringOverrides.length > hostId)) {
                    assert(m_buildStringOverrides[hostId] != null);
                    rejoinCmdLn.m_buildStringOverrideForTest = m_buildStringOverrides[hostId];
                }
            }
            //Rejoin does not do paused mode.

            //Rejoin mixed sitesperhost
            if ((m_sitesperhostOverrides != null) && (m_sitesperhostOverrides.size() > hostId)) {
                assert(m_sitesperhostOverrides.containsKey(hostId));
                rejoinCmdLn.m_sitesperhost = m_sitesperhostOverrides.get(hostId);
            }

            List<String> rejoinCmdLnStr = rejoinCmdLn.createCommandLine();
            String cmdLineFull = "Rejoin cmd line:";
            for (String element : rejoinCmdLnStr) {
                cmdLineFull += " " + element;
            }
            log.info(cmdLineFull);

            m_procBuilder.command().clear();
            m_procBuilder.command().addAll(rejoinCmdLnStr);
            Process proc = m_procBuilder.start();
            start = System.currentTimeMillis();

            // write output to obj/release/testoutput/<test name>-n.txt
            // this may need to be more unique? Also very useful to just
            // set this to a hardcoded path and use "tail -f" to debug.
            String testoutputdir = rejoinCmdLn.buildDir() + File.separator + "testoutput";
            // make sure the directory exists
            File dir = new File(testoutputdir);
            if (dir.exists()) {
                assert(dir.isDirectory());
            }
            else {
                boolean status = dir.mkdirs();
                assert(status);
            }

            String filePath = testoutputdir +
                              File.separator +
                              "LC-" +
                              getFileName() + "-" +
                              hostId + "-" +
                              "idx" + String.valueOf(perLocalClusterExtProcessIndex++) +
                              ".rejoined.txt";

            if (m_logMessageMatchPatterns == null) {
                ptf = new PipeToFile(
                        filePath,
                        proc.getInputStream(),
                        PipeToFile.m_initToken,
                        false,
                        proc);
            } else {
                if (m_logMessageMatchResults.containsKey(hostId)) {
                    resetLogMessageMatchResults(hostId);
                } else {
                    m_logMessageMatchResults.put(hostId, new ConcurrentHashSet<>());
                }
                ptf = new PipeToFile(
                        filePath,
                        proc.getInputStream(),
                        PipeToFile.m_initToken,
                        false,
                        proc,
                        m_logMessageMatchPatterns,
                        m_logMessageMatchResults.get(hostId));
                ptf.setHostId(hostId);
            }

            synchronized (this) {
                m_pipes.set(hostId, ptf);
                // replace the existing dead proc
                m_cluster.set(hostId, proc);
                m_cmdLines.set(hostId, rejoinCmdLn);
            }
            Thread t = new Thread(ptf);
            t.setName("ClusterPipe:" + String.valueOf(hostId));
            t.start();
        }
        catch (IOException ex) {
            log.error("Failed to start recovering cluster process:" + ex.getMessage(), ex);
            assert (false);
        }
        m_running = true;
        return waitOnPTFReady(ptf, logtime, startTime, start, hostId);
    }

    /*
     * Wait for the PTF to report initialization/rejoin
     */
    private boolean waitOnPTFReady(PipeToFile ptf, boolean logtime, long startTime, long start, int hostId) {
        // wait for the joining site to be ready
        synchronized (ptf) {
            if (logtime) System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
            while (ptf.m_witnessedReady.get() != true) {
                // if eof, then no point in waiting around
                if (ptf.m_eof.get()) {
                    System.out.println("PipeToFile: Reported EOF");
                    break;
                }
                // if process is dead, no point in waiting around
                if (isProcessDead(ptf.getProcess())) {
                    System.out.println("PipeToFile: Reported Dead Process");
                    break;
                }
                try {
                    // wait for explicit notification
                    ptf.wait(1000);
                }
                catch (InterruptedException ex) {
                    log.error(ex.toString(), ex);
                }
            }
        }
        if (ptf.m_witnessedReady.get()) {
            long finish = System.currentTimeMillis();
            log.info("Took " + (finish - start) +
                     " milliseconds, time from init was " + (finish - ptf.m_initTime));
            return true;
        }

        log.info("Recovering process exited before recovery completed");
        try {
            silentKillSingleHost(hostId, true);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    synchronized public void shutdownSave(Client adminClient) throws IOException {
        ClientResponse resp = null;
        try {
            resp = adminClient.callProcedure("@PrepareShutdown");
        } catch (ProcCallException e) {
            throw new IOException(e.getCause());
        }
        if (resp == null) {
            throw new IOException("Failed to prepare for shutdown.");
        }
        final long sigil = resp.getResults()[0].asScalarLong();

        long sum = Long.MAX_VALUE;
        while (sum > 0) {
            try {
                resp = adminClient.callProcedure("@Statistics", "liveclients", 0);
            } catch (ProcCallException e) {
                throw new IOException(e.getCause());
            }
            VoltTable t = resp.getResults()[0];
            long trxn=0, bytes=0, msg=0;
            if (t.advanceRow()) {
                trxn = t.getLong(6);
                bytes = t.getLong(7);
                msg = t.getLong(8);
                sum =  trxn + bytes + msg;
            }
            System.out.printf("Outstanding transactions: %d, buffer bytes :%d, response messages:%d\n", trxn, bytes, msg);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                ;
            }
        }
        if (sum != 0) {
            throw new IOException("Failed to clear any pending transactions.");
        }

        try{
            resp = adminClient.callProcedure("@Shutdown", sigil);
        } catch (ProcCallException e) {
            ;
        }
        System.out.println("@Shutdown: cluster has been shutdown via admin mode and last snapshot saved.");
    }

    @Override
    synchronized public void shutDown() throws InterruptedException {
        // there are couple of ways to shutdown. sysproc @kill could be
        // issued to listener. this would require that the test didn't
        // break the cluster somehow.  Or ... just old fashioned kill?

        try {
            if (m_localServer != null) {
                m_localServer.shutdown();
            }
        }
        catch (Exception e) {
            log.error("Failure to shutdown LocalCluster's in-process VoltDB server.", e);
        }
        finally {
            m_running = false;
        }
        shutDownExternal();

        VoltServerConfig.removeInstance(this);
    }

    public void killSingleHost(int hostNum) throws InterruptedException
    {
        log.info("Killing " + hostNum);
        if (hostNum == 0 && m_localServer != null) {
            m_localServer.shutdown();
        }
        else {
            silentKillSingleHost(hostNum, false);
        }
    }

    private void silentKillSingleHost(int hostNum, boolean forceKillEEProcs) throws InterruptedException {
        Process proc = null;
        //PipeToFile ptf = null;
        EEProcess eeProc = null;
        PipeToFile ptf;
        synchronized (this) {
           proc = m_cluster.get(hostNum);
           //ptf = m_pipes.get(hostNum);
           m_cluster.set(hostNum, null);
           ptf = m_pipes.get(hostNum);
           m_pipes.set(hostNum, null);
           if (m_eeProcs.size() > hostNum) {
               eeProc = m_eeProcs.get(hostNum);
           }
        }

        if (ptf != null && ptf.m_filename != null) {
            //new File(ptf.m_filename).delete();
        }
        if (proc != null) {
            proc.destroy();
            proc.waitFor();
        }

        // if (ptf != null) {
        //     new File(ptf.m_filename).delete();
        // }

        if (eeProc != null) {
            if (forceKillEEProcs) {
                eeProc.destroy();
            }
            File valgrindOutputFile = eeProc.waitForShutdown();
            failIfValgrindErrors(valgrindOutputFile);
        }
    }

    public void shutDownExternal() throws InterruptedException {
        shutDownExternal(false);
    }

    public void waitForNodesToShutdown() {
        if (m_cluster != null) {

            // join on all procs
            for (Process proc : m_cluster) {
                if (proc == null)
                    continue;
                int retval = 0;
                try {
                    retval = proc.waitFor();
                }
                catch (InterruptedException e) {
                    log.error("Unable to wait for Localcluster process to die: " + proc.toString(), e);
                }
                // exit code 143 is the forcible shutdown code from .destroy()
                if (retval != 0 && retval != 143)
                {
                    log.error("External VoltDB process terminated abnormally with return: " + retval);
                }
            }
        }

        if (m_cluster != null) m_cluster.clear();

        for (EEProcess proc : m_eeProcs) {
            File valgrindOutputFile = null;
            try {
                valgrindOutputFile = proc.waitForShutdown();
            }
            catch (InterruptedException e) {
                log.error("Unable to wait for EEProcess to die: " + proc.toString(), e);
            }

            failIfValgrindErrors(valgrindOutputFile);
        }

        m_eeProcs.clear();

        m_running = false;

    }

    public synchronized void shutDownExternal(boolean forceKillEEProcs)
    {
        if (m_cluster != null) {
            // kill all procs
            for (Process proc : m_cluster) {
                if (proc == null)
                    continue;
                proc.destroy();
            }
        }
        waitForNodesToShutdown();
    }

    @Override
    public String getListenerAddress(int hostId) {
        return getListenerAddress(hostId, false);
    }

    @Override
    public String getAdminAddress(int hostId) {
        return getListenerAddress(hostId, true);
    }

    private String getListenerAddress(int hostId, boolean useAdmin) {
        if (!m_running) {
            return null;
        }
        for (int i = 0; i < m_cmdLines.size(); i++) {
            CommandLine cl = m_cmdLines.get(i);
            String hostIdStr = cl.getJavaProperty(clusterHostIdProperty);

            if (hostIdStr.equals(String.valueOf(hostId))) {
                Process p = m_cluster.get(i);
                // if the process is alive, or is the in-process server
                if ((p != null) || (i == 0 && m_hasLocalServer)) {
                    return "localhost:" + (useAdmin ? cl.m_adminPort : cl.m_port);
                }
            }
        }
        return null;
    }

    @Override
    public int getListenerCount() {
        return m_cmdLines.size();
    }

    @Override
    public List<String> getListenerAddresses() {
        if (!m_running) {
            return null;
        }
        ArrayList<String> listeners = new ArrayList<>();
        for (int i = 0; i < m_cmdLines.size(); i++) {
            CommandLine cl = m_cmdLines.get(i);
            Process p = m_cluster.get(i);
            // if the process is alive, or is the in-process server
            if ((p != null) || (i == 0 && m_hasLocalServer)) {
                listeners.add("localhost:" + cl.m_port);
            }
        }
        return listeners;
    }

    /**
     * This is used in generating the cluster name, to
     * avoid name conflicts between LocalCluster instances
     * that have the same site-host-Kfactor configuration,
     * but have other configuration differences.  This could
     * be used to differentiate between LocalCluster instances
     * with different initial JVM properties through m_additionalProcessEnv,
     * for example.
     * @param prefix
     */
    public void setPrefix(String prefix) {
        m_prefix  = prefix;
    }

    @Override
    public String getName() {
        String prefix = (m_prefix == null) ? "localCluster" : String.format("localCluster-%s", m_prefix);
        if (m_failureState == FailureState.ONE_FAILURE)
            prefix += "OneFail";
        if (m_failureState == FailureState.ONE_RECOVERING)
            prefix += "OneRecov";
        return prefix +
            "-" + String.valueOf(m_siteCount) +
            "-" + String.valueOf(m_hostCount) +
            "-" + templateCmdLine.target().display.toUpperCase();
    }

    String getFileName() {
        String prefix = m_callingClassName + "-" + m_callingMethodName;
        if (m_failureState == FailureState.ONE_FAILURE)
            prefix += "-OneFail";
        if (m_failureState == FailureState.ONE_RECOVERING)
            prefix += "-OneRecov";
        return prefix +
            "-" + String.valueOf(m_siteCount) +
            "-" + String.valueOf(m_hostCount) +
            "-" + templateCmdLine.target().display.toUpperCase();
    }

    @Override
    public int getNodeCount()
    {
        return m_hostCount;
    }

    public boolean areAllNonLocalProcessesDead() {
        for (Process proc : m_cluster){
            try {
                if (proc != null) {
                    proc.exitValue();
                }
            }
            catch (IllegalThreadStateException ex) {
                return false;
            }
        }
        return true;
    }

    public int getLiveNodeCount()
    {
        int count = 0;
        if (m_hasLocalServer)
        {
            count++;
        }

        if (m_cluster != null)
        {
            for (Process proc : m_cluster)
            {
                try
                {
                    if (proc != null)
                    {
                        proc.exitValue();
                    }
                }
                catch (IllegalThreadStateException ex)
                {
                    // not dead yet!
                    count++;
                }
            }
        }

        return count;
    }

    public int getBlessedPartitionDetectionProcId() {
        int currMin = Integer.MAX_VALUE;
        int currMinIdx = 0;
        for (int i = 0; i < m_pipes.size(); i++) {
            PipeToFile p = m_pipes.get(i);
            System.out.println("Index " + i + " had hostid: " + p.getHostId());
            if (p.getHostId() < currMin) {
                currMin = p.getHostId();
                currMinIdx = i;
                System.out.println("Setting index: " + i + " to blessed.");
            }
        }
        return currMinIdx;
    }

    @Override
    public void finalize() throws Throwable {
        try {
            shutDownExternal();
        }
        finally {
            super.finalize();
        }
    }

    class ShutDownHookThread implements Runnable {
        @Override
        public void run() {
            shutDownExternal(true);
        }
    }

    @Override
    public boolean isHSQL() {
        return templateCmdLine.target() == BackendTarget.HSQLDB_BACKEND;
    }

    public void setOverridesForHotfix(String[] versions, String[] regexOverrides, String[] buildStrings) {
        assert(buildStrings != null);

        m_buildStringOverrides = buildStrings;
        setOverridesForHotfix(versions, regexOverrides);
    }

    public void setOverridesForHotfix(String[] versions, String[] regexOverrides) {
        assert(versions != null);
        assert(regexOverrides != null);
        assert(versions.length == regexOverrides.length);

        m_versionOverrides = versions;
        m_versionCheckRegexOverrides = regexOverrides;
    }

    public void setOverridesForModes(String[] modes) {
        assert(modes != null);

        m_modeOverrides = modes;
    }

    public void setOverridesForSitesperhost(Map<Integer, Integer> sphMap) {
        assert(sphMap != null);
        assert(!sphMap.isEmpty());

        m_sitesperhostOverrides = sphMap;
    }

    public void setPlacementGroups(String[] placementGroups) {
        this.m_placementGroups = placementGroups;
    }
    @Override
    public void setMaxHeap(int heap) {
        templateCmdLine.setMaxHeap(heap);
    }

    public String getPathToDeployment() {
        return templateCmdLine.pathToDeployment();
    }

    public String zkinterface(int hostId) {
        return m_cmdLines.get(hostId).zkinterface();
    }

    public int drAgentStartPort(int hostId) {
        return m_cmdLines.get(hostId).drAgentStartPort();
    }

    public int internalPort(int hostId) {
        return m_cmdLines.get(hostId).internalPort();
    }

    public NavigableSet<String> coordinators(int hostId) {
        return m_cmdLines.get(hostId).coordinators();
    }

    public int port(int hostId) {
        return m_cmdLines.get(hostId).port();
    }

    public int httpPort(int hostId) {
        return m_cmdLines.get(hostId).httpPort();
    }

    public int adminPort(int hostId) {
        return m_cmdLines.get(hostId).adminPort();
    }

    public void setPortsFromConfig(int hostId, VoltDB.Configuration config) {
        CommandLine cl = m_cmdLines.get(hostId);
        assert(cl != null);
        cl.m_port = config.m_port;
        cl.m_adminPort = config.m_adminPort;
        cl.m_zkInterface = config.m_zkInterface;
        cl.m_internalPort = config.m_internalPort;
        cl.m_leader = config.m_leader;
        cl.m_coordinators = ImmutableSortedSet.copyOf(config.m_coordinators);
    }

    public static boolean isMemcheckDefined() {
        final String buildType = System.getenv().get("BUILD");
        if (buildType == null) {
            return false;
        }
        return buildType.toLowerCase().startsWith("memcheck");
    }

    @Override
    public boolean isValgrind() {
        System.out.println("----templateCmdLine.m_backend=" + templateCmdLine.m_backend);
        return templateCmdLine.m_backend == BackendTarget.NATIVE_EE_VALGRIND_IPC;
    }

    public static boolean isDebugDefined() {
        final String buildType = System.getenv().get("BUILD");
        if (buildType == null) {
            return false;
        }
        return buildType.toLowerCase().startsWith("debug");
    }

    @Override
    public boolean isDebug() {
        return isDebugDefined();
    }

    @Override
    public void createDirectory(File path) throws IOException {
        for (File root : m_subRoots) {
            File actualPath = new File(root, path.getPath());
            if (!actualPath.mkdirs()) {
                throw new IOException();
            }
        }
    }

    @Override
    public void deleteDirectory(File path) throws IOException {
        for (File root : m_subRoots) {
            File actualPath = new File(root, path.getPath());
            VoltFile.recursivelyDelete(actualPath);
        }
    }

    @Override
    public ArrayList<File> listFiles(File path) throws IOException {
        ArrayList<File> files = new ArrayList<>();
        for (File root : m_subRoots) {
            File actualPath = new File(root, path.getPath());
            for (File f : actualPath.listFiles()) {
                files.add(f);
            }
        }
        return files;
    }

    @Override
    public File[] getPathInSubroots(File path) throws IOException {
        File retval[] = new File[m_subRoots.size()];
        for (int ii = 0; ii < m_subRoots.size(); ii++) {
            retval[ii] = new File(m_subRoots.get(ii), path.getPath());
        }
        return retval;
    }

    /**
     * @return the m_expectedToCrash
     */
    public boolean isExpectedToCrash() {
        return m_expectedToCrash;
    }

    /**
     * @param m_expectedToCrash the m_expectedToCrash to set
     */
    public void setExpectedToCrash(boolean expectedToCrash) {
        this.m_expectedToCrash = expectedToCrash;
    }

    /**
     * @return the m_expectedToInitialize
     */
    public boolean isExpectedToInitialize() {
        return m_expectedToInitialize;
    }

    /**
     * @param m_expectedToInitialize the m_expectedToInitialize to set
     */
    public void setExpectedToInitialize(boolean expectedToInitialize) {
        this.m_expectedToInitialize = expectedToInitialize;
    }

    /**
     * @param watcher watcher to attach to active output pipes
     */
    public void setOutputWatcher(OutputWatcher watcher) {
        for (PipeToFile pipe : m_pipes) {
            if (pipe != null) {
                pipe.setWatcher(watcher);
            }
        }
    }

    @Override
    public int getLogicalPartitionCount() {
        return (m_siteCount * m_hostCount) / (m_kfactor + 1);
    }

    @Override
    public int getKfactor() {
        return m_kfactor;
    }

    /**
     * Parse the output file produced by valgrind and produce a JUnit failure if
     * valgrind found any errors.
     *
     * Deletes the valgrind file if there are no errors.
     *
     * @param valgrindOutputFile
     */
    public static void failIfValgrindErrors(File valgrindOutputFile) {
        if (valgrindOutputFile == null) {
            return;
        }

        List<String> valgrindErrors = new ArrayList<>();
        ValgrindXMLParser.processValgrindOutput(valgrindOutputFile, valgrindErrors);
        if (!valgrindErrors.isEmpty()) {
            String failString = "";
            for (final String error : valgrindErrors) {
                failString = failString + "\n" +  error;
            }
            org.junit.Assert.fail(failString);
        }
        else {
            valgrindOutputFile.delete();
        }
    }

    // Use this for optionally enabling localServer in one of the DR clusters (usually for debugging)
    public static LocalCluster createLocalCluster(String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
                                                  int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
                                                  DrRoleType drRole, boolean hasLocalServer) throws IOException {
        return createLocalCluster(schemaDDL, siteCount, hostCount, kfactor, clusterId, replicationPort, remoteReplicationPort,
                pathToVoltDBRoot, jar, drRole, hasLocalServer, null, null);
    }

    public static LocalCluster createLocalCluster(String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
                                                  int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
                                                  DrRoleType drRole, boolean hasLocalServer, VoltProjectBuilder builder) throws IOException {
        return createLocalCluster(schemaDDL, siteCount, hostCount, kfactor, clusterId, replicationPort, remoteReplicationPort,
                pathToVoltDBRoot, jar, drRole, hasLocalServer, builder, null);
    }

    public static LocalCluster createLocalCluster(String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
                                                  int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
                                                  DrRoleType drRole, boolean hasLocalServer, VoltProjectBuilder builder,
                                                  String callingMethodName) throws IOException {
        return createLocalCluster(schemaDDL, siteCount, hostCount, kfactor, clusterId, replicationPort, remoteReplicationPort,
                           pathToVoltDBRoot, jar, drRole, hasLocalServer, builder, callingMethodName, false, null);
    }

    public static LocalCluster createLocalCluster(String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
                                                  int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
                                                  DrRoleType drRole, boolean hasLocalServer, VoltProjectBuilder builder,
                                                  String callingMethodName,
                                                  boolean enableSPIMigration,
                                                  Map<String, String> javaProps) throws IOException {
        if (builder == null) builder = new VoltProjectBuilder();
        LocalCluster lc = compileBuilder(schemaDDL, siteCount, hostCount, kfactor, clusterId,
                replicationPort, remoteReplicationPort, pathToVoltDBRoot, jar, drRole, builder, callingMethodName);

        System.out.println("Starting local cluster.");
        lc.setHasLocalServer(hasLocalServer);
        lc.overrideAnyRequestForValgrind();
        lc.setJavaProperty("DR_QUERY_INTERVAL", "5");
        lc.setJavaProperty("DR_RECV_TIMEOUT", "5000");
        // temporary, until we always enable SPI migration
        if (enableSPIMigration) {
            lc.setJavaProperty("DISABLE_MIGRATE_PARTITION_LEADER", "false");
        }
        if (javaProps != null)
        for (Map.Entry<String, String> prop : javaProps.entrySet()) {
            lc.setJavaProperty(prop.getKey(), prop.getValue());
        }
        if (!lc.isNewCli()) {
            lc.setDeploymentAndVoltDBRoot(builder.getPathToDeployment(), pathToVoltDBRoot);
            lc.startUp(false);
        } else {
            lc.startUp(true);
        }

        for (int i = 0; i < hostCount; i++) {
            System.out.printf("Local cluster node[%d] ports: %d, %d, %d, %d\n",
                    i, lc.internalPort(i), lc.adminPort(i), lc.port(i), lc.drAgentStartPort(i));
        }
        return lc;
    }

    public void compileDeploymentOnly(VoltProjectBuilder voltProjectBuilder) {
        // NOTE: voltDbRoot must be set prior to calling this method if you care about it.
        // When this method was written no users cared about the deployment's voltdbroot path,
        // since staged catalog tests use multi-node clusters with node specific voltdbroots.
        templateCmdLine.pathToDeployment(voltProjectBuilder.compileDeploymentOnly(m_voltdbroot, m_hostCount, m_siteCount, m_kfactor, m_clusterId));
        m_compiled = true;
    }

    public static LocalCluster compileBuilder(String schemaDDL, int siteCount, int hostCount,
                                              int kfactor, int clusterId, int replicationPort,
                                              int remoteReplicationPort, String pathToVoltDBRoot, String jar,
                                              DrRoleType drRole, VoltProjectBuilder builder,
                                              String callingMethodName) throws IOException {
        builder.addLiteralSchema(schemaDDL);
        if (drRole == DrRoleType.REPLICA) {
            builder.setDrReplica();
        } else if (drRole == DrRoleType.XDCR) {
            builder.setXDCR();
        }
        if (remoteReplicationPort != 0) {
            builder.setDRMasterHost("localhost:" + remoteReplicationPort);
        }
        builder.setUseDDLSchema(true);
        LocalCluster lc = new LocalCluster(jar, siteCount, hostCount, kfactor, clusterId, BackendTarget.NATIVE_EE_JNI, false);
        lc.setReplicationPort(replicationPort);
        if (callingMethodName != null) {
            lc.setCallingMethodName(callingMethodName);
        }
        assert(lc.compile(builder, pathToVoltDBRoot));
        return lc;
    }

    public Client createClient(ClientConfig config) throws IOException {
        Client client = ClientFactory.createClient(config);
        for (String address : getListenerAddresses()) {
            client.createConnection(address);
        }
        return client;
    }

    public Client createAdminClient(ClientConfig config) throws IOException {
        Client client = ClientFactory.createClient(config);
        client.createConnection(getAdminAddress(0));
        return client;
    }

    public void setDeplayBetweenNodeStartup(long deplayBetweenNodeStartup) {
        m_deplayBetweenNodeStartupMS = deplayBetweenNodeStartup;
    }

    // Reset the message match result
    public void resetLogMessageMatchResult(int hostNum, String regex) {
        assert(m_logMessageMatchPatterns != null);
        assert(m_logMessageMatchResults.containsKey(hostNum));
        assert(m_logMessageMatchPatterns.containsKey(regex));
        m_logMessageMatchResults.get(hostNum).remove(regex);
    }

    // Reset all the message match results
    public void resetLogMessageMatchResults() {
        m_logMessageMatchResults.values().stream().forEach(m -> m.clear());
    }

    // verify the presence of message in the log from specified host
    public boolean verifyLogMessage(int hostNum, String regex) {
        assertTrue(m_logMessageMatchPatterns != null);
        assertTrue(m_logMessageMatchResults.containsKey(hostNum));
        assertTrue(m_logMessageMatchPatterns.containsKey(regex));
        return m_logMessageMatchResults.get(hostNum).contains(regex);
    }

    // verify the presence of messages in the log from specified host
    private boolean logMessageContains(int hostId, List<String> patterns) {
        return patterns.stream().allMatch(s -> verifyLogMessage(hostId, s));
    }

    private boolean logMessageNotContains(int hostId, List<String> patterns) {
        return patterns.stream().allMatch(s -> !verifyLogMessage(hostId, s));
    }

    // Verify that the patterns provided exist in all the specified hosts
    // These patterns should have been added when constructing the class
    public boolean verifyLogMessages(List<Integer> hostIds, List<String> patterns) {
        return hostIds.stream().allMatch(id -> logMessageContains(id, patterns));
    }

    // Verify that none of the patterns provided exist in any of the specified hosts
    // These patterns should have been added when constructing the class
    public boolean verifyLogMessagesNotExist(List<Integer> hostIds, List<String> patterns) {
        return hostIds.stream().allMatch(id -> logMessageNotContains(id, patterns));
    }

    // verify that all the patterns exist in every host
    public boolean verifyLogMessages(List<String> patterns) {
        return m_logMessageMatchResults.keySet().stream().allMatch(id -> logMessageContains(id, patterns));
    }

    // verify that none of the patterns exist in any of the host
    public boolean verifyLogMessagesNotExist(List<String> patterns) {
        return m_logMessageMatchResults.keySet().stream().allMatch(id -> logMessageNotContains(id, patterns));
    }

    // verify a single pattern exists in every host
    public boolean verifyLogMessage(String regex) {
        return verifyLogMessages(Arrays.asList(new String[] {regex}));
    }

    // verify the message does not exist in all the logs
    public boolean verifyLogMessageNotExist(String regex) {
        return verifyLogMessagesNotExist(Arrays.asList(new String[] {regex}));
    }

    private void resetLogMessageMatchResults(int hostId) {
        assertTrue(m_logMessageMatchResults.containsKey(hostId));
        m_logMessageMatchResults.get(hostId).clear();
    }
}
