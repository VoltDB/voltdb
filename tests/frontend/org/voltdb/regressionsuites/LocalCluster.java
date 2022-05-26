/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.voltcore.logging.VoltLogger;
import org.voltdb.BackendTarget;
import org.voltdb.NativeLibraryLoader;
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
import org.voltdb.export.ExporterVersion;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CommandLine;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltSnapshotFile;

import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;
import org.apache.commons.io.FileUtils;

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

    // define environment variable to log the wall of text, mostly classpath
    static final private boolean logFullCmdLine = (System.getenv("FULL_CMDLINE_LOGGING") != null);

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
    private boolean m_expectedToCrash = false;
    private boolean m_expectedToInitialize = true;
    int m_replicationPort = -1;
    private String m_drPublicHost;
    private int m_drPublicPort = -1;
    private String m_topicsPublicHost;
    private int m_topicsPublicPort = -1;

    // log message pattern match results by host
    private Map<Integer, Set<String>> m_logMessageMatchResults = new ConcurrentHashMap<>();
    // log message patterns
    private Map<String, Pattern> m_logMessageMatchPatterns = new ConcurrentHashMap<>();

    Map<String, String> m_hostRoots = new HashMap<>();
    Map<String, String> m_hostScratch = new HashMap<>();
    /** Gets the dedicated paths in the filesystem used as a root for each process.
     * Used with NewCLI.
     */
    public Map<String, String> getHostRoots() {
        return m_hostRoots;
    }

    // Dedicated paths in the filesystem to be used as a root for each process
    ArrayList<File> m_subRoots = new ArrayList<>();
    public ArrayList<File> getSubRoots() {
        ArrayList<File> flist = new ArrayList<>();
        m_hostRoots.values().forEach( i ->  {
            flist.add(new File(i));
        });
        return flist;
    }

    // This should be set to true by default, otherwise the client interface
    // may throw different types of exception in other tests, which results
    // to failure
    boolean m_hasLocalServer = true;
    public void setHasLocalServer(boolean hasLocalServer) {
        m_hasLocalServer = hasLocalServer;
    }

    boolean m_enableVoltSnapshotPrefix = false;
    public void setEnableVoltSnapshotPrefix(boolean enableVoltPrefix) {
        m_enableVoltSnapshotPrefix = enableVoltPrefix;
    }

    public void setExporterVersion(ExporterVersion exporterVersion) {
        templateCmdLine.m_exporterVersion = exporterVersion;
    }

    ArrayList<PipeToFile> m_pipes = null;
    ArrayList<CommandLine> m_cmdLines = null;
    ServerThread m_localServer = null;
    ProcessBuilder m_procBuilder;

    //wait before next node is started up in millisecond
    //to help matching the host id on the real cluster with the host id on the local
    //cluster
    private long m_delayBetweenNodeStartupMS = 0;
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

    private String[] m_versionOverrides = null;
    private String[] m_versionCheckRegexOverrides = null;
    private String[] m_buildStringOverrides = null;

    private String[] m_modeOverrides = null;
    private String[] m_placementGroups = null;

    // The base command line - each process copies and customizes this.
    // Each local cluster process has a CommandLine instance configured
    // with the port numbers and command line parameter value specific to that
    // instance.
    private final CommandLine templateCmdLine = new CommandLine(StartAction.PROBE);

    private boolean isEnableSSL = Boolean.parseBoolean(System.getProperty("ENABLE_SSL", System.getenv("ENABLE_SSL")));
    public boolean isEnableSSL() { return isEnableSSL; };
    public void setEnableSSL(boolean flag) {
        isEnableSSL = flag;
        templateCmdLine.m_sslEnable = flag;
        templateCmdLine.m_sslExternal = flag;
        templateCmdLine.m_sslInternal = flag;
    }

    private String m_prefix = null;
    private boolean m_isPaused = false;
    private boolean m_usesStagedSchema;

    private int m_httpOverridePort = -1;

    /** Schema to use on the mismatched node, or null to initialize a bare node. */
    private String m_mismatchSchema = null;
    /** Node to initialize with a different schema, or null to use the same schema on all nodes. */
    private Integer m_mismatchNode = null;
    /** Set of hosts which were removed from the cluster. This is a bit hacky because it just skips these hosts */
    private Set<Integer> m_removedHosts = Collections.emptySet();

    public void setHttpOverridePort(int port) {
        m_httpOverridePort = port;
    }
    public int getHttpOverridePort() { return m_httpOverridePort; };

    @Override
    public boolean isUsingCalcite() {
        return false;
    }

    /*
     * Enable pre-compiled regex search in logs
     */
    public void setLogSearchPatterns(List<String> regexes) {
        for (String s : regexes) {
            Pattern p = Pattern.compile(s);
            m_logMessageMatchPatterns.put(s, p);
        }
    }

    public LocalCluster(
            String jarFileName, int siteCount, int hostCount, int kfactor, BackendTarget target) {
        this(jarFileName, siteCount, hostCount, kfactor, target, null);
    }

    public LocalCluster(
            String jarFileName, int siteCount, int hostCount, int kfactor, BackendTarget target, int inactiveCount) {
        this(jarFileName, siteCount, hostCount, kfactor, target, null);
        this.m_missingHostCount = inactiveCount;
    }

    public LocalCluster(
            String jarFileName, int siteCount, int hostCount, int kfactor, BackendTarget target,
            Map<String, String> env) {
        this(jarFileName, siteCount, hostCount, kfactor, target, FailureState.ALL_RUNNING, false, env);
    }

    public LocalCluster(
            String jarFileName, int siteCount, int hostCount, int kfactor, int clusterId, BackendTarget target) {
        this(jarFileName, siteCount, hostCount, kfactor, clusterId, target,
                FailureState.ALL_RUNNING, false, null);
    }

    public LocalCluster(
            String jarFileName, int siteCount, int hostCount, int kfactor, BackendTarget target,
            FailureState failureState, boolean debug) {
        this(jarFileName, siteCount, hostCount, kfactor, target, failureState, debug, null);
    }

    public LocalCluster(
            String jarFileName, int siteCount, int hostCount, int kfactor, BackendTarget target,
            FailureState failureState, boolean debug, Map<String, String> env) {
        this(jarFileName, siteCount, hostCount, kfactor, 0, target, failureState, debug, env);
    }

    public LocalCluster(
            String jarFileName, int siteCount, int hostCount, int kfactor, int clusterId, BackendTarget target,
            FailureState failureState, boolean debug, Map<String, String> env) {
        this(null, null, jarFileName, siteCount, hostCount,
                kfactor, clusterId, target, failureState, debug, env);
    }

    public LocalCluster(
            String schemaToStage, String classesJarToStage, String catalogJarFileName, int siteCount, int hostCount,
            int kfactor, int clusterId, BackendTarget target, FailureState failureState, boolean debug,
            Map<String, String> env) {
        m_usesStagedSchema = schemaToStage != null || classesJarToStage != null;

        assert siteCount > 0 : "site count is less than 1";
        assert hostCount > 0 : "host count is less than 1";

        numberOfCoordinators = Math.max(kfactor + 1, Math.min(hostCount, 3));
        internalPortGenerator = new InternalPortGeneratorForTest(portGenerator, numberOfCoordinators);

        m_additionalProcessEnv = env == null ? new HashMap<>() : env;
        if (Boolean.getBoolean(NativeLibraryLoader.USE_JAVA_LIBRARY_PATH)) {
            // set use.javalib for LocalCluster so that Eclipse runs will be OK.
            m_additionalProcessEnv.put(NativeLibraryLoader.USE_JAVA_LIBRARY_PATH, "true");
        }
        // get the name of the calling class
        StackTraceElement[] traces = Thread.currentThread().getStackTrace();
        m_callingClassName = "UnknownClass";
        m_callingMethodName = "unknownMethod";
        //ArrayUtils.reverse(traces);
        int i;
        // skip all stack frames below this method
        for (i = 0; !traces[i].getClassName().equals(getClass().getName()); i++) {}
        // skip all stack frames from localcluster itself
        for (; traces[i].getClassName().equals(getClass().getName()); i++) {}
        // skip the package name
        int dot = traces[i].getClassName().lastIndexOf('.');
        m_callingClassName = traces[i].getClassName().substring(dot + 1);
        m_callingMethodName = traces[i].getMethodName();

        if (catalogJarFileName == null) {
            if (! m_usesStagedSchema) {
                log.info("Instantiating empty LocalCluster with class.method: " +
                        m_callingClassName + "." + m_callingMethodName);
            } else {
                log.info("Instantiating LocalCluster with schema and class.method: " +
                        m_callingClassName + "." + m_callingMethodName);
            }
        } else {
            assert !m_usesStagedSchema : "Cannot use OldCLI catalog with staged schema and/or classes";
            log.info("Instantiating LocalCluster for " + catalogJarFileName + " with class.method: " +
                    m_callingClassName + "." + m_callingMethodName);
        }
        log.info("ClusterId: " + clusterId + " Sites: " + siteCount + " Hosts: " + hostCount + " ReplicationFactor: " + kfactor);

        m_cluster.ensureCapacity(hostCount);

        m_siteCount = siteCount;
        m_hostCount = hostCount;
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
                templateCmdLine.m_userSchemas = Collections
                        .singletonList(VoltProjectBuilder.createFileForSchema(schemaToStage));
                log.info("LocalCluster staged schema as \"" + templateCmdLine.m_userSchemas + "\"");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (classesJarToStage != null) {
            templateCmdLine.m_stagedClassesPaths = Collections.singletonList(new VoltSnapshotFile(classesJarToStage));
            log.info("LocalCluster staged classes as \"" + templateCmdLine.m_stagedClassesPaths + "\"");
        }

        // if the user wants valgrind and it makes sense, give it to 'em
        // For now only one host works.
        if (isMemcheckDefined() && target.isValgrindable && m_hostCount == 1) {
            m_target = BackendTarget.NATIVE_EE_VALGRIND_IPC;
        } else {
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
        } else {
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
            startCommand(StartAction.PROBE).
            jarFileName(VoltDB.Configuration.getPathToCatalogForTest(m_jarFileName)).
            buildDir(buildDir).
            classPath(classPath).
            pathToLicense(ServerThread.getTestLicensePath()).
            log4j(log4j).
            setForceVoltdbCreate(true);
        if (javaLibraryPath != null) {
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
    public void setMismatchSchemaForInit( String mismatchSchema, Integer nodeID) {
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
     * Override the Valgrind backend with a JNI backend. Called after a constructor but before startup.
     * <p>
     * The biggest reason this is used is for tests which use snapshotting, which is not supported by the IPC EE
     * implementation.
     *
     * @deprecated If the test does not support valgrind it would be better to skip the test during memcheck test runs.
     *             Use {@link #isMemcheckDefined()} or {@link #isValgrind()} to test if this is a memcheck run.
     */
    @Deprecated
    public void overrideAnyRequestForValgrind() {
        if (templateCmdLine.m_backend.isValgrindTarget) {
            m_target = BackendTarget.NATIVE_EE_JNI;
            templateCmdLine.m_backend = BackendTarget.NATIVE_EE_JNI;
        }
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

    public void setCallingClassName(String name) {
        m_callingClassName = name;
    }

    public boolean compile(VoltProjectBuilder builder, final String voltRootPath) {
        if (! m_compiled) {
            m_initialCatalog = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor, voltRootPath, m_clusterId);
            m_compiled = m_initialCatalog != null;
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
            m_voltdbroot = builder.getPathToVoltRoot().getAbsolutePath();
            if (builder.isTopicsEnabled()) {
                templateCmdLine.setTopicsHostPort(HostAndPort.fromHost(""));
            }
        }
        return m_compiled;
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        return compile(builder, null);
    }

    /**
     * Update the catalog of a running instance. It is recommended that {@code builder} is the same builder which was
     * used to compile this instance with the appropriate modifications.
     *
     * @param builder {@link VoltProjectBuilder} with updated configuration.
     * @throws ProcCallException    If there was an error calling the procedure
     * @throws InterruptedException If this thread was interrupted
     * @throws IOException          If there was an error writing or reading the updated configuration
     */
    public void updateCatalog(VoltProjectBuilder builder) throws IOException, ProcCallException, InterruptedException {
        updateCatalog(builder, new ClientConfig());
    }

    /**
     * Compile project and return deployment string. It is recommended that {@code builder} is the same builder which was
     * used to compile this instance with the appropriate modifications.
     *
     * @param builder   {@link VoltProjectBuilder} with updated configuration.
     * @return          the deployment string to use in @UpdateApplicationCatalog
     * @throws IOException
     */
    public String getDeploymentString(VoltProjectBuilder builder) throws IOException {
        m_compiled = false;
        assertTrue(compile(builder));

        String deploymentString = new String(Files.readAllBytes(Paths.get(builder.getPathToDeployment())),
                Constants.UTF8ENCODING);
        return deploymentString;
    }

    /**
     * Update the catalog of a running instance. It is recommended that {@code builder} is the same builder which was
     * used to compile this instance with the appropriate modifications.
     *
     * @param builder      {@link VoltProjectBuilder} with updated configuration.
     * @param clientConfig {@link ClientConfig} to use when creating a new admin client
     * @throws ProcCallException    If there was an error calling the procedure
     * @throws InterruptedException If this thread was interrupted
     * @throws IOException          If there was an error writing or reading the updated configuration
     */
    public void updateCatalog(VoltProjectBuilder builder, ClientConfig clientConfig)
            throws IOException, ProcCallException, InterruptedException {
        String deploymentString = getDeploymentString(builder);
        Client client = createAdminClient(clientConfig);
        try {
            assertEquals(ClientResponse.SUCCESS,
                    client.callProcedure("@UpdateApplicationCatalog", null, deploymentString).getStatus());
        } finally {
            client.close();
        }
    }

    public void updateLicense(String licensePath) throws IOException, ProcCallException, InterruptedException {
        byte[] licenseBytes = Files.readAllBytes(Paths.get(licensePath));
        Client client = createAdminClient(new ClientConfig());
        try {
            assertEquals(ClientResponse.SUCCESS,
                    client.callProcedure("@UpdateLicense", licenseBytes).getStatus());
        } finally {
            client.close();
        }
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
    public boolean compileWithAdminMode(VoltProjectBuilder builder, int adminPort, boolean adminOnStartup) {
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
        // Clear dir, do init, do start
        startImpl(true, true, true);
    }

    @Override
    public void startUp(boolean clearLocalDataDirectories) {
        // If clearing dir, we must do init, otherwise not; always do start
        startImpl(clearLocalDataDirectories, clearLocalDataDirectories, true);
    }

    public void setForceVoltdbCreate(boolean newVoltdb) {
        templateCmdLine.setForceVoltdbCreate(newVoltdb);
    }

    public void setDeploymentAndVoltDBRoot(String pathToDeployment, String pathToVoltDBRoot) {
        templateCmdLine.pathToDeployment(pathToDeployment);
        m_voltdbroot = pathToVoltDBRoot;
        m_compiled = true;
    }

    public void setHostCount(int hostCount) {
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

    public void setDrPublicHost(String host) {
        m_drPublicHost = host;
    }

    public void setDrPublicPort(int port) {
        m_drPublicPort = port;
    }

    public void setTopicsPublicHost(String host) {
        m_topicsPublicHost = host;
    }

    public void setTopicsPublicPort(int port) {
        m_topicsPublicPort = port;
    }

    private void startLocalServer(int hostId, boolean clearLocalDataDirectories) throws IOException {
        startLocalServer(hostId, templateCmdLine.internalPort(), clearLocalDataDirectories, templateCmdLine.m_startAction);
    }

    private void startLocalServer(int hostId, int leaderPort, boolean clearLocalDataDirectories, StartAction action) {
        // make sure the local server has the same environment properties as separate process
        m_additionalProcessEnv.forEach(System::setProperty);
        // Generate a new root for the in-process server if clearing directories.
        File subroot = null;

        CommandLine cmdln;
        if (hostId >= m_cmdLines.size()) {
            // Make the local Configuration object...
            cmdln = (templateCmdLine.makeCopy());
            cmdln.internalPort(internalPortGenerator.nextInternalPort(hostId));
            cmdln.port(portGenerator.nextClient());
            cmdln.adminPort(portGenerator.nextAdmin());
            cmdln.zkport(portGenerator.nextZkPort());
            cmdln.httpPort(portGenerator.nextHttp());
            // replication port and its two automatic followers.
            cmdln.drAgentStartPort(m_replicationPort != -1 ? m_replicationPort : portGenerator.nextReplicationPort());
            setDrPublicInterface(cmdln);
            portGenerator.nextReplicationPort();
            portGenerator.nextReplicationPort();
            setTopicsPublicInterface(cmdln);
        }
        else {
            cmdln = m_cmdLines.get(hostId);
        }
        cmdln.leaderPort(leaderPort);
        cmdln.startCommand(action);
        cmdln.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));
        if (this.m_additionalProcessEnv != null) {
            for (String name : this.m_additionalProcessEnv.keySet()) {
                cmdln.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
            }
        }
        if (m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC) {
            EEProcess proc = m_eeProcs.get(hostId);
            assert(proc != null);
            cmdln.m_ipcPort = proc.port();
        }
        if (cmdln.m_topicsHostPort != null) {
            cmdln.m_topicsHostPort = cmdln.m_topicsHostPort.withDefaultPort(portGenerator.nextTopics());
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

        // for debug, dump the command line to a unique file.
        // cmdln.dumpToFile("/Users/rbetts/cmd_" + Integer.toString(portGenerator.next()));

        synchronized(this) {
            m_cluster.add(null);
            m_pipes.add(null);
            m_cmdLines.add(cmdln);
        }
        cmdln.m_startAction = StartAction.PROBE;
        cmdln.enableAdd(action == StartAction.JOIN);
        cmdln.hostCount(m_hostCount - m_removedHosts.size());
        String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
        String root = m_hostRoots.get(hostIdStr);
        //For new CLI dont pass deployment for probe.
        cmdln.pathToDeployment(null);
        cmdln.voltdbRoot(root + File.separator + Constants.DBROOT);
        if (m_enableVoltSnapshotPrefix) {
            System.setProperty("VoltSnapshotFilePrefix", getServerSpecificScratchDir(hostIdStr));
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
     * WARNING: behavior is inconsistent with {@link VoltSnapshotFile#getServerSpecificRoot(String, boolean)},
     * which returns the parent directory of voltdbroot.
     * @param hostId
     * @return  The location of voltdbroot
     */
    public String getServerSpecificRoot(String hostId) {
        if (!m_hostRoots.containsKey(hostId)) {
            throw new IllegalArgumentException("getServerSpecificRoot possibly called before cluster has started.");
        }
        assert(! new File(m_hostRoots.get(hostId)).getName().equals(Constants.DBROOT)) : m_hostRoots.get(hostId);
        return m_hostRoots.get(hostId) + File.separator + Constants.DBROOT;
    }

    public String getServerSpecificScratchDir(String hostId) {
        if (!m_hostScratch.containsKey(hostId)) {
            throw new IllegalArgumentException("getServerSpecificScratchDir possibly called before cluster has started.");
        }
        String scratchDir = m_hostScratch.get(hostId);
        try {
            FileUtils.forceMkdir(new File(scratchDir));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("getServerSpecificScratchDir failed to create scratch dir.");
        }
        return scratchDir;
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
            cmdln.m_userSchemas = m_mismatchSchema == null ? null
                    : Collections.singletonList(VoltProjectBuilder.createFileForSchema(m_mismatchSchema));
        }
        cmdln.setForceVoltdbCreate(clearLocalDataDirectories);

        //If we are initializing lets wait for it to finish.
        String rootPath = m_hostRoots.get(String.valueOf(hostId));
        File root;
        if (rootPath == null) {
            root = VoltSnapshotFile.getServerSpecificRoot(String.valueOf(hostId), clearLocalDataDirectories);
            assert (!root.getName().equals(Constants.DBROOT)) : root.getAbsolutePath();
            cmdln.voltdbRoot(new File(root, Constants.DBROOT));
        } else {
            root = new File(rootPath);
            cmdln.voltdbRoot(new File(root, Constants.DBROOT));
        }
        //Keep track by hostid the voltdbroot
        ServerThread th = new ServerThread(cmdln);
        String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
        try {
            if (m_enableVoltSnapshotPrefix) {
                String scratch = m_hostScratch.get(hostIdStr);
                if (scratch == null) {
                    scratch = root + File.separator + "scratch";
                }
                System.setProperty("VoltSnapshotFilePrefix", scratch);
                VoltSnapshotFile.setVoltSnapshotFilePrefix(scratch);
            }
            th.initialize();
        } catch (VoltDB.SimulatedExitException expected) {
            //All ok
        } catch (Exception ex) {
            log.error("Failed to initialize cluster process:" + ex.getMessage(), ex);
            assert (false);
        }
        m_hostRoots.putIfAbsent(hostIdStr, root.getAbsolutePath());
        m_hostScratch.putIfAbsent(hostIdStr, root.getAbsolutePath() + File.separator + "scratch");
    }

    private boolean waitForAllReady() {
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
                    } else if (pipeToFile.m_eof.get()) { // if eof, then no point in waiting around
                        continue;
                    }

                    // if not eof, then wait for statement of readiness
                    if ( ! pipeToFile.m_witnessedReady.get()) {
                        try {
                            // use a timeout to prevent a forever hang
                            pipeToFile.wait(250);
                        } catch (InterruptedException ex) {
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
        startImpl(clearLocalDataDirectories, !skipInit, true);
    }

    public void startUpInitOnly(boolean clearLocalDataDirectories) {
        startImpl(clearLocalDataDirectories, true, false);
    }

    // Negative options make my head hurt: inverting them here.
    private void startImpl(boolean clearLocalDataDirectories, boolean doInit, boolean doStart) {
        assertTrue(doInit | doStart);
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
        // set to true to spew startup timing data
        boolean logtime = false;
        long startTime = 0;
        printTiming(logtime, "Starting cluster at: " + System.currentTimeMillis());

        // reset the port generator. RegressionSuite always expects
        // to find ClientInterface and Admin mode on known ports.
        portGenerator.reset();
        internalPortGenerator = new InternalPortGeneratorForTest(portGenerator, numberOfCoordinators);

        templateCmdLine.leaderPort(portGenerator.nextInternalPort());

        NavigableSet<String> coordinators = internalPortGenerator.getCoordinators();
        if (!m_removedHosts.isEmpty()) {
            ImmutableSortedSet.Builder<String> sb = ImmutableSortedSet.naturalOrder();
            int i = 0;
            for (String coordinator : coordinators) {
                if (!m_removedHosts.contains(i++)) {
                    sb.add(coordinator);
                }
            }
            coordinators = sb.build();
        }

        templateCmdLine.coordinators(coordinators);
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
            while (m_removedHosts.contains(oopStartIndex)) {
                ++oopStartIndex;
            }
            try {
                if (doInit) {
                    initLocalServer(oopStartIndex, clearLocalDataDirectories);
                }
                if (doStart) {
                    startLocalServer(oopStartIndex, clearLocalDataDirectories);
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            ++oopStartIndex;
        }

        // create all the out-of-process servers
        for (int i = oopStartIndex; i < hostCount; i++) {
            if (m_removedHosts.contains(i)) {
                continue;
            }
            try {
                String placementGroup = null;
                if (m_placementGroups != null && m_placementGroups.length == m_hostCount) {
                    placementGroup = m_placementGroups[i];
                }

                if (doInit) {
                    initOne(i, clearLocalDataDirectories, !doStart);
                }
                if (doStart) {
                    startOne(i, clearLocalDataDirectories, StartAction.CREATE, true, placementGroup);
                }

                //wait before next one
                if (m_delayBetweenNodeStartupMS > 0) {
                    try {
                        Thread.sleep(m_delayBetweenNodeStartupMS);
                    } catch (InterruptedException ignored) {
                    }
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        if (!doStart) {
            System.out.println("VoltDB start not required");
            return;
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
                    try { proc.destroy(); } catch (Exception ignored) {}
                }
            }

            if (downProcesses > 0) {
                int expectedProcesses = m_hostCount - (m_hasLocalServer ? 1 : 0);
                if (!m_expectedToCrash) {
                    throw new RuntimeException( String.format("%d/%d external processes failed to start",
                            downProcesses, expectedProcesses));
                }
            } else if (! allReady) { // this error case should only be from a timeout
                throw new RuntimeException("One or more external processes failed to complete initialization.");
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
        } catch (InterruptedException e) {
            log.info("External VoltDB process is acting crazy.");
        } finally {
            m_cluster.set(procIndex, null);
        }

        // exit code 143 is the forcible shutdown code from .destroy()
        if (retval != 0 && retval != 143) {
            log.info("killOne: External VoltDB process terminated abnormally with return: " + retval);
        }

        failIfValgrindErrors(valgrindOutputFile);
    }

    public void initOne(int hostId, boolean clearLocalDataDirectories, boolean savePipe) throws IOException {
        PipeToFile ptf = null;
        CommandLine cmdln = (templateCmdLine.makeCopy());
        cmdln.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));
        if (this.m_additionalProcessEnv != null) {
            for (String name : this.m_additionalProcessEnv.keySet()) {
                cmdln.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
            }
        }
        File root = null;
        String scratch = null;
        try {
            root = VoltSnapshotFile.getServerSpecificRoot(String.valueOf(hostId), clearLocalDataDirectories);
            scratch = m_hostScratch.get(String.valueOf(hostId));
            assert(!root.getName().equals(Constants.DBROOT)) : root.getAbsolutePath();
            cmdln = cmdln.voltdbRoot(new File(root, Constants.DBROOT));
            if (scratch == null) {
                scratch = root.getAbsolutePath() + "/scratch";
            }
            cmdln = cmdln.startCommand(StartAction.INITIALIZE);
            cmdln.setForceVoltdbCreate(clearLocalDataDirectories);
            if (new Integer(hostId).equals(m_mismatchNode)) {
                assert m_usesStagedSchema;
                cmdln.m_userSchemas = m_mismatchSchema == null ? null
                        : Collections.singletonList(VoltProjectBuilder.createFileForSchema(m_mismatchSchema));
            }
            m_procBuilder.command().clear();
            List<String> cmdlnList = cmdln.createCommandLine(m_enableVoltSnapshotPrefix);
            String cmdLineFull = "Init cmd host=" + String.valueOf(hostId);
            if (logFullCmdLine) {
                cmdLineFull += " :";
                for (String element : cmdlnList) {
                    assert(element != null);
                    cmdLineFull += " " + element;
                }
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
                assert dir.mkdirs();
            }

            File dirFile = new VoltSnapshotFile(testoutputdir);
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
                ptf = new PipeToFile(fileName, proc.getInputStream(), String.valueOf(hostId), false, proc);
            } else {
                if (m_logMessageMatchResults.get(hostId) == null) {
                    m_logMessageMatchResults.put(hostId, Collections.newSetFromMap(new ConcurrentHashMap<>()));
                }
                ptf = new PipeToFile(fileName, proc.getInputStream(), String.valueOf(hostId), false,
                        proc, m_logMessageMatchPatterns, m_logMessageMatchResults.get(hostId));
                ptf.setHostId(hostId);
            }
            if (savePipe) {
                m_pipes.add(ptf);
            }
            ptf.setName("ClusterPipe:" + String.valueOf(hostId));
            ptf.start();
            proc.waitFor(); // Wait for the server initialization to finish ?
        } catch (IOException ex) {
            log.error("Failed to start cluster process:" + ex.getMessage(), ex);
            assert (false);
        } catch (InterruptedException ex) {
            log.error("Failed to start cluster process:" + ex.getMessage(), ex);
            assert (false);
        }
        if ( root != null ) {
            String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
            m_hostRoots.put(hostIdStr, root.getPath());
            m_hostScratch.putIfAbsent(hostIdStr, scratch);
        }
    }

    void startOne(int hostId, boolean clearLocalDataDirectories,
            StartAction startAction, boolean waitForReady, String placementGroup) {
        startOne(hostId, clearLocalDataDirectories, startAction, waitForReady, placementGroup, false);
    }

    void startOne(int hostId, boolean clearLocalDataDirectories,
            StartAction startAction, boolean waitForReady, String placementGroup,
            boolean resetLogMessageMatchResults) {
        PipeToFile ptf = null;
        CommandLine cmdln = (templateCmdLine.makeCopy());
        cmdln.setJavaProperty(clusterHostIdProperty, String.valueOf(hostId));
        cmdln.m_startAction = StartAction.PROBE;
        cmdln.enableAdd(startAction == StartAction.JOIN);
        cmdln.hostCount(m_hostCount - m_removedHosts.size());
        String hostIdStr = cmdln.getJavaProperty(clusterHostIdProperty);
        String root = m_hostRoots.get(hostIdStr);
        // This prefix is used to prepend for snapshot files.
        cmdln.voltSnapshotFilePrefix(getServerSpecificScratchDir(hostIdStr));
        //For new CLI dont pass deployment for probe.
        cmdln.voltdbRoot(root);
        cmdln.pathToDeployment(null);
        cmdln.setForceVoltdbCreate(clearLocalDataDirectories);

        if (this.m_additionalProcessEnv != null) {
            for (String name : this.m_additionalProcessEnv.keySet()) {
                cmdln.setJavaProperty(name, this.m_additionalProcessEnv.get(name));
            }
        }
        try {
            cmdln.internalPort(internalPortGenerator.nextInternalPort(hostId));
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

            setDrPublicInterface(cmdln);
            setTopicsPublicInterface(cmdln);
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
            if (cmdln.m_httpPort != Constants.HTTP_PORT_DISABLED) {
                cmdln.httpPort(portGenerator.nextHttp());
            }
            cmdln.timestampSalt(getRandomTimestampSalt());
            cmdln.setPlacementGroup(placementGroup);
            if (m_debug) {
                cmdln.debugPort(portGenerator.next());
            }

            cmdln.zkport(portGenerator.nextZkPort());

            if (cmdln.m_topicsHostPort != null) {
                cmdln.m_topicsHostPort = cmdln.m_topicsHostPort.withDefaultPort(portGenerator.nextTopics());
            }

            // If local directories are being cleared
            // generate a new subroot, otherwise reuse the existing directory
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

            cmdln.setMissingHostCount(m_missingHostCount);
            m_cmdLines.add(cmdln);
            m_procBuilder.command().clear();
            List<String> cmdlnList = cmdln.createCommandLine(m_enableVoltSnapshotPrefix);
            String cmdLineFull = "Start cmd host=" + String.valueOf(hostId);
            if (logFullCmdLine) {
                cmdLineFull += " :";
                for (String element : cmdlnList) {
                    assert(element != null);
                    cmdLineFull += " " + element;
                }
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
            } else {
                assert dir.mkdirs();
            }

            File dirFile = new VoltSnapshotFile(testoutputdir);
            if (dirFile.listFiles() != null) {
                for (File f : dirFile.listFiles()) {
                    if (f.getName().startsWith(getName() + "-" + hostId)) {
                        f.delete();
                    }
                }
            }
            Process proc = m_procBuilder.start();
            synchronized(this) {
                m_cluster.add(proc);
            }
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
                ptf = new PipeToFile(fileName, proc.getInputStream(), PipeToFile.m_initToken, false, proc);
            } else {
                if (m_logMessageMatchResults.containsKey(hostId)) {
                    if (resetLogMessageMatchResults) {
                        resetLogMessageMatchResults(hostId);
                    }
                } else {
                    m_logMessageMatchResults.put(hostId, Collections.newSetFromMap(new ConcurrentHashMap<>()));
                }
                ptf = new PipeToFile(fileName, proc.getInputStream(), PipeToFile.m_initToken, false,
                        proc, m_logMessageMatchPatterns, m_logMessageMatchResults.get(hostId));
                ptf.setHostId(hostId);
            }

            m_pipes.add(ptf);
            ptf.setName("ClusterPipe:" + hostId);
            ptf.start();
        } catch (IOException ex) {
            log.error("Failed to start cluster process:" + ex.getMessage(), ex);
            assert (false);
        }

        if (waitForReady && (startAction == StartAction.JOIN || startAction == StartAction.PROBE ||
                startAction == StartAction.REJOIN)) {
            waitOnPTFReady(ptf, true, System.currentTimeMillis(), System.currentTimeMillis(), hostId);
        }

        if (hostId > (m_hostCount - 1)) {
            m_hostCount++;
            this.m_compiled = false; //Host count changed, should recompile
        }
    }

    private void setDrPublicInterface(CommandLine cmdln) {
        if (m_drPublicHost != null) {
            cmdln.m_drPublicHost = m_drPublicHost;
        }
        if (m_drPublicPort != -1) {
            cmdln.m_drPublicPort = m_drPublicPort;
        }
    }

    private void setTopicsPublicInterface(CommandLine cmdln) {
        if (m_topicsPublicHost != null) {
            cmdln.m_topicsPublicHostPort = MiscUtils.getHostAndPortFromInterfaceSpec(m_topicsPublicHost, "",
                    m_topicsPublicPort != -1 ? m_topicsPublicPort : VoltDB.DEFAULT_TOPICS_PORT);
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
        } catch (IllegalThreadStateException e) {
            return false; // process is still alive
        }
    }

    public boolean recoverOne(int hostId, Integer portOffset, boolean liveRejoin) {
        StartAction startAction = StartAction.PROBE;
        return recoverOne(false, 0, hostId, portOffset, startAction);
    }

    public void joinOne(int hostId) {
        try {
            if (!m_hostRoots.containsKey(Integer.toString(hostId))) {
                initLocalServer(hostId, true);
            }
            startOne(hostId, true, StartAction.JOIN, true, null);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void joinOne(int hostId, String placementGroup) {
        try {
            if (!m_hostRoots.containsKey(Integer.toString(hostId))) {
                initLocalServer(hostId, true);
            }
            startOne(hostId, true, StartAction.JOIN, true, placementGroup);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    //create a new node and join to the cluster via rejoin
    public void rejoinOne(int hostId) {
        rejoinOne(hostId, true);
    }

    public void rejoinOne(int hostId, boolean clearLocalDataDirectories) {
        try {
            if (!m_hostRoots.containsKey(Integer.toString(hostId))) {
                initLocalServer(hostId, true);
            }
            startOne(hostId, clearLocalDataDirectories, StartAction.REJOIN, true, null);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /**
     * join multiple nodes to the cluster
     * @param hostIds a set of new host ids
     */
    public boolean join(Set<Integer> hostIds) {
        for (int hostId : hostIds) {
            try {
                if (!m_hostRoots.containsKey(Integer.toString(hostId))) {
                    initLocalServer(hostId, true);
                }
                startOne(hostId, true, StartAction.JOIN, false, null);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        return waitForAllReady();
    }

    /**
     * join multiple nodes to the cluster under their placement groups
     * @param hostIdByPlacementGroup a set of new host ids and their placement groups
     */
    public void join(Map<Integer, String> hostIdByPlacementGroup) {
        for (Map.Entry<Integer, String> entry : hostIdByPlacementGroup.entrySet()) {
            try {
                if (!m_hostRoots.containsKey(Integer.toString(entry.getKey()))) {
                    initLocalServer(entry.getKey(), true);
                }
                startOne(entry.getKey(), true, StartAction.JOIN, false, entry.getValue());
                if (m_delayBetweenNodeStartupMS > 0) {
                    try {
                        Thread.sleep(m_delayBetweenNodeStartupMS);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
            catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        waitForAllReady();
    }

    /**
     * Update the set of removed hosts to be {@code removeHosts}
     *
     * @param removeHosts Set of hosts which were removed
     */
    public void setRemovedHosts(Set<Integer> removeHosts) {
        m_removedHosts = removeHosts;
    }

    public boolean recoverOne(int hostId, Integer leaderHostId) {
        return recoverOne(false, 0, hostId, leaderHostId, StartAction.REJOIN);
    }

    private boolean recoverOne(boolean logtime, long startTime, int hostId) {
        return recoverOne( logtime, startTime, hostId, null, StartAction.REJOIN);
    }

    // Re-start a (dead) process. HostId is the enumeration of the host
    // in the cluster (0, 1, ... hostCount-1) -- not an hsid, for example.
    private boolean recoverOne(boolean logtime, long startTime, int hostId, Integer leaderHostId,
                               StartAction startAction) {
        // Lookup the client interface port of the rejoin host
        // I have no idea why this code ignores the user's input
        // based on other state in this class except to say that whoever wrote
        // it this way originally probably eats kittens and hates cake.
        if (leaderHostId == null || (m_hasLocalServer && hostId != 0)) {
            leaderHostId = 0;
        }
        startAction = StartAction.PROBE;
        int portNoToRejoin = m_cmdLines.get(leaderHostId).internalPort();

        if (hostId == 0 && m_hasLocalServer) {
            startLocalServer(hostId, portNoToRejoin, false, startAction);
            m_localServer.waitForRejoin();
            return true;
        }

        // For some mythical reason rejoinHostId is not actually used for the newly created host,
        // hostNum is used by default (in fact hostNum should equal to hostId, otherwise some tests
        // may fail)
        log.info("Rejoining " + hostId + " to hostID: " + leaderHostId);

        // rebuild the EE proc set.
        if (templateCmdLine.target().isIPC && m_eeProcs.size() < hostId) {
            EEProcess eeProc = m_eeProcs.get(hostId);
            File valgrindOutputFile;
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
            rejoinCmdLn.leader(":" + String.valueOf(portNoToRejoin));

            rejoinCmdLn.m_port = portGenerator.nextClient();
            rejoinCmdLn.m_adminPort = portGenerator.nextAdmin();
            rejoinCmdLn.m_httpPort = portGenerator.nextHttp();
            rejoinCmdLn.m_zkInterface = "127.0.0.1";
            rejoinCmdLn.m_zkPort = portGenerator.next();
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

            List<String> rejoinCmdLnStr = rejoinCmdLn.createCommandLine(m_enableVoltSnapshotPrefix);
            String cmdLineFull = "Rejoin cmd host=" + String.valueOf(hostId);
            if (logFullCmdLine) {
                cmdLineFull += " :";
                for (String element : rejoinCmdLnStr) {
                    cmdLineFull += " " + element;
                }
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
            } else {
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
                ptf = new PipeToFile(filePath, proc.getInputStream(), PipeToFile.m_rejoinCompleteToken, false, proc);
            } else {
                if (m_logMessageMatchResults.containsKey(hostId)) {
                    resetLogMessageMatchResults(hostId);
                } else {
                    m_logMessageMatchResults.put(hostId, Collections.newSetFromMap(new ConcurrentHashMap<>()));
                }
                ptf = new PipeToFile(filePath, proc.getInputStream(), PipeToFile.m_rejoinCompleteToken, false,
                        proc, m_logMessageMatchPatterns, m_logMessageMatchResults.get(hostId));
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
        } catch (IOException ex) {
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
            if (logtime) {
                System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
            }
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
                } catch (InterruptedException ex) {
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
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    synchronized public void shutdownSave(Client adminClient) throws IOException {
        ClientResponse resp = null;
        try {
            resp = adminClient.callProcedure("@PrepareShutdown");
        } catch (ProcCallException e) {
            throw new IOException(e);
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
            } catch (InterruptedException ignored) { }
        }
        if (sum != 0) {
            throw new IOException("Failed to clear any pending transactions.");
        }

        try{
            adminClient.callProcedure("@Shutdown", sigil);
        } catch (ProcCallException ignored) {
            ignored.printStackTrace();
        }
        System.out.println("@Shutdown: cluster has been shutdown via admin mode and last snapshot saved.");
    }

    synchronized public void shutdown(Client adminClient) throws IOException {
        try{
            adminClient.callProcedure("@Shutdown");
        } catch (ProcCallException ignored) {
            ignored.printStackTrace();
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
        } catch (Exception e) {
            log.error("Failure to shutdown LocalCluster's in-process VoltDB server.", e);
        } finally {
            m_running = false;
        }
        shutDownExternal();
        VoltServerConfig.removeInstance(this);
    }

    public void killSingleHost(int hostNum) throws InterruptedException {
        log.info("Killing " + hostNum);
        if (hostNum == 0 && m_localServer != null) {
            m_localServer.shutdown();
        } else {
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

    private synchronized boolean buildSafeClusterMemberList(ArrayList<Process> members) {
        int previousMemberCount = members.size();
        if (m_cluster != null && m_cluster.size() > previousMemberCount) {
            members.clear();
            for (int ii = 0; ii < previousMemberCount; ii++) {
                members.add(null);
            }
            members.addAll(m_cluster.subList(previousMemberCount, m_cluster.size()));
            return true;
        }
        else {
            return false;
        }
    }

    public void waitForNodesToShutdown() {
        ArrayList<Process> members = new ArrayList<>();
        while (buildSafeClusterMemberList(members)) {
            // It is possible to have a ConcurrentModificationException here so make a copy to be safe
            // join on all procs
            for (Process proc : members) {
                if (proc == null) {
                    continue;
                }
                int retval = 0;
                try {
                    retval = proc.waitFor();
                } catch (InterruptedException e) {
                    log.error("Unable to wait for Localcluster process to die: " + proc.toString(), e);
                }
                // exit code 143 is the forcible shutdown code from .destroy()
                if (retval != 0 && retval != 143) {
                    log.error("External VoltDB process terminated abnormally with return: " + retval);
                }
            }
        }

        if (m_cluster != null) {
            m_cluster.clear();
        }

        for (EEProcess proc : m_eeProcs) {
            File valgrindOutputFile = null;
            try {
                valgrindOutputFile = proc.waitForShutdown();
            } catch (InterruptedException e) {
                log.error("Unable to wait for EEProcess to die: " + proc.toString(), e);
            }

            failIfValgrindErrors(valgrindOutputFile);
        }

        m_eeProcs.clear();

        m_running = false;

    }

    public synchronized void shutDownExternal(boolean forceKillEEProcs) {
        ArrayList<Process> members = new ArrayList<>();
        while (buildSafeClusterMemberList(members)) {
            // kill all procs
            for (Process proc : members) {
                if (proc == null) {
                    continue;
                }
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
        return getListenerAddresses(ListenerPort.SQL);
    }

    public List<String> getListenerAddresses(ListenerPort port) {
        if (!m_running) {
            return null;
        }
        ArrayList<String> listeners = new ArrayList<>();
        for (int i = 0; i < m_cmdLines.size(); i++) {
            CommandLine cl = m_cmdLines.get(i);
            Process p = m_cluster.get(i);
            // if the process is alive, or is the in-process server
            if ((p != null) || (i == 0 && m_hasLocalServer)) {
                listeners.add("localhost:" + port.getPort(cl));
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
        if (m_failureState == FailureState.ONE_FAILURE) {
            prefix += "OneFail";
        }
        if (m_failureState == FailureState.ONE_RECOVERING) {
            prefix += "OneRecov";
        }
        return prefix + "-" + m_siteCount + "-" + m_hostCount +
            "-" + templateCmdLine.target().display.toUpperCase();
    }

    String getFileName() {
        String prefix = m_callingClassName + "-" + m_callingMethodName;
        if (m_failureState == FailureState.ONE_FAILURE) {
            prefix += "-OneFail";
        }
        if (m_failureState == FailureState.ONE_RECOVERING) {
            prefix += "-OneRecov";
        }
        return prefix + "-" + m_siteCount + "-" + m_hostCount +
            "-" + templateCmdLine.target().display.toUpperCase();
    }

    @Override
    public int getNodeCount()
    {
        return m_hostCount;
    }

    public boolean areAllNonLocalProcessesDead() {
        ArrayList<Process> members = new ArrayList<>();
        buildSafeClusterMemberList(members);
        for (Process proc : members){
            try {
                if (proc != null) {
                    proc.exitValue();
                }
            } catch (IllegalThreadStateException ex) {
                return false;
            }
        }
        return true;
    }

    public int getLiveNodeCount() {
        int count = 0;
        if (m_hasLocalServer) {
            count++;
        }

        ArrayList<Process> members = new ArrayList<>();
        if (buildSafeClusterMemberList(members)) {
            for (Process proc : members) {
                try {
                    if (proc != null) {
                        proc.exitValue();
                    }
                } catch (IllegalThreadStateException ex) {
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
        } finally {
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

    public void clearOverridesForModes() {
        m_modeOverrides = null;
        if (m_cmdLines != null) {
            for (CommandLine commandLine : m_cmdLines) {
                commandLine.m_modeOverrideForTest = null;
            }
        }
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

    public String zkInterface(int hostId) {
        return m_cmdLines.get(hostId).zkinterface();
    }

    public int zkPort(int hostId) {
        return m_cmdLines.get(hostId).zkport();
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
        cl.zkport(config.getZKPort());
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
            FileUtils.deleteDirectory(actualPath);
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

    public Path[] getPathInSubroots(String path) {
        Path retval[] = new Path[m_subRoots.size()];
        for (int ii = 0; ii < m_subRoots.size(); ii++) {
            retval[ii] = Paths.get(m_subRoots.get(ii).getPath(), path);
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
     * @param expectedToCrash set expectedToCrash flag
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
     * @param expectedToInitialize set expectedToInitialize flag
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
        } else {
            valgrindOutputFile.delete();
        }
    }

    public static LocalCluster createOnly(
            String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
            int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
            DrRoleType drRole, boolean hasLocalServer) throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        LocalCluster lc = compileBuilder(schemaDDL, siteCount, hostCount, kfactor, clusterId,
                replicationPort, remoteReplicationPort, pathToVoltDBRoot, jar, drRole, builder, null, null);
        lc.setHasLocalServer(hasLocalServer);
        lc.overrideAnyRequestForValgrind();
        lc.setJavaProperty("DR_QUERY_INTERVAL", "5");
        lc.setJavaProperty("DR_RECV_TIMEOUT", "5000");
        lc.setDeploymentAndVoltDBRoot(builder.getPathToDeployment(), pathToVoltDBRoot);

        return lc;
    }

    public void startCluster() {
        startUp(true);
    }

    // Use this for optionally enabling localServer in one of the DR clusters (usually for debugging)
    public static LocalCluster createLocalCluster(
            String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
            int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
            DrRoleType drRole, boolean hasLocalServer) throws IOException {
        return createLocalCluster(schemaDDL, siteCount, hostCount, kfactor, clusterId, replicationPort, remoteReplicationPort,
                pathToVoltDBRoot, jar, drRole, hasLocalServer, null, null);
    }

    public static LocalCluster createLocalCluster(
            String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
            int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
            DrRoleType drRole, boolean hasLocalServer, VoltProjectBuilder builder) throws IOException {
        return createLocalCluster(schemaDDL, siteCount, hostCount, kfactor, clusterId, replicationPort, remoteReplicationPort,
                pathToVoltDBRoot, jar, drRole, hasLocalServer, builder, null);
    }

    public static LocalCluster createLocalCluster(
            String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
            int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
            DrRoleType drRole, boolean hasLocalServer, VoltProjectBuilder builder,
            String callingMethodName) throws IOException {
        return createLocalCluster(schemaDDL, siteCount, hostCount, kfactor, clusterId, replicationPort, remoteReplicationPort,
                pathToVoltDBRoot, jar, drRole, hasLocalServer, builder, callingMethodName,
                false, null);
    }

    public static LocalCluster createLocalCluster(
            String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
            int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar, DrRoleType drRole,
            boolean hasLocalServer, VoltProjectBuilder builder, String callingMethodName, boolean enableSPIMigration,
            Map<String, String> javaProps) throws IOException {
        return createLocalCluster(schemaDDL, siteCount, hostCount, kfactor, clusterId, replicationPort,
                remoteReplicationPort, pathToVoltDBRoot, jar, drRole, hasLocalServer, builder, null,
                callingMethodName, enableSPIMigration, javaProps);
    }

    public static LocalCluster createLocalCluster(
            String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId,
            int replicationPort, int remoteReplicationPort, String pathToVoltDBRoot, String jar,
            DrRoleType drRole, boolean hasLocalServer, VoltProjectBuilder builder,
            String callingClassName, String callingMethodName,
            boolean enableSPIMigration, Map<String, String> javaProps) throws IOException {
        if (builder == null) {
            builder = new VoltProjectBuilder();
        }
        LocalCluster lc = compileBuilder(schemaDDL, siteCount, hostCount, kfactor, clusterId,
                replicationPort, remoteReplicationPort, pathToVoltDBRoot, jar, drRole, builder, callingClassName,
                callingMethodName);

        System.out.println("Starting local cluster.");
        lc.setHasLocalServer(hasLocalServer);
        lc.overrideAnyRequestForValgrind();
        lc.setJavaProperty("DR_QUERY_INTERVAL", "5");
        lc.setJavaProperty("DR_RECV_TIMEOUT", "5000");
        // temporary, until we always enable SPI migration
        if (enableSPIMigration) {
            lc.setJavaProperty("DISABLE_MIGRATE_PARTITION_LEADER", "false");
        }
        if (javaProps != null) {
            for (Map.Entry<String, String> prop : javaProps.entrySet()) {
                lc.setJavaProperty(prop.getKey(), prop.getValue());
            }
        }
        lc.startUp(true);

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

    private static LocalCluster compileBuilder(
            String schemaDDL, int siteCount, int hostCount, int kfactor, int clusterId, int replicationPort,
            int remoteReplicationPort, String pathToVoltDBRoot, String jar, DrRoleType drRole, VoltProjectBuilder builder,
            String callingClassName, String callingMethodName) throws IOException {
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
        LocalCluster lc = new LocalCluster(jar, siteCount, hostCount, kfactor, clusterId, BackendTarget.NATIVE_EE_JNI);
        lc.setReplicationPort(replicationPort);
        if (callingClassName != null) {
            lc.setCallingClassName(callingClassName);
        }
        if (callingMethodName != null) {
            lc.setCallingMethodName(callingMethodName);
        }
        assert(lc.compile(builder, pathToVoltDBRoot));
        return lc;
    }

    private ClientConfig createClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.setProcedureCallTimeout(30 * 1000);
        return cc;
    }

    public Client createClient() throws IOException {
        return createClient(createClientConfig());
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
        for (String address : getListenerAddresses(ListenerPort.ADMIN)) {
            client.createConnection(address);
        }
        return client;
    }

    public void setDelayBetweenNodeStartup(long delayBetweenNodeStartup) {
        m_delayBetweenNodeStartupMS = delayBetweenNodeStartup;
    }

    // Reset the message match result
    public void resetLogMessageMatchResult(int hostNum, String regex) {
        assertNotNull(m_logMessageMatchPatterns);
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
        assertNotNull(m_logMessageMatchPatterns);
        assertTrue(m_logMessageMatchResults.containsKey(hostNum));
        assertTrue(m_logMessageMatchPatterns.containsKey(regex));
        return m_logMessageMatchResults.get(hostNum).contains(regex);
    }

    // verify the presence of messages in the log from specified host
    public boolean logMessageContains(int hostId, List<String> patterns) {
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

    public boolean anyHostHasLogMessage(String regex) {
        return m_logMessageMatchResults.values().stream().anyMatch(s -> s.contains(regex));
    }

    private void resetLogMessageMatchResults(int hostId) {
        assertTrue(m_logMessageMatchResults.containsKey(hostId));
        m_logMessageMatchResults.get(hostId).clear();
    }

    // Get the host's real hostId or -1 if undefined
    public int getRealHostId(int hostId) {
        if (m_pipes == null) {
            return -1;
        }

        int realHostId = m_pipes.get(hostId).getHostId();
        return realHostId == Integer.MAX_VALUE ? -1 : realHostId;
    }

    public enum ListenerPort {
        SQL {
            @Override
            int getPort(CommandLine cl) {
                return cl.m_port;
            }
        },
        ADMIN {
            @Override
            int getPort(CommandLine cl) {
                return cl.m_adminPort;
            }
        },
        TOPICS {
            @Override
            int getPort(CommandLine cl) {
                return cl.m_topicsHostPort.getPort();
            }
        };

        abstract int getPort(CommandLine cl);
    }
}
