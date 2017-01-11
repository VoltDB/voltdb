/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.compiler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltcore.TransactionIdManager;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext;
import org.voltdb.ProcInfoData;
import org.voltdb.RealVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.FilteredCatalogDiffEngine;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.common.Permission;
import org.voltdb.compilereport.ReportMaker;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.utils.CatalogSchemaTools;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.InMemoryJarfile.JarLoader;
import org.voltdb.utils.MiscUtils;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableSet;

/**
 * Compiles a project XML file and some metadata into a Jarfile
 * containing stored procedure code and a serialzied catalog.
 *
 */
public class VoltCompiler {
    /** Represents the level of severity for a Feedback message generated during compiling. */
    public static enum Severity { INFORMATIONAL, WARNING, ERROR, UNEXPECTED }
    public static final int NO_LINE_NUMBER = -1;
    private static final String NO_FILENAME = "null";

    // Causes the "debugoutput" folder to be generated and populated.
    // Also causes explain plans on disk to include cost.
    public final static boolean DEBUG_MODE
      = Boolean.valueOf(System.getProperty("org.voltdb.compilerdebug", "false"));

    // was this voltcompiler instantiated in a main(), or as part of VoltDB
    public final boolean standaloneCompiler;

    // tables that change between the previous compile and this one
    // used for Live-DDL caching of plans
    private final Set<String> m_dirtyTables = new TreeSet<>();
    // A collection of statements from the previous catalog
    // used for Live-DDL caching of plans
    private final Map<String, Statement> m_previousCatalogStmts = new HashMap<>();

    // feedback by filename
    ArrayList<Feedback> m_infos = new ArrayList<>();
    ArrayList<Feedback> m_warnings = new ArrayList<>();
    ArrayList<Feedback> m_errors = new ArrayList<>();

    // set of annotations by procedure name
    private Map<String, ProcInfoData> m_procInfoOverrides = null;

    // Name of DDL file built by the DDL VoltCompiler from the catalog and added to the jar.
    public static String AUTOGEN_DDL_FILE_NAME = "autogen-ddl.sql";
    // Environment variable used to verify that a catalog created from autogen-dll.sql is effectively
    // identical to the original catalog that was used to create the autogen-ddl.sql file.
    public static final boolean DEBUG_VERIFY_CATALOG = Boolean.valueOf(System.getenv().get("VERIFY_CATALOG_DEBUG"));

    /// Set this to true to automatically retry a failed attempt to round-trip
    /// a rebuild of a catalog from its canonical ddl. This gives a chance to
    /// set breakpoints and step through a do-over of only the flawed catalogs.
    public static boolean RETRY_FAILED_CATALOG_REBUILD_UNDER_DEBUG = false;

    String m_projectFileURL = null;
    private String m_currentFilename = NO_FILENAME;
    Map<String, String> m_ddlFilePaths = new HashMap<>();
    String[] m_addedClasses = null;
    Set<String> m_importLines = null;

    // generated html text for catalog report
    String m_report = null;
    String m_reportPath = null;
    static String m_canonicalDDL = null;
    Catalog m_catalog = null;

    DatabaseEstimates m_estimates = new DatabaseEstimates();

    private List<String> m_capturedDiagnosticDetail = null;

    private static final VoltLogger compilerLog = new VoltLogger("COMPILER");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");
    private static final VoltLogger Log = new VoltLogger("org.voltdb.compiler.VoltCompiler");

    private final static String m_emptyDDLComment = "-- This DDL file is a placeholder for starting without a user-supplied catalog.\n";

    private ClassLoader m_classLoader = ClassLoader.getSystemClassLoader();

    /**
     * Represents output from a compile. This works similarly to Log4j; there
     * are different levels of feedback including info, warning, error, and
     * unexpected error. Feedback can be output to a printstream (like stdout)
     * or can be examined programatically.
     *
     */
    public static class Feedback {
        Severity severityLevel;
        String fileName;
        int lineNo;
        String message;

        Feedback(final Severity severityLevel, final String message, final String fileName, final int lineNo) {
            this.severityLevel = severityLevel;
            this.message = message;
            this.fileName = fileName;
            this.lineNo = lineNo;
        }

        public String getStandardFeedbackLine() {
            String retval = "";
            if (severityLevel == Severity.INFORMATIONAL)
                retval = "INFO";
            if (severityLevel == Severity.WARNING)
                retval = "WARNING";
            if (severityLevel == Severity.ERROR)
                retval = "ERROR";
            if (severityLevel == Severity.UNEXPECTED)
                retval = "UNEXPECTED ERROR";

            return retval + " " + getLogString();
        }

        public String getLogString() {
            String retval = new String();
            if (fileName != null) {
                retval += "[" + fileName;
                if (lineNo != NO_LINE_NUMBER)
                    retval += ":" + lineNo;
                retval += "]";
            }

            retval += ": " + message;
            return retval;
        }

        public Severity getSeverityLevel() {
            return severityLevel;
        }

        public String getFileName() {
            return fileName;
        }

        public int getLineNumber() {
            return lineNo;
        }

        public String getMessage() {
            return message;
        }
    }

    class VoltCompilerException extends Exception {
        private static final long serialVersionUID = -2267780579911448600L;
        private String message = null;

        VoltCompilerException(final Exception e) {
            super(e);
        }

        VoltCompilerException(final String message, final int lineNo) {
            addErr(message, lineNo);
            this.message = message;
        }

        VoltCompilerException(final String message) {
            addErr(message);
            this.message = message;
        }

        VoltCompilerException(String message, Throwable cause) {
            message += "\n   caused by:\n   " + cause.toString();
            addErr(message);
            this.message = message;
            this.initCause(cause);
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    class VoltXMLErrorHandler implements ErrorHandler {
        @Override
        public void error(final SAXParseException exception) throws SAXException {
            addErr(exception.getMessage(), exception.getLineNumber());
        }

        @Override
        public void fatalError(final SAXParseException exception) throws SAXException {
            //addErr(exception.getMessage(), exception.getLineNumber());
        }

        @Override
        public void warning(final SAXParseException exception) throws SAXException {
            addWarn(exception.getMessage(), exception.getLineNumber());
        }
    }

    public class ProcedureDescriptor {
        public final ArrayList<String> m_authGroups;
        public final String m_className;
        // for single-stmt procs
        public final String m_singleStmt;
        public final String m_joinOrder;
        public final String m_partitionString;
        public final boolean m_builtInStmt;    // autogenerated sql statement
        public final Language m_language;      // Java or Groovy
        public final String m_scriptImpl;      // Procedure code from DDL (if any)
        public final Class<?> m_class;

        ProcedureDescriptor (final ArrayList<String> authGroups, final String className) {
            assert(className != null);

            m_authGroups = authGroups;
            m_className = className;
            m_singleStmt = null;
            m_joinOrder = null;
            m_partitionString = null;
            m_builtInStmt = false;
            m_language = null;
            m_scriptImpl = null;
            m_class = null;
        }

        public ProcedureDescriptor(final ArrayList<String> authGroups, final Language language, final String scriptImpl, Class<?> clazz) {
            assert(clazz != null && language != null);

            m_authGroups = authGroups;
            m_className = clazz.getName();
            m_singleStmt = null;
            m_joinOrder = null;
            m_partitionString = null;
            m_builtInStmt = false;
            m_language = language;
            m_scriptImpl = scriptImpl;
            m_class = clazz;
        }

        ProcedureDescriptor(final ArrayList<String> authGroups, final Class<?> clazz, final String partitionString, final Language language, final String scriptImpl) {
            assert(clazz != null);
            assert(partitionString != null);

            m_authGroups = authGroups;
            m_className = clazz.getName();
            m_singleStmt = null;
            m_joinOrder = null;
            m_partitionString = partitionString;
            m_builtInStmt = false;
            m_language = language;
            m_scriptImpl = scriptImpl;
            m_class = clazz;
        }

        ProcedureDescriptor (final ArrayList<String> authGroups, final String className,
                final String singleStmt, final String joinOrder, final String partitionString,
                boolean builtInStmt, Language language, final String scriptImpl, Class<?> clazz)
        {
            assert(className != null);
            assert(singleStmt != null);

            m_authGroups = authGroups;
            m_className = className;
            m_singleStmt = singleStmt;
            m_joinOrder = joinOrder;
            m_partitionString = partitionString;
            m_builtInStmt = builtInStmt;
            m_language = language;
            m_scriptImpl = scriptImpl;
            m_class = clazz;
        }
    }

    /** Passing true to constructor indicates the compiler is being run in standalone mode */
    public VoltCompiler(boolean standaloneCompiler) {
        this.standaloneCompiler = standaloneCompiler;
    }

    /** Parameterless constructor is for embedded VoltCompiler use only. */
    public VoltCompiler() {
        this(false);
    }

    public boolean hasErrors() {
        return m_errors.size() > 0;
    }

    public boolean hasErrorsOrWarnings() {
        return (m_warnings.size() > 0) || hasErrors();
    }

    void addInfo(final String msg) {
        addInfo(msg, NO_LINE_NUMBER);
    }

    void addWarn(final String msg) {
        addWarn(msg, NO_LINE_NUMBER);
    }

    void addErr(final String msg) {
        addErr(msg, NO_LINE_NUMBER);
    }

    void addInfo(final String msg, final int lineNo) {
        final Feedback fb = new Feedback(Severity.INFORMATIONAL, msg, m_currentFilename, lineNo);
        m_infos.add(fb);
        if (standaloneCompiler) {
            compilerLog.info(fb.getLogString());
        }
        else {
            compilerLog.debug(fb.getLogString());
        }
    }

    void addWarn(final String msg, final int lineNo) {
        final Feedback fb = new Feedback(Severity.WARNING, msg, m_currentFilename, lineNo);
        m_warnings.add(fb);
        compilerLog.warn(fb.getLogString());
    }

    void addErr(final String msg, final int lineNo) {
        final Feedback fb = new Feedback(Severity.ERROR, msg, m_currentFilename, lineNo);
        m_errors.add(fb);
        compilerLog.error(fb.getLogString());
    }

    /**
     * Compile from a set of DDL files.
     *
     * @param jarOutputPath The location to put the finished JAR to.
     * @param ddlFilePaths The array of DDL files to compile (at least one is required).
     * @return true if successful
     * @throws VoltCompilerException
     */
    public boolean compileFromDDL(
            final String jarOutputPath,
            final String... ddlFilePaths)
    {
        if (ddlFilePaths.length == 0) {
            compilerLog.error("At least one DDL file is required.");
            return false;
        }
        List<VoltCompilerReader> ddlReaderList;
        try {
            ddlReaderList = DDLPathsToReaderList(ddlFilePaths);
        }
        catch (VoltCompilerException e) {
            compilerLog.error("Unable to open DDL file.", e);
            return false;
        }
        return compileInternalToFile(jarOutputPath, null, null, ddlReaderList, null);
    }

    /**
     * Compile from DDL in a single string
     *
     * @param ddl The inline DDL text
     * @param jarPath The location to put the finished JAR to.
     * @return true if successful
     * @throws VoltCompilerException
     */
    public boolean compileDDLString(String ddl, String jarPath) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        schemaFile.deleteOnExit();
        final String schemaPath = schemaFile.getPath();

        return compileFromDDL(jarPath, schemaPath);
    }

    /**
     * Compile empty catalog jar
     * @param jarOutputPath output jar path
     * @return true if successful
     */
    public boolean compileEmptyCatalog(final String jarOutputPath) {
        // Use a special DDL reader to provide the contents.
        List<VoltCompilerReader> ddlReaderList = new ArrayList<>(1);
        ddlReaderList.add(new VoltCompilerStringReader("ddl.sql", m_emptyDDLComment));
        // Seed it with the DDL so that a version upgrade hack in compileInternalToFile()
        // doesn't try to get the DDL file from the path.
        InMemoryJarfile jarFile = new InMemoryJarfile();
        try {
            ddlReaderList.get(0).putInJar(jarFile, "ddl.sql");
        }
        catch (IOException e) {
            compilerLog.error("Failed to add DDL file to empty in-memory jar.");
            return false;
        }
        return compileInternalToFile(jarOutputPath, null, null, ddlReaderList, jarFile);
    }

    private static void addBuildInfo(final InMemoryJarfile jarOutput) {
        StringBuilder buildinfo = new StringBuilder();
        String info[] = RealVoltDB.extractBuildInfo(compilerLog);
        buildinfo.append(info[0]).append('\n');
        buildinfo.append(info[1]).append('\n');
        buildinfo.append(System.getProperty("user.name")).append('\n');
        buildinfo.append(System.getProperty("user.dir")).append('\n');
        buildinfo.append(Long.toString(System.currentTimeMillis())).append('\n');

        byte buildinfoBytes[] = buildinfo.toString().getBytes(Constants.UTF8ENCODING);
        jarOutput.put(CatalogUtil.CATALOG_BUILDINFO_FILENAME, buildinfoBytes);
    }

    /**
     * Internal method that takes the generated DDL from the catalog and builds a new catalog.
     * The generated catalog is diffed with the original catalog to verify compilation and
     * catalog generation consistency.
     */
    private void debugVerifyCatalog(InMemoryJarfile origJarFile, Catalog origCatalog)
    {
        final VoltCompiler autoGenCompiler = new VoltCompiler();
        // Make the new compiler use the original jarfile's classloader so it can
        // pull in the class files for procedures and imports
        autoGenCompiler.m_classLoader = origJarFile.getLoader();
        List<VoltCompilerReader> autogenReaderList = new ArrayList<>(1);
        autogenReaderList.add(new VoltCompilerJarFileReader(origJarFile, AUTOGEN_DDL_FILE_NAME));
        InMemoryJarfile autoGenJarOutput = new InMemoryJarfile();
        autoGenCompiler.m_currentFilename = AUTOGEN_DDL_FILE_NAME;
        // This call is purposely replicated in retryFailedCatalogRebuildUnderDebug,
        // where it provides an opportunity to set a breakpoint on a do-over when this
        // mainline call produces a flawed catalog that fails the catalog diff.
        // Keep the two calls in synch to allow debugging under the same exact conditions.
        Catalog autoGenCatalog = autoGenCompiler.compileCatalogInternal(null, null,
                autogenReaderList, autoGenJarOutput);
        if (autoGenCatalog == null) {
            Log.info("Did not verify catalog because it could not be compiled.");
            return;
        }

        FilteredCatalogDiffEngine diffEng =
                new FilteredCatalogDiffEngine(origCatalog, autoGenCatalog, false);
        String diffCmds = diffEng.commands();
        if (diffCmds != null && !diffCmds.equals("")) {
            // This retry is disabled by default to avoid confusing the unwary developer
            // with a "pointless" replay of an apparently flawed catalog rebuild.
            // Enable it via this flag to provide a chance to set an early breakpoint
            // that is only triggered in hopeless cases.
            if (RETRY_FAILED_CATALOG_REBUILD_UNDER_DEBUG) {
                autoGenCatalog = replayFailedCatalogRebuildUnderDebug(
                        autoGenCompiler, autogenReaderList,
                        autoGenJarOutput);
            }
            // Re-run a failed diff more verbosely as a pre-crash test diagnostic.
            diffEng = new FilteredCatalogDiffEngine(origCatalog, autoGenCatalog, true);
            diffCmds = diffEng.commands();
            String crashAdvice = "Catalog Verification from Generated DDL failed! " +
                    "VoltDB dev: Consider" +
                    (RETRY_FAILED_CATALOG_REBUILD_UNDER_DEBUG ? "" :
                        " setting VoltCompiler.RETRY_FAILED_CATALOG_REBUILD_UNDER_DEBUG = true and") +
                    " setting a breakpoint in VoltCompiler.replayFailedCatalogRebuildUnderDebug" +
                    " to debug a replay of the faulty catalog rebuild roundtrip. ";
            VoltDB.crashLocalVoltDB(crashAdvice + "The offending diffcmds were: " + diffCmds);
        }
        else {
            Log.info("Catalog verification completed successfuly.");
        }
    }

    /** Take two steps back to retry and potentially debug a catalog rebuild
     *  that generated an unintended change. This code is PURPOSELY redundant
     *  with the mainline call in debugVerifyCatalog above.
     *  Keep the two calls in synch and only redirect through this function
     *  in the post-mortem replay after the other call created a flawed catalog.
     */
    private Catalog replayFailedCatalogRebuildUnderDebug(
            VoltCompiler autoGenCompiler,
            List<VoltCompilerReader> autogenReaderList,
            InMemoryJarfile autoGenJarOutput)
    {
        // Be sure to set RETRY_FAILED_CATALOG_REBUILD_UNDER_DEBUG = true to enable
        // this last ditch retry before crashing.
        // BREAKPOINT HERE!
        // Then step IN to debug the failed rebuild -- or, just as likely, the canonical ddl.
        // Or step OVER to debug just the catalog diff process, retried with verbose output --
        // maybe it's just being too sensitive to immaterial changes?
        Catalog autoGenCatalog = autoGenCompiler.compileCatalogInternal(null, null,
                autogenReaderList, autoGenJarOutput);
        return autoGenCatalog;
    }

    /**
     * Internal method for compiling with and without a project.xml file or DDL files.
     *
     * @param projectReader Reader for project file or null if a project file is not used.
     * @param jarOutputPath The location to put the finished JAR to.
     * @param ddlFilePaths The list of DDL files to compile (when no project is provided).
     * @param jarOutputRet The in-memory jar to populate or null if the caller doesn't provide one.
     * @return true if successful
     */
    private boolean compileInternalToFile(
            final String jarOutputPath,
            final VoltCompilerReader cannonicalDDLIfAny,
            final Catalog previousCatalogIfAny,
            final List<VoltCompilerReader> ddlReaderList,
            final InMemoryJarfile jarOutputRet)
    {
        if (jarOutputPath == null) {
            addErr("The output jar path is null.");
            return false;
        }

        InMemoryJarfile jarOutput = compileInternal(cannonicalDDLIfAny, previousCatalogIfAny, ddlReaderList, jarOutputRet);
        if (jarOutput == null) {
            return false;
        }

        try {
            jarOutput.writeToFile(new File(jarOutputPath)).run();
        }
        catch (final Exception e) {
            e.printStackTrace();
            addErr("Error writing catalog jar to disk: " + e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * Internal method for compiling with and without a project.xml file or DDL files.
     *
     * @param projectReader Reader for project file or null if a project file is not used.
     * @param ddlFilePaths The list of DDL files to compile (when no project is provided).
     * @param jarOutputRet The in-memory jar to populate or null if the caller doesn't provide one.
     * @return The InMemoryJarfile containing the compiled catalog if
     * successful, null if not.  If the caller provided an InMemoryJarfile, the
     * return value will be the same object, not a copy.
     */
    private InMemoryJarfile compileInternal(
            final VoltCompilerReader cannonicalDDLIfAny,
            final Catalog previousCatalogIfAny,
            final List<VoltCompilerReader> ddlReaderList,
            final InMemoryJarfile jarOutputRet)
    {
        // Expect to have either >1 ddl file or a project file.
        assert(ddlReaderList.size() > 0);
        // Make a temporary local output jar if one wasn't provided.
        final InMemoryJarfile jarOutput = (jarOutputRet != null
                                                ? jarOutputRet
                                                : new InMemoryJarfile());

        if (ddlReaderList == null || ddlReaderList.isEmpty()) {
            addErr("One or more DDL files are required.");
            return null;
        }

        // clear out the warnings and errors
        m_warnings.clear();
        m_infos.clear();
        m_errors.clear();

        // do all the work to get the catalog
        final Catalog catalog = compileCatalogInternal(cannonicalDDLIfAny, previousCatalogIfAny, ddlReaderList, jarOutput);
        if (catalog == null) {
            return null;
        }
        Cluster cluster = catalog.getClusters().get("cluster");
        assert(cluster != null);
        Database database = cluster.getDatabases().get("database");
        assert(database != null);

        // Build DDL from Catalog Data
        String ddlWithBatchSupport = CatalogSchemaTools.toSchema(catalog, m_importLines);
        m_canonicalDDL = CatalogSchemaTools.toSchemaWithoutInlineBatches(ddlWithBatchSupport);

        // generate the catalog report and write it to disk
        try {
            VoltDBInterface voltdb = VoltDB.instance();
            // try to get a catalog context
            CatalogContext catalogContext = voltdb != null ? voltdb.getCatalogContext() : null;
            ClusterSettings clusterSettings = catalogContext != null ? catalogContext.getClusterSettings() : null;
            int tableCount = catalogContext != null ? catalogContext.tables.size() : 0;
            Deployment deployment = catalogContext != null ? catalogContext.cluster.getDeployment().get("deployment") : null;
            int hostcount = clusterSettings != null ? clusterSettings.hostcount() : 1;
            int kfactor = deployment != null ? deployment.getKfactor() : 0;
            int sitesPerHost = 8;
            if  (voltdb != null && voltdb.getCatalogContext() != null) {
                sitesPerHost =  voltdb.getCatalogContext().getNodeSettings().getLocalSitesCount();
            }
            boolean isPro = MiscUtils.isPro();

            long minHeapRqt = RealVoltDB.computeMinimumHeapRqt(isPro, tableCount, sitesPerHost, kfactor);
            m_report = ReportMaker.report(m_catalog, minHeapRqt, isPro, hostcount,
                    sitesPerHost, kfactor, m_warnings, ddlWithBatchSupport);
            m_reportPath = null;
            File file = null;

            // write to working dir when using VoltCompiler directly
            if (standaloneCompiler) {
                file = new File("catalog-report.html");
            }
            else {
                // it's possible that standaloneCompiler will be false and catalogContext will be null
                //   in test code.

                // if we have a context, write report to voltroot
                if (catalogContext != null) {
                    file = new File(VoltDB.instance().getVoltDBRootPath(), "catalog-report.html");
                }
            }

            // if there's a good place to write the report, do so
            if (file != null) {
                FileWriter fw = new FileWriter(file);
                fw.write(m_report);
                fw.close();
                m_reportPath = file.getAbsolutePath();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        jarOutput.put(AUTOGEN_DDL_FILE_NAME, m_canonicalDDL.getBytes(Constants.UTF8ENCODING));
        if (DEBUG_VERIFY_CATALOG) {
            debugVerifyCatalog(jarOutput, catalog);
        }

        // WRITE CATALOG TO JAR HERE
        final String catalogCommands = catalog.serialize();

        byte[] catalogBytes = catalogCommands.getBytes(Constants.UTF8ENCODING);

        try {
            // Don't update buildinfo if it's already present, e.g. while upgrading.
            // Note when upgrading the version has already been updated by the caller.
            if (!jarOutput.containsKey(CatalogUtil.CATALOG_BUILDINFO_FILENAME)) {
                addBuildInfo(jarOutput);
            }
            jarOutput.put(CatalogUtil.CATALOG_FILENAME, catalogBytes);
            // put the compiler report into the jarfile
            jarOutput.put("catalog-report.html", m_report.getBytes(Constants.UTF8ENCODING));
        }
        catch (final Exception e) {
            e.printStackTrace();
            return null;
        }

        assert(!hasErrors());

        if (hasErrors()) {
            return null;
        }

        return jarOutput;
    }

    /**
     * Get textual explain plan info for each plan from the
     * catalog to be shoved into the catalog jarfile.
     */
    HashMap<String, byte[]> getExplainPlans(Catalog catalog) {
        HashMap<String, byte[]> retval = new HashMap<>();
        Database db = getCatalogDatabase();
        assert(db != null);
        for (Procedure proc : db.getProcedures()) {
            for (Statement stmt : proc.getStatements()) {
                String s = "SQL: " + stmt.getSqltext() + "\n";
                s += "COST: " + Integer.toString(stmt.getCost()) + "\n";
                s += "PLAN:\n\n";
                s += Encoder.hexDecodeToString(stmt.getExplainplan()) + "\n";
                byte[] b = s.getBytes(Constants.UTF8ENCODING);
                retval.put(proc.getTypeName() + "_" + stmt.getTypeName() + ".txt", b);
            }
        }
        return retval;
    }

    private VoltCompilerFileReader createDDLFileReader(String path)
            throws VoltCompilerException
    {
        try {
            return new VoltCompilerFileReader(VoltCompilerFileReader.getSchemaPath(m_projectFileURL, path));
        }
        catch (IOException e) {
            String msg = String.format("Unable to open schema file \"%s\" for reading: %s", path, e.getMessage());
            throw new VoltCompilerException(msg);
        }
    }

    private List<VoltCompilerReader> DDLPathsToReaderList(final String... ddlFilePaths)
            throws VoltCompilerException
    {
        List<VoltCompilerReader> ddlReaderList = new ArrayList<>(ddlFilePaths.length);
        for (int i = 0; i < ddlFilePaths.length; ++i) {
            ddlReaderList.add(createDDLFileReader(ddlFilePaths[i]));
        }
        return ddlReaderList;
    }

    /**
     * Compile from DDL files (only).
     * @param ddlFilePaths  input ddl files
     * @return  compiled catalog
     * @throws VoltCompilerException
     */
    public Catalog compileCatalogFromDDL(final String... ddlFilePaths)
            throws VoltCompilerException
    {
        InMemoryJarfile jarOutput = new InMemoryJarfile();
        return compileCatalogInternal(null, null, DDLPathsToReaderList(ddlFilePaths), jarOutput);
    }

    /**
     * Internal method for compiling the catalog.
     *
     * @param database catalog-related info parsed from a project file
     * @param ddlReaderList Reader objects for ddl files.
     * @param jarOutput The in-memory jar to populate or null if the caller doesn't provide one.
     * @return true if successful
     */
    private Catalog compileCatalogInternal(
            final VoltCompilerReader cannonicalDDLIfAny,
            final Catalog previousCatalogIfAny,
            final List<VoltCompilerReader> ddlReaderList,
            final InMemoryJarfile jarOutput)
    {
        // Compiler instance is reusable. Clear the cache.
        cachedAddedClasses.clear();

        m_catalog = new Catalog();
        // Initialize the catalog for one cluster
        m_catalog.execute("add / clusters cluster");
        m_catalog.getClusters().get("cluster").setSecurityenabled(false);

        // shutdown and make a new hsqldb
        try {
            Database previousDBIfAny = null;
            if (previousCatalogIfAny != null) {
                previousDBIfAny = previousCatalogIfAny.getClusters().get("cluster").getDatabases().get("database");
            }
            compileDatabaseNode(cannonicalDDLIfAny, previousDBIfAny, ddlReaderList, jarOutput);
        } catch (final VoltCompilerException e) {
            return null;
        }

        assert(m_catalog != null);

        // add epoch info to catalog
        final int epoch = (int)(TransactionIdManager.getEpoch() / 1000);
        m_catalog.getClusters().get("cluster").setLocalepoch(epoch);

        return m_catalog;
    }

    ProcInfoData getProcInfoOverride(final String procName) {
        if (m_procInfoOverrides == null)
            return null;
        return m_procInfoOverrides.get(procName);
    }

    public String getCanonicalDDL() {
        if(m_canonicalDDL == null) {
            throw new RuntimeException();
        }
        return m_canonicalDDL;
    }

    public Catalog getCatalog() {
        return m_catalog;
    }

    public Database getCatalogDatabase() {
        return m_catalog.getClusters().get("cluster").getDatabases().get("database");
    }

    private Database initCatalogDatabase() {
        // create the database in the catalog
        m_catalog.execute("add /clusters#cluster databases database");
        addDefaultRoles();
        return getCatalogDatabase();
    }

    /**
     * Create default roles. These roles cannot be removed nor overridden in the DDL.
     * Make sure to omit these roles in the generated DDL in {@link org.voltdb.utils.CatalogSchemaTools}
     * Also, make sure to prevent them from being dropped by DROP ROLE in the DDLCompiler
     * !!!
     * IF YOU ADD A THIRD ROLE TO THE DEFAULTS, IT'S TIME TO BUST THEM OUT INTO A CENTRAL
     * LOCALE AND DO ALL THIS MAGIC PROGRAMATICALLY --izzy 11/20/2014
     */
    private void addDefaultRoles()
    {
        // admin
        m_catalog.execute("add /clusters#cluster/databases#database groups administrator");
        Permission.setPermissionsInGroup(getCatalogDatabase().getGroups().get("administrator"),
                                         Permission.getPermissionsFromAliases(Arrays.asList("ADMIN")));

        // user
        m_catalog.execute("add /clusters#cluster/databases#database groups user");
        Permission.setPermissionsInGroup(getCatalogDatabase().getGroups().get("user"),
                                         Permission.getPermissionsFromAliases(Arrays.asList("SQL", "ALLPROC")));
    }

    public static enum DdlProceduresToLoad
    {
        NO_DDL_PROCEDURES, ONLY_SINGLE_STATEMENT_PROCEDURES, ALL_DDL_PROCEDURES
    }


    /**
     * Simplified interface for loading a ddl file with full support for VoltDB
     * extensions (partitioning, procedures, export), but no support for "project file" input.
     * This is, at least initially, only a back door to create a fully functional catalog for
     * the purposes of planner unit testing.
     * @param hsql an interface to the hsql frontend, initialized and potentially reused by the caller.
     * @param whichProcs indicates which ddl-defined procedures to load: none, single-statement, or all
     * @param ddlFilePaths schema file paths
     * @throws VoltCompilerException
     */
    public Catalog loadSchema(HSQLInterface hsql,
                              DdlProceduresToLoad whichProcs,
                              String... ddlFilePaths) throws VoltCompilerException
    {
        m_catalog = new Catalog(); //
        m_catalog.execute("add / clusters cluster");
        Database db = initCatalogDatabase();
        List<VoltCompilerReader> ddlReaderList = DDLPathsToReaderList(ddlFilePaths);
        final VoltDDLElementTracker voltDdlTracker = new VoltDDLElementTracker(this);
        InMemoryJarfile jarOutput = new InMemoryJarfile();
        compileDatabase(db, hsql, voltDdlTracker, null, null, ddlReaderList, null, whichProcs, jarOutput);

        return m_catalog;
    }

    /**
     * Load a ddl file with full support for VoltDB extensions (partitioning, procedures,
     * export), AND full support for input via a project xml file's "database" node.
     * @param database catalog-related info parsed from a project file
     * @param ddlReaderList Reader objects for ddl files.
     * @param jarOutput The in-memory jar to populate or null if the caller doesn't provide one.
     * @throws VoltCompilerException
     */
    private void compileDatabaseNode(
            VoltCompilerReader cannonicalDDLIfAny,
            Database previousDBIfAny,
            final List<VoltCompilerReader> ddlReaderList,
            final InMemoryJarfile jarOutput)
                    throws VoltCompilerException
    {
        final ArrayList<Class<?>> classDependencies = new ArrayList<>();
        final VoltDDLElementTracker voltDdlTracker = new VoltDDLElementTracker(this);

        Database db = initCatalogDatabase();

        // shutdown and make a new hsqldb
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        compileDatabase(db, hsql, voltDdlTracker, cannonicalDDLIfAny, previousDBIfAny, ddlReaderList, classDependencies,
                        DdlProceduresToLoad.ALL_DDL_PROCEDURES, jarOutput);
    }

    /**
     * Common code for schema loading shared by loadSchema and compileDatabaseNode
     *
     * @param db the database entry in the catalog
     * @param hsql an interface to the hsql frontend, initialized and potentially reused by the caller.
     * @param voltDdlTracker non-standard VoltDB schema annotations, initially those from a project file
     * @param schemas the ddl input files
     * @param export optional export connector configuration (from the project file)
     * @param classDependencies optional additional jar files required by procedures
     * @param whichProcs indicates which ddl-defined procedures to load: none, single-statement, or all
     * @param jarOutput The in-memory jar to populate or null if the caller doesn't provide one.
     */
    private void compileDatabase(
            Database db,
            HSQLInterface hsql,
            VoltDDLElementTracker voltDdlTracker,
            VoltCompilerReader cannonicalDDLIfAny,
            Database previousDBIfAny,
            List<VoltCompilerReader> schemaReaders,
            Collection<Class<?>> classDependencies,
            DdlProceduresToLoad whichProcs,
            InMemoryJarfile jarOutput)
                    throws VoltCompilerException
    {
        // Actually parse and handle all the DDL
        // DDLCompiler also provides partition descriptors for DDL PARTITION
        // and REPLICATE statements.
        final DDLCompiler ddlcompiler = new DDLCompiler(this, hsql, voltDdlTracker, m_classLoader);

        if (cannonicalDDLIfAny != null) {
            // add the file object's path to the list of files for the jar
            m_ddlFilePaths.put(cannonicalDDLIfAny.getName(), cannonicalDDLIfAny.getPath());

            ddlcompiler.loadSchema(cannonicalDDLIfAny, db, whichProcs);
        }

        m_dirtyTables.clear();

        for (final VoltCompilerReader schemaReader : schemaReaders) {
            String origFilename = m_currentFilename;
            try {
                if (m_currentFilename == null || m_currentFilename.equals(NO_FILENAME))
                    m_currentFilename = schemaReader.getName();

                // add the file object's path to the list of files for the jar
                m_ddlFilePaths.put(schemaReader.getName(), schemaReader.getPath());

                ddlcompiler.loadSchema(schemaReader, db, whichProcs);
            }
            finally {
                m_currentFilename = origFilename;
            }
        }

        // When A/A is enabled, create an export table for every DR table to log possible conflicts
        ddlcompiler.loadAutogenExportTableSchema(db, previousDBIfAny, whichProcs);

        ddlcompiler.compileToCatalog(db);

        // add database estimates info
        addDatabaseEstimatesInfo(m_estimates, db);

        // Process DDL exported tables
        NavigableMap<String, NavigableSet<String>> exportTables = voltDdlTracker.getExportedTables();
        for (Entry<String, NavigableSet<String>> e : exportTables.entrySet()) {
            String targetName = e.getKey();
            for (String tableName : e.getValue()) {
                addExportTableToConnector(targetName, tableName, db);
            }
        }
        ddlcompiler.processMaterializedViewWarnings(db);

        // process DRed tables
        for (Entry<String, String> drNode: voltDdlTracker.getDRedTables().entrySet()) {
            compileDRTable(drNode, db);
        }

        if (whichProcs != DdlProceduresToLoad.NO_DDL_PROCEDURES) {
            Collection<ProcedureDescriptor> allProcs = voltDdlTracker.getProcedureDescriptors();
            CatalogMap<Procedure> previousProcsIfAny = null;
            if (previousDBIfAny != null) {
                previousProcsIfAny = previousDBIfAny.getProcedures();
            }
            compileProcedures(db, hsql, allProcs, classDependencies, whichProcs, previousProcsIfAny, jarOutput);
        }

        // add extra classes from the DDL
        m_addedClasses = voltDdlTracker.m_extraClassses.toArray(new String[0]);
        // Also, grab the IMPORT CLASS lines so we can add them to the
        // generated DDL
        m_importLines = ImmutableSet.copyOf(voltDdlTracker.m_importLines);
        addExtraClasses(jarOutput);

        compileRowLimitDeleteStmts(db, hsql, ddlcompiler.getLimitDeleteStmtToXmlEntries());
    }

    private void compileRowLimitDeleteStmts(
            Database db,
            HSQLInterface hsql,
            Collection<Map.Entry<Statement, VoltXMLElement>> deleteStmtXmlEntries)
            throws VoltCompilerException {

        for (Map.Entry<Statement, VoltXMLElement> entry : deleteStmtXmlEntries) {
            Statement stmt = entry.getKey();
            VoltXMLElement xml = entry.getValue();

            // choose DeterminismMode.FASTER for determinism, and rely on the planner to error out
            // if we generated a plan that is content-non-deterministic.
            StatementCompiler.compileStatementAndUpdateCatalog(this,
                    hsql,
                    db.getCatalog(),
                    db,
                    m_estimates,
                    stmt,
                    xml,
                    stmt.getSqltext(),
                    null, // no user-supplied join order
                    DeterminismMode.FASTER,
                    StatementPartitioning.partitioningForRowLimitDelete());
        }
    }

    /**
     * Once the DDL file is over, take all of the extra classes found and add them to the jar.
     */
    private void addExtraClasses(final InMemoryJarfile jarOutput) throws VoltCompilerException {

        List<String> addedClasses = new ArrayList<>();

        for (String className : m_addedClasses) {
            /*
             * Only add the class if it isn't already in the output jar.
             * The jar will be pre-populated when performing an automatic
             * catalog version upgrade.
             */
            if (!jarOutput.containsKey(className)) {
                try {
                    Class<?> clz = Class.forName(className, true, m_classLoader);

                    if (addClassToJar(jarOutput, clz)) {
                        addedClasses.add(className);
                    }

                }
                catch (Exception e) {
                    String msg = "Class %s could not be loaded/found/added to the jar.";
                    msg = String.format(msg, className);
                    throw new VoltCompilerException(msg);
                }
                // reset the added classes to the actual added classes
            }
        }
        m_addedClasses = addedClasses.toArray(new String[0]);
    }

    /**
     * @param db the database entry in the catalog
     * @param hsql an interface to the hsql frontend, initialized and potentially reused by the caller.
     * @param classDependencies
     * @param voltDdlTracker non-standard VoltDB schema annotations
     * @param whichProcs indicates which ddl-defined procedures to load: none, single-statement, or all
     * @throws VoltCompilerException
     */
    private void compileProcedures(Database db,
                                   HSQLInterface hsql,
                                   Collection<ProcedureDescriptor> allProcs,
                                   Collection<Class<?>> classDependencies,
                                   DdlProceduresToLoad whichProcs,
                                   CatalogMap<Procedure> prevProcsIfAny,
                                   InMemoryJarfile jarOutput) throws VoltCompilerException
    {
        // build a cache of previous SQL stmts
        m_previousCatalogStmts.clear();
        if (prevProcsIfAny != null) {
            for (Procedure prevProc : prevProcsIfAny) {
                for (Statement prevStmt : prevProc.getStatements()) {
                    addStatementToCache(prevStmt);
                }
            }
        }

        // Ignore class dependencies if ignoring java stored procs.
        // This extra qualification anticipates some (undesirable) overlap between planner
        // testing and additional library code in the catalog jar file.
        // That is, if it became possible for ddl file syntax to trigger additional
        // (non-stored-procedure) class loading into the catalog jar,
        // planner-only testing would find it convenient to ignore those
        // dependencies for its "dry run" on an unchanged application ddl file.
        if (whichProcs == DdlProceduresToLoad.ALL_DDL_PROCEDURES) {
            // Add all the class dependencies to the output jar
            for (final Class<?> classDependency : classDependencies) {
                addClassToJar(jarOutput, classDependency);
            }
        }

        final List<ProcedureDescriptor> procedures = new ArrayList<>();
        procedures.addAll(allProcs);

        // Actually parse and handle all the Procedures
        for (final ProcedureDescriptor procedureDescriptor : procedures) {
            final String procedureName = procedureDescriptor.m_className;
            if (procedureDescriptor.m_singleStmt == null) {
                m_currentFilename = procedureName.substring(procedureName.lastIndexOf('.') + 1);
                m_currentFilename += ".class";
            }
            else if (whichProcs == DdlProceduresToLoad.ONLY_SINGLE_STATEMENT_PROCEDURES) {
                // In planner test mode, especially within the plannerTester framework,
                // ignore any java procedures referenced in ddl CREATE PROCEDURE statements to allow
                // re-use of actual application ddl files without introducing class dependencies.
                // This potentially allows automatic plannerTester regression test support
                // for all the single-statement procedures of an unchanged application ddl file.
                continue;
            }
            else {
                m_currentFilename = procedureName;
            }
            ProcedureCompiler.compile(this, hsql, m_estimates, m_catalog, db, procedureDescriptor, jarOutput);
        }
        // done handling files
        m_currentFilename = NO_FILENAME;

        // allow gc to reclaim any cache memory here
        m_previousCatalogStmts.clear();
    }

    /** Provide a feedback path to monitor plan output via harvestCapturedDetail */
    public void enableDetailedCapture() {
        m_capturedDiagnosticDetail = new ArrayList<>();
    }

    /** Access recent plan output, for diagnostic purposes */
    public List<String> harvestCapturedDetail() {
        List<String> harvested = m_capturedDiagnosticDetail;
        m_capturedDiagnosticDetail = null;
        return harvested;
    }

    /** Capture plan context info -- statement, cost, high-level "explain". */
    public void captureDiagnosticContext(String planDescription) {
        if (m_capturedDiagnosticDetail == null) {
            return;
        }
        m_capturedDiagnosticDetail.add(planDescription);
    }

    /** Capture plan content in terse json format. */
    public void captureDiagnosticJsonFragment(String json) {
        if (m_capturedDiagnosticDetail == null) {
            return;
        }
        m_capturedDiagnosticDetail.add(json);
    }

    static void addDatabaseEstimatesInfo(final DatabaseEstimates estimates, final Database db) {
        // Not implemented yet. Don't panic.

        /*for (Table table : db.getTables()) {
            DatabaseEstimates.TableEstimates tableEst = new DatabaseEstimates.TableEstimates();
            tableEst.maxTuples = 1000000;
            tableEst.minTuples = 100000;
            estimates.tables.put(table, tableEst);
        }*/
    }

    void addExportTableToConnector(final String targetName, final String tableName, final Database catdb)
            throws VoltCompilerException
    {
        assert tableName != null && ! tableName.trim().isEmpty() && catdb != null;

        // Catalog Connector
        org.voltdb.catalog.Connector catconn = catdb.getConnectors().getIgnoreCase(targetName);
        if (catconn == null) {
            catconn = catdb.getConnectors().add(targetName);
        }
        org.voltdb.catalog.Table tableref = catdb.getTables().getIgnoreCase(tableName);
        if (tableref == null) {
            throw new VoltCompilerException("While configuring export, table " + tableName + " was not present in " +
            "the catalog.");
        }

        // streams cannot have tuple limits
        if (tableref.getTuplelimit() != Integer.MAX_VALUE) {
            throw new VoltCompilerException("Streams cannot have row limits configured");
        }
        Column pc = tableref.getPartitioncolumn();
        //Get views
        List<Table> tlist = CatalogUtil.getMaterializeViews(catdb, tableref);
        if (pc == null && tlist.size() != 0) {
            compilerLog.error("While configuring export, stream " + tableName + " is a source table " +
                    "for a materialized view. Streams support views as long as partitioned column is part of the view.");
            throw new VoltCompilerException("Stream configured with materialized view without partitioned column.");
        }
        if (pc != null && pc.getName() != null && tlist.size() != 0) {
            for (Table t : tlist) {
                if (t.getColumns().get(pc.getName()) == null) {
                    compilerLog.error("While configuring export, table " + t + " is a source table " +
                            "for a materialized view. Export only tables support views as long as partitioned column is part of the view.");
                    throw new VoltCompilerException("Stream configured with materialized view without partitioned column in the view.");
                } else {
                    //Set partition column of view table to partition column of stream
                    t.setPartitioncolumn(t.getColumns().get(pc.getName()));
                }
            }
        }
        if (tableref.getMaterializer() != null)
        {
            compilerLog.error("While configuring export, " + tableName + " is a " +
                                        "materialized view.  A view cannot be export source.");
            throw new VoltCompilerException("View configured as export source");
        }
        if (tableref.getIndexes().size() > 0) {
            compilerLog.error("While configuring export, stream " + tableName + " has indexes defined. " +
                    "Streams can't have indexes (including primary keys).");
            throw new VoltCompilerException("Streams cannot be configured with indexes");
        }
        if (tableref.getIsreplicated()) {
            // if you don't specify partition columns, make
            // export tables partitioned, but on no specific column (iffy)
            tableref.setIsreplicated(false);
            tableref.setPartitioncolumn(null);
        }
        org.voltdb.catalog.ConnectorTableInfo connTableInfo =
                catconn.getTableinfo().getIgnoreCase(tableName);

        if (connTableInfo == null) {
            connTableInfo = catconn.getTableinfo().add(tableName);
            connTableInfo.setTable(tableref);
            connTableInfo.setAppendonly(true);
        }
        else  {
            throw new VoltCompilerException(String.format(
                    "Table \"%s\" is already exported", tableName
                    ));
        }

    }

    void compileDRTable(final Entry<String, String> drNode, final Database db)
            throws VoltCompilerException
    {
        String tableName = drNode.getKey();
        String action = drNode.getValue();

        org.voltdb.catalog.Table tableref = db.getTables().getIgnoreCase(tableName);
        if (tableref.getMaterializer() != null) {
            throw new VoltCompilerException("While configuring dr, table " + tableName + " is a materialized view." +
                                            " DR does not support materialized view.");
        }

        if (action.equalsIgnoreCase("DISABLE")) {
            tableref.setIsdred(false);
        } else {
            tableref.setIsdred(true);
        }
    }

    // Usage messages for new and legacy syntax.
    static final String usageNew    = "VoltCompiler <output-JAR> <input-DDL> ...";
    static final String usageLegacy = "VoltCompiler <project-file> <output-JAR>";

    /**
     * Main
     *
     * Incoming arguments:
     *
     *         New syntax: OUTPUT_JAR INPUT_DDL ...
     *      Legacy syntax: PROJECT_FILE OUTPUT_JAR
     *
     * @param args  arguments (see above)
     */
    public static void main(final String[] args)
    {
        // passing true to constructor indicates the compiler is being run in standalone mode
        final VoltCompiler compiler = new VoltCompiler(true);

        boolean success = false;
        if (args.length > 0 && args[0].toLowerCase().endsWith(".jar")) {
            // The first argument is *.jar for the new syntax.
            if (args.length >= 2) {
                // Check for accidental .jar or .xml files specified for argument 2
                // to catch accidental incomplete use of the legacy syntax.
                if (args[1].toLowerCase().endsWith(".xml") || args[1].toLowerCase().endsWith(".jar")) {
                    System.err.println("Error: Expecting a DDL file as the second argument.\n"
                                     + "      .xml and .jar are invalid DDL file extensions.");
                    System.exit(-1);
                }
                success = compiler.compileFromDDL(args[0], ArrayUtils.subarray(args, 1, args.length));
            }
            else {
                System.err.printf("Usage: %s\n", usageNew);
                System.exit(-1);
            }
        }
        else {
            // Can't recognize the arguments or there are no arguments.
            System.err.printf("Usage: %s\n       %s\n", usageNew, usageLegacy);
            System.exit(-1);
        }

        // Should have exited if inadequate arguments were provided.
        assert(args.length > 0);

        // Exit with error code if we failed
        if (!success) {
            compiler.summarizeErrors(System.out, null);
            System.exit(-1);
        }
        compiler.summarizeSuccess(System.out, null, args[0]);
    }

    public void summarizeSuccess(PrintStream outputStream, PrintStream feedbackStream, String jarOutputPath) {
        if (outputStream != null) {

            Database database = getCatalogDatabase();

            outputStream.println("------------------------------------------");
            outputStream.println("Successfully created " + jarOutputPath);

            for (String ddl : m_ddlFilePaths.keySet()) {
                outputStream.println("Includes schema: " + m_ddlFilePaths.get(ddl));
            }

            outputStream.println();

            // Accumulate a summary of the summary for a briefer report
            ArrayList<Procedure> nonDetProcs = new ArrayList<>();
            ArrayList<Procedure> tableScans = new ArrayList<>();
            int countSinglePartition = 0;
            int countMultiPartition = 0;
            int countDefaultProcs = 0;

            for (Procedure p : database.getProcedures()) {
                if (p.getSystemproc()) {
                    continue;
                }

                // Aggregate statistics about MP/SP/SEQ
                if (!p.getDefaultproc()) {
                    if (p.getSinglepartition()) {
                        countSinglePartition++;
                    }
                    else {
                        countMultiPartition++;
                    }
                }
                else {
                    countDefaultProcs++;
                }
                if (p.getHasseqscans()) {
                    tableScans.add(p);
                }

                outputStream.printf("[%s][%s] %s\n",
                                      p.getSinglepartition() ? "SP" : "MP",
                                      p.getReadonly() ? "READ" : "WRITE",
                                      p.getTypeName());
                for (Statement s : p.getStatements()) {
                    String seqScanTag = "";
                    if (s.getSeqscancount() > 0) {
                        seqScanTag = "[TABLE SCAN] ";
                    }
                    String determinismTag = "";

                    // if the proc is a java stored proc that is read&write,
                    // output determinism warnings
                    if (p.getHasjava() && (!p.getReadonly())) {
                        if (s.getIscontentdeterministic() == false) {
                            determinismTag = "[NDC] ";
                            nonDetProcs.add(p);
                        }
                        else if (s.getIsorderdeterministic() == false) {
                            determinismTag = "[NDO] ";
                            nonDetProcs.add(p);
                        }
                    }

                    String statementLine;
                    String sqlText = s.getSqltext();
                    sqlText = squeezeWhitespace(sqlText);
                    if (seqScanTag.length() + determinismTag.length() + sqlText.length() > 80) {
                        statementLine = "  " + (seqScanTag + determinismTag + sqlText).substring(0, 80) + "...";
                    } else {
                        statementLine = "  " + seqScanTag + determinismTag + sqlText;
                    }
                    outputStream.println(statementLine);
                }
                outputStream.println();
            }
            outputStream.println("------------------------------------------\n");

            if (m_addedClasses.length > 0) {

                if (m_addedClasses.length > 10) {
                    outputStream.printf("Added %d additional classes to the catalog jar.\n\n",
                            m_addedClasses.length);
                }
                else {
                    String logMsg = "Added the following additional classes to the catalog jar:\n";
                    for (String className : m_addedClasses) {
                        logMsg += "  " + className + "\n";
                    }
                    outputStream.println(logMsg);
                }

                outputStream.println("------------------------------------------\n");
            }

            //
            // post-compile summary and legend.
            //
            outputStream.printf(
                    "Catalog contains %d built-in CRUD procedures.\n" +
                    "\tSimple insert, update, delete, upsert and select procedures are created\n" +
                    "\tautomatically for convenience.\n\n",
                    countDefaultProcs);
            if (countSinglePartition > 0) {
                outputStream.printf(
                        "[SP] Catalog contains %d single partition procedures.\n" +
                        "\tSingle partition procedures run in parallel and scale\n" +
                        "\tas partitions are added to a cluster.\n\n",
                        countSinglePartition);
            }
            if (countMultiPartition > 0) {
                outputStream.printf(
                        "[MP] Catalog contains %d multi-partition procedures.\n" +
                        "\tMulti-partition procedures run globally at all partitions\n" +
                        "\tand do not run in parallel with other procedures.\n\n",
                        countMultiPartition);
            }
            if (!tableScans.isEmpty()) {
                outputStream.printf("[TABLE SCAN] Catalog contains %d procedures that use a table scan:\n\n",
                        tableScans.size());
                for (Procedure p : tableScans) {
                    outputStream.println("\t\t" + p.getClassname());
                }
                outputStream.printf(
                        "\n\tTable scans do not use indexes and may become slower as tables grow.\n\n");
            }
            if (!nonDetProcs.isEmpty()) {
                outputStream.println(
                        "[NDO][NDC] NON-DETERMINISTIC CONTENT OR ORDER WARNING:\n" +
                        "\tThe procedures listed below contain non-deterministic queries.\n");

                for (Procedure p : nonDetProcs) {
                    outputStream.println("\t\t" + p.getClassname());
                }

                outputStream.printf(
                        "\n" +
                        "\tUsing the output of these queries as input to subsequent\n" +
                        "\twrite queries can result in differences between replicated\n" +
                        "\tpartitions at runtime, forcing VoltDB to shutdown the cluster.\n" +
                        "\tReview the compiler messages above to identify the offending\n" +
                        "\tSQL statements (marked as \"[NDO] or [NDC]\").  Add a unique\n" +
                        "\tindex to the schema or an explicit ORDER BY clause to the\n" +
                        "\tquery to make these queries deterministic.\n\n");
            }
            if (countSinglePartition == 0 && countMultiPartition > 0) {
                outputStream.printf(
                        "ALL MULTI-PARTITION WARNING:\n" +
                        "\tAll of the user procedures are multi-partition. This often\n" +
                        "\tindicates that the application is not utilizing VoltDB partitioning\n" +
                        "\tfor best performance. For information on VoltDB partitioning, see:\n"+
                        "\thttp://voltdb.com/docs/UsingVoltDB/ChapAppDesign.php\n\n");
            }
            if (m_reportPath != null) {
                outputStream.println("------------------------------------------\n");
                outputStream.println(String.format(
                        "Full catalog report can be found at file://%s.\n",
                        m_reportPath));
            }
            outputStream.println("------------------------------------------\n");
        }
        if (feedbackStream != null) {
            for (Feedback fb : m_warnings) {
                feedbackStream.println(fb.getLogString());
            }
            for (Feedback fb : m_infos) {
                feedbackStream.println(fb.getLogString());
            }
        }
    }

    /**
     * Return a copy of the input sqltext with each run of successive whitespace characters replaced by a single space.
     * This is just for informal feedback purposes, so quoting is not respected.
     * @param sqltext
     * @return a possibly modified copy of the input sqltext
     **/
    private static String squeezeWhitespace(String sqltext) {
        String compact = sqltext.replaceAll("\\s+", " ");
        return compact;
    }

    public void summarizeErrors(PrintStream outputStream, PrintStream feedbackStream) {
        if (outputStream != null) {
            outputStream.println("------------------------------------------");
            outputStream.println("Catalog compilation failed.");
            outputStream.println("------------------------------------------");
        }
        if (feedbackStream != null) {
            for (Feedback fb : m_errors) {
                feedbackStream.println(fb.getLogString());
            }
        }
    }

    // this needs to be reset in the main compile func
    private static final HashSet<Class<?>> cachedAddedClasses = new HashSet<>();


    public List<Class<?>> getInnerClasses(Class <?> c)
            throws VoltCompilerException {
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        ClassLoader cl = c.getClassLoader();
        if (cl == null) {
            cl = Thread.currentThread().getContextClassLoader();
        }

        // if loading from an InMemoryJarFile, the process is a bit different...
        if (cl instanceof JarLoader) {
            String[] classes = ((JarLoader) cl).getInnerClassesForClass(c.getName());
            for (String innerName : classes) {
                Class<?> clz = null;
                try {
                    clz = cl.loadClass(innerName);
                }
                catch (ClassNotFoundException e) {
                    String msg = "Unable to load " + c + " inner class " + innerName +
                            " from in-memory jar representation.";
                    throw new VoltCompilerException(msg);
                }
                assert(clz != null);
                builder.add(clz);
            }
        }
        else {
            String stem = c.getName().replace('.', '/');
            String cpath = stem + ".class";
            URL curl = cl.getResource(cpath);
            if (curl == null) {
                throw new VoltCompilerException(String.format(
                        "Failed to find class file %s in jar.", cpath));
            }

            // load from an on-disk jar
            if ("jar".equals(curl.getProtocol())) {
                Pattern nameRE = Pattern.compile("\\A(" + stem + "\\$[^/]+).class\\z");
                String jarFN;
                try {
                    jarFN = URLDecoder.decode(curl.getFile(), "UTF-8");
                }
                catch (UnsupportedEncodingException e) {
                    String msg = "Unable to UTF-8 decode " + curl.getFile() + " for class " + c;
                    throw new VoltCompilerException(msg);
                }
                jarFN = jarFN.substring(5, jarFN.indexOf('!'));
                JarFile jar = null;
                try {
                    jar = new JarFile(jarFN);
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        String name = entries.nextElement().getName();
                        Matcher mtc = nameRE.matcher(name);
                        if (mtc.find()) {
                            String innerName = mtc.group(1).replace('/', '.');
                            Class<?> inner;
                            try {
                                inner = cl.loadClass(innerName);
                            } catch (ClassNotFoundException e) {
                                String msg = "Unable to load " + c + " inner class " + innerName;
                                throw new VoltCompilerException(msg);
                            }
                            builder.add(inner);
                        }
                    }
                }
                catch (IOException e) {
                    String msg = "Cannot access class " + c + " source code location of " + jarFN;
                    throw new VoltCompilerException(msg);
                }
                finally {
                    if ( jar != null) try {jar.close();} catch (Exception ignoreIt) {};
                }
            }
            // load directly from a classfile
            else if ("file".equals(curl.getProtocol())) {
                Pattern nameRE = Pattern.compile("/(" + stem + "\\$[^/]+).class\\z");
                File sourceDH = new File(curl.getFile()).getParentFile();
                for (File f: sourceDH.listFiles()) {
                    Matcher mtc = nameRE.matcher(f.getAbsolutePath());
                    if (mtc.find()) {
                        String innerName = mtc.group(1).replace('/', '.');
                        Class<?> inner;
                        try {
                            inner = cl.loadClass(innerName);
                        } catch (ClassNotFoundException e) {
                            String msg = "Unable to load " + c + " inner class " + innerName;
                            throw new VoltCompilerException(msg);
                        }
                        builder.add(inner);
                    }
                }

            }
        }
        return builder.build();
    }

    public boolean addClassToJar(InMemoryJarfile jarOutput, final Class<?> cls)
            throws VoltCompiler.VoltCompilerException
    {
        if (cachedAddedClasses.contains(cls)) {
            return false;
        } else {
            cachedAddedClasses.add(cls);
        }

        for (final Class<?> nested : getInnerClasses(cls)) {
            addClassToJar(jarOutput, nested);
        }

        try {
            return VoltCompilerUtils.addClassToJar(jarOutput, cls);
        } catch (IOException e) {
            throw new VoltCompilerException(e.getMessage());
        }
    }

    /**
     * @param m_procInfoOverrides the m_procInfoOverrides to set
     */
    public void setProcInfoOverrides(Map<String, ProcInfoData> procInfoOverrides) {
        m_procInfoOverrides = procInfoOverrides;
    }

    /**
     * Helper enum that scans sax exception messages for deprecated xml elements
     *
     * @author ssantoro
     */
    enum DeprecatedProjectElement {
        security(
                "(?i)\\Acvc-[^:]+:\\s+Invalid\\s+content\\s+.+?\\s+element\\s+'security'",
                "security may be enabled in the deployment file only"
                );

        /**
         * message regular expression that pertains to the deprecated element
         */
        private final Pattern messagePattern;
        /**
         * a suggestion string to exaplain alternatives
         */
        private final String suggestion;

        DeprecatedProjectElement(String messageRegex, String suggestion) {
            this.messagePattern = Pattern.compile(messageRegex);
            this.suggestion = suggestion;
        }

        String getSuggestion() {
            return suggestion;
        }

        /**
         * Given a JAXBException it determines whether or not the linked
         * exception is associated with a deprecated xml elements
         *
         * @param jxbex a {@link JAXBException}
         * @return an enum of {@code DeprecatedProjectElement} if the
         *    given exception corresponds to a deprecated xml element
         */
        static DeprecatedProjectElement valueOf( JAXBException jxbex) {
            if(    jxbex == null
                || jxbex.getLinkedException() == null
                || ! (jxbex.getLinkedException() instanceof org.xml.sax.SAXParseException)
            ) {
                return null;
            }
            org.xml.sax.SAXParseException saxex =
                    org.xml.sax.SAXParseException.class.cast(jxbex.getLinkedException());
            for( DeprecatedProjectElement dpe: DeprecatedProjectElement.values()) {
                Matcher mtc = dpe.messagePattern.matcher(saxex.getMessage());
                if( mtc.find()) return dpe;
            }

            return null;
        }
    }

    /**
     * Compile the provided jarfile.  Basically, treat the jarfile as a staging area
     * for the artifacts to be included in the compile, and then compile it in place.
     *
     * *NOTE*: Does *NOT* work with project.xml jarfiles.
     *
     * @return the compiled catalog is contained in the provided jarfile.
     * @throws VoltCompilerException
     *
     */
    public void compileInMemoryJarfileWithNewDDL(InMemoryJarfile jarfile, String newDDL, Catalog oldCatalog) throws IOException, VoltCompilerException
    {
        String oldDDL = new String(jarfile.get(VoltCompiler.AUTOGEN_DDL_FILE_NAME),
                Constants.UTF8ENCODING);
        compilerLog.trace("OLD DDL: " + oldDDL);

        VoltCompilerStringReader canonicalDDLReader = null;
        VoltCompilerStringReader newDDLReader = null;

        // Use the in-memory jarfile-provided class loader so that procedure
        // classes can be found and copied to the new file that gets written.
        ClassLoader originalClassLoader = m_classLoader;
        try {
            canonicalDDLReader = new VoltCompilerStringReader(VoltCompiler.AUTOGEN_DDL_FILE_NAME, oldDDL);
            newDDLReader = new VoltCompilerStringReader("Ad Hoc DDL Input", newDDL);

            List<VoltCompilerReader> ddlList = new ArrayList<>();
            ddlList.add(newDDLReader);

            m_classLoader = jarfile.getLoader();
            // Do the compilation work.
            InMemoryJarfile jarOut = compileInternal(canonicalDDLReader, oldCatalog, ddlList, jarfile);
            // Trim the compiler output to try to provide a concise failure
            // explanation
            if (jarOut == null) {
                String errString = "Adhoc DDL failed";
                if (m_errors.size() > 0) {
                    errString = m_errors.get(m_errors.size() - 1).getLogString();
                }

                int endtrim = errString.indexOf(" in statement starting");
                if (endtrim < 0) { endtrim = errString.length(); }
                String trimmed = errString.substring(0, endtrim);
                throw new VoltCompilerException(trimmed);
            }
            compilerLog.debug("Successfully recompiled InMemoryJarfile");
        }
        finally {
            // Restore the original class loader
            m_classLoader = originalClassLoader;

            if (canonicalDDLReader != null) {
                try {
                    canonicalDDLReader.close();
                }
                catch (IOException ioe) {}
            }
            if (newDDLReader != null) {
                try {
                    newDDLReader.close();
                }
                catch (IOException ioe) {}
            }
        }
    }

    /**
     * Compile the provided jarfile.  Basically, treat the jarfile as a staging area
     * for the artifacts to be included in the compile, and then compile it in place.
     *
     * *NOTE*: Does *NOT* work with project.xml jarfiles.
     *
     * @return the compiled catalog is contained in the provided jarfile.
     *
     */
    public void compileInMemoryJarfile(InMemoryJarfile jarfile) throws IOException
    {
        // Gather DDL files for recompilation
        List<VoltCompilerReader> ddlReaderList = new ArrayList<>();
        Entry<String, byte[]> entry = jarfile.firstEntry();
        while (entry != null) {
            String path = entry.getKey();
            // SOMEDAY: It would be better to have a manifest that explicitly lists
            // ddl files instead of using a brute force *.sql glob.
            if (path.toLowerCase().endsWith(".sql")) {
                ddlReaderList.add(new VoltCompilerJarFileReader(jarfile, path));
                compilerLog.trace("Added SQL file from jarfile to compilation: " + path);
            }
            entry = jarfile.higherEntry(entry.getKey());
        }

        // Use the in-memory jarfile-provided class loader so that procedure
        // classes can be found and copied to the new file that gets written.
        ClassLoader originalClassLoader = m_classLoader;
        try {
            m_classLoader = jarfile.getLoader();
            // Do the compilation work.
            InMemoryJarfile jarOut = compileInternal(null, null, ddlReaderList, jarfile);
            // Trim the compiler output to try to provide a concise failure
            // explanation
            if (jarOut != null) {
                compilerLog.debug("Successfully recompiled InMemoryJarfile");
            }
            else {
                String errString = "Adhoc DDL failed";
                if (m_errors.size() > 0) {
                    errString = m_errors.get(m_errors.size() - 1).getLogString();
                }
                int fronttrim = errString.indexOf("DDL Error");
                if (fronttrim < 0) { fronttrim = 0; }
                int endtrim = errString.indexOf(" in statement starting");
                if (endtrim < 0) { endtrim = errString.length(); }
                String trimmed = errString.substring(fronttrim, endtrim);
                throw new IOException(trimmed);
            }
        }
        finally {
            // Restore the original class loader
            m_classLoader = originalClassLoader;
        }
    }

    /**
     * Check a loaded catalog. If it needs to be upgraded recompile it and save
     * an upgraded jar file.
     *
     * @param outputJar  in-memory jar file (updated in place here)
     * @return source version upgraded from or null if not upgraded
     * @throws IOException
     */
    public String upgradeCatalogAsNeeded(InMemoryJarfile outputJar)
                    throws IOException
    {
        // getBuildInfoFromJar() performs some validation.
        String[] buildInfoLines = CatalogUtil.getBuildInfoFromJar(outputJar);
        String versionFromCatalog = buildInfoLines[0];
        // Set if an upgrade happens.
        String upgradedFromVersion = null;

        // Check if it's compatible (or the upgrade is being forced).
        // getConfig() may return null if it's being mocked for a test.
        if (   VoltDB.Configuration.m_forceCatalogUpgrade
            || !versionFromCatalog.equals(VoltDB.instance().getVersionString())) {

            // Patch the buildinfo.
            String versionFromVoltDB = VoltDB.instance().getVersionString();
            buildInfoLines[0] = versionFromVoltDB;
            buildInfoLines[1] = String.format("voltdb-auto-upgrade-to-%s", versionFromVoltDB);
            byte[] buildInfoBytes = StringUtils.join(buildInfoLines, "\n").getBytes();
            outputJar.put(CatalogUtil.CATALOG_BUILDINFO_FILENAME, buildInfoBytes);

            // Gather DDL files for recompilation if not using a project file.
            List<VoltCompilerReader> ddlReaderList = new ArrayList<>();
            Entry<String, byte[]> entry = outputJar.firstEntry();
            while (entry != null) {
                String path = entry.getKey();
                //TODO: It would be better to have a manifest that explicitly lists
                // ddl files instead of using a brute force *.sql glob.
                if (path.toLowerCase().endsWith(".sql")) {
                    ddlReaderList.add(new VoltCompilerJarFileReader(outputJar, path));
                }
                entry = outputJar.higherEntry(entry.getKey());
            }

            // Use the in-memory jarfile-provided class loader so that procedure
            // classes can be found and copied to the new file that gets written.
            ClassLoader originalClassLoader = m_classLoader;

            // Compile and save the file to voltdbroot. Assume it's a test environment if there
            // is no catalog context available.
            String jarName = String.format("catalog-%s.jar", versionFromVoltDB);
            String textName = String.format("catalog-%s.out", versionFromVoltDB);
            CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
            final String outputJarPath = (catalogContext != null
                    ? new File(VoltDB.instance().getVoltDBRootPath(), jarName).getPath()
                    : VoltDB.Configuration.getPathToCatalogForTest(jarName));
            // Place the compiler output in a text file in the same folder.
            final String outputTextPath = (catalogContext != null
                    ? new File(VoltDB.instance().getVoltDBRootPath(), textName).getPath()
                    : VoltDB.Configuration.getPathToCatalogForTest(textName));
            try {
                m_classLoader = outputJar.getLoader();

                consoleLog.info(String.format(
                        "Version %s catalog will be automatically upgraded to version %s.",
                        versionFromCatalog, versionFromVoltDB));

                // Do the compilation work.
                boolean success = compileInternalToFile(outputJarPath, null, null, ddlReaderList, outputJar);

                // Sanitize the *.sql files in the jarfile so that only the autogenerated
                // canonical DDL file will be used for future compilations
                // Bomb out if we failed to generate the canonical DDL
                if (success) {
                    boolean foundCanonicalDDL = false;
                    entry = outputJar.firstEntry();
                    while (entry != null) {
                        String path = entry.getKey();
                        if (path.toLowerCase().endsWith(".sql")) {
                            if (!path.toLowerCase().equals(AUTOGEN_DDL_FILE_NAME)) {
                                outputJar.remove(path);
                            }
                            else {
                                foundCanonicalDDL = true;
                            }
                        }
                        entry = outputJar.higherEntry(entry.getKey());
                    }
                    success = foundCanonicalDDL;
                }

                if (success) {
                    // Set up the return string.
                    upgradedFromVersion = versionFromCatalog;
                }

                // Summarize the results to a file.
                // Briefly log success or failure and mention the output text file.
                PrintStream outputStream = new PrintStream(outputTextPath);
                try {
                    if (success) {
                        summarizeSuccess(outputStream, outputStream, outputJarPath);
                        consoleLog.info(String.format(
                                "The catalog was automatically upgraded from " +
                                "version %s to %s and saved to \"%s\". " +
                                "Compiler output is available in \"%s\".",
                                versionFromCatalog, versionFromVoltDB,
                                outputJarPath, outputTextPath));
                    }
                    else {
                        summarizeErrors(outputStream, outputStream);
                        outputStream.close();
                        compilerLog.error("Catalog upgrade failed.");
                        compilerLog.info(String.format(
                                "Had attempted to perform an automatic version upgrade of a " +
                                "catalog that was compiled by an older %s version of VoltDB, " +
                                "but the automatic upgrade failed. The cluster  will not be " +
                                "able to start until the incompatibility is fixed. " +
                                "Try re-compiling the catalog with the newer %s version " +
                                "of the VoltDB compiler. Compiler output from the failed " +
                                "upgrade is available in \"%s\".",
                                versionFromCatalog, versionFromVoltDB, outputTextPath));
                        throw new IOException(String.format(
                                "Catalog upgrade failed. You will need to recompile using voltdb compile."));
                    }
                }
                finally {
                    outputStream.close();
                }
            }
            catch (IOException ioe) {
                // Do nothing because this could come from the normal failure path
                throw ioe;
            }
            catch (Exception e) {
                compilerLog.error("Catalog upgrade failed with error:");
                compilerLog.error(e.getMessage());
                compilerLog.info(String.format(
                        "Had attempted to perform an automatic version upgrade of a " +
                        "catalog that was compiled by an older %s version of VoltDB, " +
                        "but the automatic upgrade failed. The cluster  will not be " +
                        "able to start until the incompatibility is fixed. " +
                        "Try re-compiling the catalog with the newer %s version " +
                        "of the VoltDB compiler. Compiler output from the failed " +
                        "upgrade is available in \"%s\".",
                        versionFromCatalog, versionFromVoltDB, outputTextPath));
                throw new IOException(String.format(
                        "Catalog upgrade failed. You will need to recompile using voltdb compile."));
            }
            finally {
                // Restore the original class loader
                m_classLoader = originalClassLoader;
            }
        }
        return upgradedFromVersion;
    }

    /**
     * Note that a table changed in order to invalidate potential cached
     * statements that reference the changed table.
     */
    void markTableAsDirty(String tableName) {
        m_dirtyTables.add(tableName.toLowerCase());
    }

    /**
     * Key prefix includes attributes that make a cached statement usable if they match
     *
     * For example, if the SQL is the same, but the partitioning isn't, then the statements
     * aren't actually interchangeable.
     */
    String getKeyPrefix(StatementPartitioning partitioning, DeterminismMode detMode, String joinOrder) {
        // no caching for inferred yet
        if (partitioning.isInferred()) {
            return null;
        }

        String joinOrderPrefix = "#";
        if (joinOrder != null) {
            joinOrderPrefix += joinOrder;
        }

        boolean partitioned = partitioning.wasSpecifiedAsSingle();

        return joinOrderPrefix + String.valueOf(detMode.toChar()) + (partitioned ? "P#" : "R#");
    }

    void addStatementToCache(Statement stmt) {
        String key = stmt.getCachekeyprefix() + stmt.getSqltext();
        m_previousCatalogStmts.put(key, stmt);
    }

    // track hits and misses for debugging
    static long m_stmtCacheHits = 0;
    static long m_stmtCacheMisses = 0;

    /** Look for a match from the previous catalog that matches the key + sql */
    Statement getCachedStatement(String keyPrefix, String sql) {
        String key = keyPrefix + sql;

        Statement candidate = m_previousCatalogStmts.get(key);
        if (candidate == null) {
            ++m_stmtCacheMisses;
            return null;
        }

        // check that no underlying tables have been modified since the proc had been compiled
        String[] tablesTouched = candidate.getTablesread().split(",");
        for (String tableName : tablesTouched) {
            if (m_dirtyTables.contains(tableName.toLowerCase())) {
                ++m_stmtCacheMisses;
                return null;
            }
        }
        tablesTouched = candidate.getTablesupdated().split(",");
        for (String tableName : tablesTouched) {
            if (m_dirtyTables.contains(tableName.toLowerCase())) {
                ++m_stmtCacheMisses;
                return null;
            }
        }

        ++m_stmtCacheHits;
        // easy debugging stmt
        //printStmtCacheStats();
        return candidate;
    }

    @SuppressWarnings("unused")
    private void printStmtCacheStats() {
        System.out.printf("Hits: %d, Misses %d, Percent %.2f\n",
                m_stmtCacheHits, m_stmtCacheMisses,
                (m_stmtCacheHits * 100.0) / (m_stmtCacheHits + m_stmtCacheMisses));
        System.out.flush();
    }
}
