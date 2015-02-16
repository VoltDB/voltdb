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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext;
import org.voltdb.ProcInfoData;
import org.voltdb.RealVoltDB;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.FilteredCatalogDiffEngine;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.common.Permission;
import org.voltdb.compiler.projectfile.ClassdependenciesType.Classdependency;
import org.voltdb.compiler.projectfile.DatabaseType;
import org.voltdb.compiler.projectfile.ExportType;
import org.voltdb.compiler.projectfile.ExportType.Tables;
import org.voltdb.compiler.projectfile.GroupsType;
import org.voltdb.compiler.projectfile.PartitionsType;
import org.voltdb.compiler.projectfile.ProceduresType;
import org.voltdb.compiler.projectfile.ProjectType;
import org.voltdb.compiler.projectfile.RolesType;
import org.voltdb.compiler.projectfile.SchemasType;
import org.voltdb.compilereport.ReportMaker;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.utils.CatalogSchemaTools;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.InMemoryJarfile.JarLoader;
import org.voltdb.utils.LogKeys;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Compiles a project XML file and some metadata into a Jarfile
 * containing stored procedure code and a serialzied catalog.
 *
 */
public class VoltCompiler {
    /** Represents the level of severity for a Feedback message generated during compiling. */
    public static enum Severity { INFORMATIONAL, WARNING, ERROR, UNEXPECTED }
    public static final int NO_LINE_NUMBER = -1;

    // Causes the "debugoutput" folder to be generated and populated.
    // Also causes explain plans on disk to include cost.
    public final static boolean DEBUG_MODE = System.getProperties().contains("compilerdebug");

    // was this voltcompiler instantiated in a main(), or as part of VoltDB
    public final boolean standaloneCompiler;

    // tables that change between the previous compile and this one
    // used for Live-DDL caching of plans
    private Set<String> m_dirtyTables = new TreeSet<>();
    // A collection of statements from the previous catalog
    // used for Live-DDL caching of plans
    private Map<String, Statement> m_previousCatalogStmts = new HashMap<>();

    // feedback by filename
    ArrayList<Feedback> m_infos = new ArrayList<Feedback>();
    ArrayList<Feedback> m_warnings = new ArrayList<Feedback>();
    ArrayList<Feedback> m_errors = new ArrayList<Feedback>();

    // set of annotations by procedure name
    private Map<String, ProcInfoData> m_procInfoOverrides = null;

    // Name of DDL file built by the DDL VoltCompiler from the catalog and added to the jar.
    public static String AUTOGEN_DDL_FILE_NAME = "autogen-ddl.sql";
    // Environment variable used to verify that a catalog created from autogen-dll.sql is effectively
    // identical to the original catalog that was used to create the autogen-ddl.sql file.
    public static final boolean DEBUG_VERIFY_CATALOG = Boolean.valueOf(System.getenv().get("VERIFY_CATALOG_DEBUG"));

    String m_projectFileURL = null;
    String m_currentFilename = null;
    Map<String, String> m_ddlFilePaths = new HashMap<String, String>();
    String[] m_addedClasses = null;
    String[] m_importLines = null;

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
     * Compile from a set of DDL files, but no project.xml.
     *
     * @param jarOutputPath The location to put the finished JAR to.
     * @param ddlFilePaths The array of DDL files to compile (at least one is required).
     * @return true if successful
     * @throws VoltCompilerException
     */
    public boolean compileFromDDL(
            final String jarOutputPath,
            final String... ddlFilePaths)
                    throws VoltCompilerException
    {
        return compileWithProjectXML(null, jarOutputPath, ddlFilePaths);
    }

    /**
     * Compile optionally using a (DEPRECATED) project.xml file.
     * This internal method prepares to compile with or without a project file.
     *
     * @param projectFileURL URL of the project file or NULL if not used.
     * @param jarOutputPath The location to put the finished JAR to.
     * @param ddlFilePaths The array of DDL files to compile (at least one is required if there's a project file).
     * @return true if successful
     */
    public boolean compileWithProjectXML(
            final String projectFileURL,
            final String jarOutputPath,
            final String... ddlFilePaths)
    {
        VoltCompilerReader projectReader = null;
        if (projectFileURL != null) {
            try {
                projectReader = new VoltCompilerFileReader(projectFileURL);
            }
            catch (IOException e) {
                compilerLog.error(String.format(
                        "Failed to initialize reader for project file \"%s\".",
                        projectFileURL));
                return false;
            }
        }
        else if (ddlFilePaths.length == 0) {
            compilerLog.error(String.format(
                    "At least one DDL file is required if no project file is specified.",
                    projectFileURL));
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
        return compileInternalToFile(projectReader, jarOutputPath, null, null, ddlReaderList, null);
    }

    /**
     * Compile empty catalog jar
     * @param jarOutputPath output jar path
     * @return true if successful
     */
    public boolean compileEmptyCatalog(final String jarOutputPath) {
        // Use a special DDL reader to provide the contents.
        List<VoltCompilerReader> ddlReaderList = new ArrayList<VoltCompilerReader>(1);
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
        return compileInternalToFile(null, jarOutputPath, null, null, ddlReaderList, jarFile);
    }

    private static void addBuildInfo(final InMemoryJarfile jarOutput) {
        StringBuilder buildinfo = new StringBuilder();
        String info[] = RealVoltDB.extractBuildInfo();
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
        List<VoltCompilerReader> autogenReaderList = new ArrayList<VoltCompilerReader>(1);
        autogenReaderList.add(new VoltCompilerJarFileReader(origJarFile, AUTOGEN_DDL_FILE_NAME));
        DatabaseType autoGenDatabase = getProjectDatabase(null);
        InMemoryJarfile autoGenJarOutput = new InMemoryJarfile();
        autoGenCompiler.m_currentFilename = AUTOGEN_DDL_FILE_NAME;
        Catalog autoGenCatalog = autoGenCompiler.compileCatalogInternal(autoGenDatabase, null, null,
                autogenReaderList, autoGenJarOutput);
        FilteredCatalogDiffEngine diffEng = new FilteredCatalogDiffEngine(origCatalog, autoGenCatalog);
        String diffCmds = diffEng.commands();
        if (diffCmds != null && !diffCmds.equals("")) {
            VoltDB.crashLocalVoltDB("Catalog Verification from Generated DDL failed! " +
                    "The offending diffcmds were: " + diffCmds);
        }
        else {
            Log.info("Catalog verification completed successfuly.");
        }
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
            final VoltCompilerReader projectReader,
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

        InMemoryJarfile jarOutput = compileInternal(projectReader, cannonicalDDLIfAny, previousCatalogIfAny, ddlReaderList, jarOutputRet);
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
            final VoltCompilerReader projectReader,
            final VoltCompilerReader cannonicalDDLIfAny,
            final Catalog previousCatalogIfAny,
            final List<VoltCompilerReader> ddlReaderList,
            final InMemoryJarfile jarOutputRet)
    {
        // Expect to have either >1 ddl file or a project file.
        assert(ddlReaderList.size() > 0 || projectReader != null);
        // Make a temporary local output jar if one wasn't provided.
        final InMemoryJarfile jarOutput = (jarOutputRet != null
                                                ? jarOutputRet
                                                : new InMemoryJarfile());
        m_projectFileURL = (projectReader != null ? projectReader.getPath() : null);

        if (m_projectFileURL == null && (ddlReaderList == null || ddlReaderList.isEmpty())) {
            addErr("One or more DDL files are required.");
            return null;
        }

        // clear out the warnings and errors
        m_warnings.clear();
        m_infos.clear();
        m_errors.clear();

        // do all the work to get the catalog
        DatabaseType database = getProjectDatabase(projectReader);
        if (database == null) {
            return null;
        }
        final Catalog catalog = compileCatalogInternal(database, cannonicalDDLIfAny, previousCatalogIfAny, ddlReaderList, jarOutput);
        if (catalog == null) {
            return null;
        }

        // Build DDL from Catalog Data
        m_canonicalDDL = CatalogSchemaTools.toSchema(catalog, m_importLines);

        // generate the catalog report and write it to disk
        try {
            m_report = ReportMaker.report(m_catalog, m_warnings, m_canonicalDDL);
            m_reportPath = null;
            File file = null;

            // write to working dir when using VoltCompiler directly
            if (standaloneCompiler) {
                file = new File("catalog-report.html");
            }
            else {
                // try to get a catalog context
                VoltDBInterface voltdb = VoltDB.instance();
                CatalogContext catalogContext = voltdb != null ? voltdb.getCatalogContext() : null;

                // it's possible that standaloneCompiler will be false and catalogContext will be null
                //   in test code.

                // if we have a context, write report to voltroot
                if (catalogContext != null) {
                    file = new File(catalogContext.cluster.getVoltroot(), "catalog-report.html");
                }
            }

            // if there's a good place to write the report, do so
            if (file != null) {
                FileWriter fw = new FileWriter(file);
                fw.write(m_report);
                fw.close();
                m_reportPath = file.getAbsolutePath();
            }
        } catch (IOException e) {
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
        HashMap<String, byte[]> retval = new HashMap<String, byte[]>();
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
        List<VoltCompilerReader> ddlReaderList = new ArrayList<VoltCompilerReader>(ddlFilePaths.length);
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
        DatabaseType database = getProjectDatabase(null);
        InMemoryJarfile jarOutput = new InMemoryJarfile();
        return compileCatalogInternal(database, null, null, DDLPathsToReaderList(ddlFilePaths), jarOutput);
    }

    /**
     * Compile from project file (without explicit DDL file paths).
     * @param projectFileURL  project file URL/path
     * @return  compiled catalog
     * @throws VoltCompilerException
     */
    public Catalog compileCatalogFromProject(final String projectFileURL)
            throws VoltCompilerException
    {
        VoltCompilerReader projectReader = null;
        try {
            projectReader = new VoltCompilerFileReader(projectFileURL);
        }
        catch (IOException e) {
            throw new VoltCompilerException(String.format(
                    "Unable to create project reader for \"%s\": %s",
                    projectFileURL, e.getMessage()));
        }
        DatabaseType database = getProjectDatabase(projectReader);
        InMemoryJarfile jarOutput = new InMemoryJarfile();
        // Provide an empty DDL reader list.
        return compileCatalogInternal(database, null, null, DDLPathsToReaderList(), jarOutput);
    }

    /**
     * Read the project file and get the database object.
     * @param projectFileURL  project file URL/path
     * @return  database for project or null
     */
    private DatabaseType getProjectDatabase(final VoltCompilerReader projectReader)
    {
        DatabaseType database = null;
        m_currentFilename = (projectReader != null ? projectReader.getName() : "null");
        if (projectReader != null) {
            try {
                JAXBContext jc = JAXBContext.newInstance("org.voltdb.compiler.projectfile");
                // This schema shot the sheriff.
                SchemaFactory sf = SchemaFactory.newInstance(
                  javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
                Schema schema = sf.newSchema(this.getClass().getResource("ProjectFileSchema.xsd"));
                Unmarshaller unmarshaller = jc.createUnmarshaller();
                // But did not shoot unmarshaller!
                unmarshaller.setSchema(schema);
                @SuppressWarnings("unchecked")
                JAXBElement<ProjectType> result = (JAXBElement<ProjectType>) unmarshaller.unmarshal(projectReader);
                ProjectType project = result.getValue();
                database = project.getDatabase();
            }
            catch (JAXBException e) {
                // Convert some linked exceptions to more friendly errors.
                if (e.getLinkedException() instanceof java.io.FileNotFoundException) {
                    addErr(e.getLinkedException().getMessage());
                    compilerLog.error(e.getLinkedException().getMessage());
                }
                else {
                    DeprecatedProjectElement deprecated = DeprecatedProjectElement.valueOf(e);
                    if( deprecated != null) {
                        addErr("Found deprecated XML element \"" + deprecated.name() + "\" in project.xml file, "
                                + deprecated.getSuggestion());
                        addErr("Error schema validating project.xml file. " + e.getLinkedException().getMessage());
                        compilerLog.error("Found deprecated XML element \"" + deprecated.name() + "\" in project.xml file");
                        compilerLog.error(e.getMessage());
                        compilerLog.error(projectReader.getPath());
                    }
                    else if (e.getLinkedException() instanceof org.xml.sax.SAXParseException) {
                        addErr("Error schema validating project.xml file. " + e.getLinkedException().getMessage());
                        compilerLog.error("Error schema validating project.xml file: " + e.getLinkedException().getMessage());
                        compilerLog.error(e.getMessage());
                        compilerLog.error(projectReader.getPath());
                    }
                    else {
                        throw new RuntimeException(e);
                    }
                }
            }
            catch (SAXException e) {
                addErr("Error schema validating project.xml file. " + e.getMessage());
                compilerLog.error("Error schema validating project.xml file. " + e.getMessage());
            }
        }
        else {
            // No project.xml - create a stub object.
            database = new DatabaseType();
        }

        return database;
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
            final DatabaseType database,
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

        if (database != null) {
            final String databaseName = database.getName();
            // schema does not verify that the database is named "database"
            if (databaseName.equals("database") == false) {
                return null; // error messaging handled higher up
            }
            // shutdown and make a new hsqldb
            try {
                Database previousDBIfAny = null;
                if (previousCatalogIfAny != null) {
                    previousDBIfAny = previousCatalogIfAny.getClusters().get("cluster").getDatabases().get("database");
                }
                compileDatabaseNode(database, cannonicalDDLIfAny, previousDBIfAny, ddlReaderList, jarOutput);
            } catch (final VoltCompilerException e) {
                return null;
            }
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
        compileDatabase(db, hsql, voltDdlTracker, null, null, ddlReaderList, null, null, whichProcs, jarOutput);

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
            final DatabaseType database,
            VoltCompilerReader cannonicalDDLIfAny,
            Database previousDBIfAny,
            final List<VoltCompilerReader> ddlReaderList,
            final InMemoryJarfile jarOutput)
                    throws VoltCompilerException
    {
        final ArrayList<Class<?>> classDependencies = new ArrayList<Class<?>>();
        final VoltDDLElementTracker voltDdlTracker = new VoltDDLElementTracker(this);

        Database db = initCatalogDatabase();

        // schemas/schema
        if (database.getSchemas() != null) {
            for (SchemasType.Schema schema : database.getSchemas().getSchema()) {
                compilerLog.l7dlog( Level.INFO, LogKeys.compiler_VoltCompiler_CatalogPath.name(),
                                    new Object[] {schema.getPath()}, null);
                // Prefer to use the in-memory copy.
                // All ddl.sql is placed in the jar root folder.
                File schemaFile = new File(schema.getPath());
                String schemaName = schemaFile.getName();
                if (jarOutput != null && jarOutput.containsKey(schemaName)) {
                    ddlReaderList.add(new VoltCompilerJarFileReader(jarOutput, schemaName));
                }
                else {
                    ddlReaderList.add(createDDLFileReader(schema.getPath()));
                }
            }
        }

        // groups/group (alias for roles/role).
        if (database.getGroups() != null) {
            for (GroupsType.Group group : database.getGroups().getGroup()) {
                org.voltdb.catalog.Group catGroup = db.getGroups().add(group.getName());
                catGroup.setSql(group.isAdhoc());
                catGroup.setSqlread(catGroup.getSql());
                catGroup.setDefaultproc(group.isDefaultproc() || catGroup.getSql());
                catGroup.setDefaultprocread(group.isDefaultprocread() || catGroup.getDefaultproc() || catGroup.getSqlread());

                if (group.isSysproc()) {
                    catGroup.setAdmin(true);
                    catGroup.setSql(true);
                    catGroup.setSqlread(true);
                    catGroup.setDefaultproc(true);
                    catGroup.setDefaultprocread(true);
                }
            }
        }

        // roles/role (alias for groups/group).
        if (database.getRoles() != null) {
            for (RolesType.Role role : database.getRoles().getRole()) {
                org.voltdb.catalog.Group catGroup = db.getGroups().add(role.getName());
                catGroup.setSql(role.isAdhoc());
                catGroup.setSqlread(catGroup.getSql());
                catGroup.setDefaultproc(role.isDefaultproc() || catGroup.getSql());
                catGroup.setDefaultprocread(role.isDefaultprocread() || catGroup.getDefaultproc() || catGroup.getSqlread());

                if (role.isSysproc()) {
                    catGroup.setAdmin(true);
                    catGroup.setSql(true);
                    catGroup.setSqlread(true);
                    catGroup.setDefaultproc(true);
                    catGroup.setDefaultprocread(true);
                }
            }
        }

        // procedures/procedure
        if (database.getProcedures() != null) {
            for (ProceduresType.Procedure proc : database.getProcedures().getProcedure()) {
                voltDdlTracker.add(getProcedure(proc));
            }
        }

        // classdependencies/classdependency
        if (database.getClassdependencies() != null) {
            for (Classdependency dep : database.getClassdependencies().getClassdependency()) {
                classDependencies.add(getClassDependency(dep));
            }
        }

        // partitions/table
        if (database.getPartitions() != null) {
            for (PartitionsType.Partition table : database.getPartitions().getPartition()) {
                voltDdlTracker.addPartition(table.getTable(), table.getColumn());
            }
        }

        // shutdown and make a new hsqldb
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        compileDatabase(db, hsql, voltDdlTracker, cannonicalDDLIfAny, previousDBIfAny, ddlReaderList, database.getExport(), classDependencies,
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
            ExportType export,
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
            // add the file object's path to the list of files for the jar
            m_ddlFilePaths.put(schemaReader.getName(), schemaReader.getPath());

            ddlcompiler.loadSchema(schemaReader, db, whichProcs);
        }

        ddlcompiler.compileToCatalog(db);

        // Actually parse and handle all the partitions
        // this needs to happen before procedures are compiled
        String msg = "In database, ";
        final CatalogMap<Table> tables = db.getTables();
        for (Table table: tables) {
            String tableName = table.getTypeName();
            if (voltDdlTracker.m_partitionMap.containsKey(tableName.toLowerCase())) {
                String colName = voltDdlTracker.m_partitionMap.get(tableName.toLowerCase());
                // A null column name indicates a replicated table. Ignore it here
                // because it defaults to replicated in the catalog.
                if (colName != null) {
                    assert(tables.getIgnoreCase(tableName) != null);
                    final Column partitionCol = table.getColumns().getIgnoreCase(colName);
                    // make sure the column exists
                    if (partitionCol == null) {
                        msg += "PARTITION has unknown COLUMN '" + colName + "'";
                        throw new VoltCompilerException(msg);
                    }
                    // make sure the column is marked not-nullable
                    if (partitionCol.getNullable() == true) {
                        msg += "Partition column '" + tableName + "." + colName + "' is nullable. " +
                            "Partition columns must be constrained \"NOT NULL\".";
                        throw new VoltCompilerException(msg);
                    }
                    // verify that the partition column is a supported type
                    VoltType pcolType = VoltType.get((byte) partitionCol.getType());
                    switch (pcolType) {
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case STRING:
                        case VARBINARY:
                            break;
                        default:
                            msg += "Partition column '" + tableName + "." + colName + "' is not a valid type. " +
                            "Partition columns must be an integer or varchar type.";
                            throw new VoltCompilerException(msg);
                    }

                    table.setPartitioncolumn(partitionCol);
                    table.setIsreplicated(false);

                    // Check valid indexes, whether they contain the partition column or not.
                    for (Index index: table.getIndexes()) {
                        checkValidPartitionTableIndex(index, partitionCol, tableName);
                    }
                    // Set the partitioning of destination tables of associated views.
                    // If a view's source table is replicated, then a full scan of the
                    // associated view is single-sited. If the source is partitioned,
                    // a full scan of the view must be distributed, unless it is filtered
                    // by the original table's partitioning key, which, to be filtered,
                    // must also be a GROUP BY key.
                    final CatalogMap<MaterializedViewInfo> views = table.getViews();
                    for (final MaterializedViewInfo mvi : views) {
                        mvi.getDest().setIsreplicated(false);
                        setGroupedTablePartitionColumn(mvi, partitionCol);
                    }
                }
            }
        }

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

        // Process and add exports and connectors to the catalog
        // Must do this before compiling procedures to deny updates
        // on append-only tables.
        if (export != null) {
            // currently, only a single connector is allowed
            compileExport(export, db);
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
        m_importLines = voltDdlTracker.m_importLines.toArray(new String[0]);
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

    private void checkValidPartitionTableIndex(Index index, Column partitionCol, String tableName)
            throws VoltCompilerException {
        // skip checking for non-unique indexes.
        if (!index.getUnique()) {
            return;
        }

        boolean containsPartitionColumn = false;
        String jsonExpr = index.getExpressionsjson();
        // if this is a pure-column index...
        if (jsonExpr.isEmpty()) {
            for (ColumnRef cref : index.getColumns()) {
                Column col = cref.getColumn();
                // unique index contains partitioned column
                if (col.equals(partitionCol)) {
                    containsPartitionColumn = true;
                    break;
                }
            }
        }
        // if this is a fancy expression-based index...
        else {
            try {
                int partitionColIndex = partitionCol.getIndex();
                List<AbstractExpression> indexExpressions = AbstractExpression.fromJSONArrayString(jsonExpr, null);
                for (AbstractExpression expr: indexExpressions) {
                    if (expr instanceof TupleValueExpression &&
                            ((TupleValueExpression) expr).getColumnIndex() == partitionColIndex ) {
                        containsPartitionColumn = true;
                        break;
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace(); // danger will robinson
                assert(false);
            }
        }

        if (containsPartitionColumn) {
            if (index.getAssumeunique()) {
                String exceptionMsg = String.format("ASSUMEUNIQUE is not valid " +
                "for an index that includes the partitioning column. Please use UNIQUE instead.");
                throw new VoltCompilerException(exceptionMsg);
            }
        }
        else if ( ! index.getAssumeunique()) {
            // Throw compiler exception.
            String indexName = index.getTypeName();
            String keyword = "";
            if (indexName.startsWith(HSQLInterface.AUTO_GEN_PRIMARY_KEY_PREFIX)) {
                indexName = "PRIMARY KEY";
                keyword = "PRIMARY KEY";
            } else {
                indexName = "UNIQUE INDEX " + indexName;
                keyword = "UNIQUE";
            }

            String exceptionMsg = "Invalid use of " + keyword +
                    ". The " + indexName + " on the partitioned table " + tableName +
                    " does not include the partitioning column " + partitionCol.getName() +
                    ". See the documentation for the 'CREATE TABLE' and 'CREATE INDEX' commands and the 'ASSUMEUNIQUE' keyword.";
            throw new VoltCompilerException(exceptionMsg);
        }

    }

    /**
     * Once the DDL file is over, take all of the extra classes found and add them to the jar.
     */
    private void addExtraClasses(final InMemoryJarfile jarOutput) throws VoltCompilerException {

        List<String> addedClasses = new ArrayList<String>();

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
        m_currentFilename = null;

        // allow gc to reclaim any cache memory here
        m_previousCatalogStmts.clear();
    }

    private void setGroupedTablePartitionColumn(MaterializedViewInfo mvi, Column partitionColumn)
            throws VoltCompilerException {
        // A view of a replicated table is replicated.
        // A view of a partitioned table is partitioned -- regardless of whether it has a partition key
        // -- it certainly isn't replicated!
        // If the partitioning column is grouped, its counterpart is the partitioning column of the view table.
        // Otherwise, the view table just doesn't have a partitioning column
        // -- it is seemingly randomly distributed,
        // and its grouped columns are only locally unique but not globally unique.
        Table destTable = mvi.getDest();
        // Get the grouped columns in "index" order.
        // This order corresponds to the iteration order of the MaterializedViewInfo's getGroupbycols.
        List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(destTable.getColumns(), "index");
        String partitionColName = partitionColumn.getTypeName(); // Note getTypeName gets the column name -- go figure.
        int index = 0;
        for (ColumnRef cref : CatalogUtil.getSortedCatalogItems(mvi.getGroupbycols(), "index")) {
            Column srcCol = cref.getColumn();
            if (srcCol.getName().equals(partitionColName)) {
                Column destCol = destColumnArray.get(index);
                destTable.setPartitioncolumn(destCol);
                return;
            }
            ++index;
        }
    }

    /** Provide a feedback path to monitor plan output via harvestCapturedDetail */
    public void enableDetailedCapture() {
        m_capturedDiagnosticDetail = new ArrayList<String>();
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

    ProcedureDescriptor getProcedure(
        org.voltdb.compiler.projectfile.ProceduresType.Procedure xmlproc)
        throws VoltCompilerException
    {
        final ArrayList<String> groups = new ArrayList<String>();

        // @groups
        if (xmlproc.getGroups() != null) {
            for (String group : xmlproc.getGroups().split(",")) {
                groups.add(group);
            }
        }

        // @class
        String classattr = xmlproc.getClazz();

        // If procedure/sql is present, this is a "statement procedure"
        if (xmlproc.getSql() != null) {
            String partattr = xmlproc.getPartitioninfo();
            // null partattr means multi-partition
            // set empty attributes to multi-partition
            if (partattr != null && partattr.length() == 0)
                partattr = null;
            return new ProcedureDescriptor(groups, classattr,
                                           xmlproc.getSql().getValue(),
                                           xmlproc.getSql().getJoinorder(),
                                           partattr, false, null, null, null);
        }
        else {
            String partattr = xmlproc.getPartitioninfo();
            if (partattr != null) {
                String msg = "Java procedures must specify partition info using " +
                "@ProcInfo annotation in the Java class implementation " +
                "and may not use the @partitioninfo project file procedure attribute.";
                throw new VoltCompilerException(msg);
            }
            Class<?> clazz;
            try {
                clazz = Class.forName(classattr, true, m_classLoader);
            }
            catch (ClassNotFoundException e) {
                throw new VoltCompilerException(String.format(
                        "Cannot load class for procedure: %s",
                        classattr));
            }
            catch (Throwable cause) {
                // We are here because the class was found and the initializer of the class
                // threw an error we can't anticipate. So we will wrap the error with a
                // runtime exception that we can trap in our code.
                throw new VoltCompilerException(String.format(
                        "Cannot load class for procedure: %s",
                        classattr), cause);

            }

            return new ProcedureDescriptor(groups, Language.JAVA, null, clazz);
        }
    }


    Class<?> getClassDependency(Classdependency xmlclassdep)
    throws VoltCompilerException
    {
        String msg = "";
        String className = xmlclassdep.getClazz();

        // schema doesn't currently enforce this.. but could I guess.
        if (className.length() == 0) {
            msg += "\"classDependency\" element has empty \"class\" attribute.";
            throw new VoltCompilerException(msg);
        }

        Class<?> cls = null;
        try {
            cls = Class.forName(className, true, m_classLoader);
        } catch (final ClassNotFoundException e) {
            msg += "\"classDependency\" can not find class " + className + " in classpath";
            throw new VoltCompilerException(msg);
        }

        return cls;
    }

    private void compileExport(final ExportType export, final Database catdb)
        throws VoltCompilerException
    {
        // Test the error paths before touching the catalog
        if (export == null) {
            return;
        }

        // This code is used for adding export tables to the default group connector
        if (export.getTables() != null) {
            for (Tables.Table xmltable : export.getTables().getTable()) {
                addExportTableToConnector(Constants.DEFAULT_EXPORT_CONNECTOR_NAME, xmltable.getName(), catdb);
            }
            if (export.getTables().getTable().isEmpty()) {
                compilerLog.warn("Export defined with an empty <tables> element");
            }
        } else {
            compilerLog.warn("Export defined with no <tables> element");
        }
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
        if (CatalogUtil.isTableMaterializeViewSource(catdb, tableref)) {
            compilerLog.error("While configuring export, table " + tableName + " is a source table " +
                    "for a materialized view. Export only tables do not support views.");
            throw new VoltCompilerException("Export table configured with materialized view.");
        }
        if (tableref.getMaterializer() != null)
        {
            compilerLog.error("While configuring export, table " + tableName + " is a " +
                                        "materialized view.  A view cannot be an export table.");
            throw new VoltCompilerException("View configured as an export table");
        }
        if (tableref.getIndexes().size() > 0) {
            compilerLog.error("While configuring export, table " + tableName + " has indexes defined. " +
                    "Export tables can't have indexes (including primary keys).");
            throw new VoltCompilerException("Table with indexes configured as an export table");
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
                try {
                    success = compiler.compileFromDDL(args[0], ArrayUtils.subarray(args, 1, args.length));
                } catch (VoltCompilerException e) {
                    System.err.printf("Compiler exception: %s\n", e.getMessage());
                }
            }
            else {
                System.err.printf("Usage: %s\n", usageNew);
                System.exit(-1);
            }
        }
        else if (args.length > 0 && args[0].toLowerCase().endsWith(".xml")) {
            // The first argument is *.xml for the legacy syntax.
            if (args.length == 2) {
                // warn the user that this is deprecated
                consoleLog.warn("Compiling from a project file is deprecated and will be removed in a future release.");
                success = compiler.compileWithProjectXML(args[0], args[1]);
            }
            else {
                System.err.printf("Usage: %s\n", usageLegacy);
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
            ArrayList<Procedure> nonDetProcs = new ArrayList<Procedure>();
            ArrayList<Procedure> tableScans = new ArrayList<Procedure>();
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
                outputStream.println("Full catalog report can be found at file://" + m_reportPath + "\n" +
                            "\t or can be viewed at \"http://localhost:8080\" when the server is running.\n");
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
    private static final HashSet<Class<?>> cachedAddedClasses = new HashSet<Class<?>>();


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
     *
     */
    public void compileInMemoryJarfileWithNewDDL(InMemoryJarfile jarfile, String newDDL, Catalog oldCatalog) throws IOException
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
            newDDLReader = new VoltCompilerStringReader("ADHOCDDL.sql", newDDL);

            List<VoltCompilerReader> ddlList = new ArrayList<>();
            ddlList.add(newDDLReader);

            m_classLoader = jarfile.getLoader();
            // Do the compilation work.
            InMemoryJarfile jarOut = compileInternal(null, canonicalDDLReader, oldCatalog, ddlList, jarfile);
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

            if (canonicalDDLReader != null) {
                try { canonicalDDLReader.close(); } catch (IOException ioe) {}
            }
            if (newDDLReader != null) {
                try { newDDLReader.close(); } catch (IOException ioe) {}
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
        List<VoltCompilerReader> ddlReaderList = new ArrayList<VoltCompilerReader>();
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
            InMemoryJarfile jarOut = compileInternal(null, null, null, ddlReaderList, jarfile);
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

            // Check if there's a project.
            VoltCompilerReader projectReader =
                    (outputJar.containsKey("project.xml")
                        ? new VoltCompilerJarFileReader(outputJar, "project.xml")
                        : null);

            // Patch the buildinfo.
            String versionFromVoltDB = VoltDB.instance().getVersionString();
            buildInfoLines[0] = versionFromVoltDB;
            buildInfoLines[1] = String.format("voltdb-auto-upgrade-to-%s", versionFromVoltDB);
            byte[] buildInfoBytes = StringUtils.join(buildInfoLines, "\n").getBytes();
            outputJar.put(CatalogUtil.CATALOG_BUILDINFO_FILENAME, buildInfoBytes);

            // Gather DDL files for recompilation if not using a project file.
            List<VoltCompilerReader> ddlReaderList = new ArrayList<VoltCompilerReader>();
            if (projectReader == null) {
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
                    ? new File(catalogContext.cluster.getVoltroot(), jarName).getPath()
                    : VoltDB.Configuration.getPathToCatalogForTest(jarName));
            // Place the compiler output in a text file in the same folder.
            final String outputTextPath = (catalogContext != null
                    ? new File(catalogContext.cluster.getVoltroot(), textName).getPath()
                    : VoltDB.Configuration.getPathToCatalogForTest(textName));
            try {
                m_classLoader = outputJar.getLoader();

                consoleLog.info(String.format(
                        "Version %s catalog will be automatically upgraded to version %s.",
                        versionFromCatalog, versionFromVoltDB));

                // Do the compilation work.
                boolean success = compileInternalToFile(projectReader, outputJarPath, null, null, ddlReaderList, outputJar);

                // Sanitize the *.sql files in the jarfile so that only the autogenerated
                // canonical DDL file will be used for future compilations
                // Bomb out if we failed to generate the canonical DDL
                if (success) {
                    boolean foundCanonicalDDL = false;
                    Entry<String, byte[]> entry = outputJar.firstEntry();
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
