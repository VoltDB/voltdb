/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ProcInfoData;
import org.voltdb.RealVoltDB;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.compiler.projectfile.ClassdependenciesType.Classdependency;
import org.voltdb.compiler.projectfile.DatabaseType;
import org.voltdb.compiler.projectfile.ExportType;
import org.voltdb.compiler.projectfile.ExportType.Tables;
import org.voltdb.compiler.projectfile.GroupsType;
import org.voltdb.compiler.projectfile.InfoType;
import org.voltdb.compiler.projectfile.PartitionsType;
import org.voltdb.compiler.projectfile.ProceduresType;
import org.voltdb.compiler.projectfile.ProjectType;
import org.voltdb.compiler.projectfile.RolesType;
import org.voltdb.compiler.projectfile.SchemasType;
import org.voltdb.compilereport.ReportMaker;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ConstraintType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;
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

    // feedback by filename
    ArrayList<Feedback> m_infos = new ArrayList<Feedback>();
    ArrayList<Feedback> m_warnings = new ArrayList<Feedback>();
    ArrayList<Feedback> m_errors = new ArrayList<Feedback>();

    // set of annotations by procedure name
    private Map<String, ProcInfoData> m_procInfoOverrides = null;

    String m_projectFileURL = null;
    String m_jarOutputPath = null;
    String m_currentFilename = null;
    Map<String, String> m_ddlFilePaths = new HashMap<String, String>();
    String[] m_addedClasses = null;

    // generated html text for catalog report
    String m_report = null;
    String m_reportPath = null;

    InMemoryJarfile m_jarOutput = null;
    Catalog m_catalog = null;

    DatabaseEstimates m_estimates = new DatabaseEstimates();

    private List<String> m_capturedDiagnosticDetail = null;

    private static final VoltLogger compilerLog = new VoltLogger("COMPILER");
    @SuppressWarnings("unused")
    private static final VoltLogger Log = new VoltLogger("org.voltdb.compiler.VoltCompiler");

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

    class ProcedureDescriptor {
        final ArrayList<String> m_authGroups;
        final String m_className;
        // for single-stmt procs
        final String m_singleStmt;
        final String m_joinOrder;
        final String m_partitionString;
        final boolean m_builtInStmt;    // autogenerated sql statement
        final Language m_language;
        final Class<?> m_class;

        ProcedureDescriptor (final ArrayList<String> authGroups, final String className) {
            assert(className != null);

            m_authGroups = authGroups;
            m_className = className;
            m_singleStmt = null;
            m_joinOrder = null;
            m_partitionString = null;
            m_builtInStmt = false;
            m_language = null;
            m_class = null;
        }

        public ProcedureDescriptor(final ArrayList<String> authGroups, final Language language, Class<?> clazz) {
            assert(clazz != null && language != null);

            m_authGroups = authGroups;
            m_className = clazz.getName();
            m_singleStmt = null;
            m_joinOrder = null;
            m_partitionString = null;
            m_builtInStmt = false;
            m_language = language;
            m_class = clazz;
        }

        ProcedureDescriptor(final ArrayList<String> authGroups, final Class<?> clazz, String partitionString, Language language) {
            assert(clazz != null);
            assert(partitionString != null);

            m_authGroups = authGroups;
            m_className = clazz.getName();
            m_singleStmt = null;
            m_joinOrder = null;
            m_partitionString = partitionString;
            m_builtInStmt = false;
            m_language = language;
            m_class = clazz;
        }

        ProcedureDescriptor (final ArrayList<String> authGroups, final String className,
                final String singleStmt, final String joinOrder, final String partitionString,
                boolean builtInStmt, Language language, Class<?> clazz)
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
            m_class = clazz;
        }
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
        compilerLog.info(fb.getLogString());
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
     */
    public boolean compileFromDDL(
            final String jarOutputPath,
            final String... ddlFilePaths)
    {
        return compileInternal(null, jarOutputPath, ddlFilePaths);
    }

    /**
     * Compile using a project.xml file (DEPRECATED).
     *
     * @param projectFileURL URL of the project file.
     * @param jarOutputPath The location to put the finished JAR to.
     * @return true if successful
     */
    public boolean compileWithProjectXML(
            final String projectFileURL,
            final String jarOutputPath)
    {
        return compileInternal(projectFileURL, jarOutputPath, new String[] {});
    }

    /**
     * Internal method for compiling with and without a project.xml file or DDL files.
     *
     * @param projectFileURL URL of the project file or null if not used.
     * @param jarOutputPath The location to put the finished JAR to.
     * @param ddlFilePaths The list of DDL files to compile (when no project is provided).
     * @return true if successful
     */
    private boolean compileInternal(
            final String projectFileURL,
            final String jarOutputPath,
            final String[] ddlFilePaths)
    {
        m_projectFileURL = projectFileURL;
        m_jarOutputPath = jarOutputPath;

        if (m_projectFileURL == null && (ddlFilePaths == null || ddlFilePaths.length == 0)) {
            addErr("One or more DDL files are required.");
            return false;
        }
        if (m_jarOutputPath == null) {
            addErr("The output jar path is null.");
            return false;
        }

        // clear out the warnings and errors
        m_warnings.clear();
        m_infos.clear();
        m_errors.clear();

        // do all the work to get the catalog
        final Catalog catalog = compileCatalog(projectFileURL, ddlFilePaths);
        if (catalog == null) {
            compilerLog.error("Catalog compilation failed.");
            return false;
        }

        // WRITE CATALOG TO JAR HERE
        final String catalogCommands = catalog.serialize();

        byte[] catalogBytes = null;
        try {
            catalogBytes =  catalogCommands.getBytes("UTF-8");
        }
        catch (final UnsupportedEncodingException e1) {
            addErr("Can't encode the compiled catalog file correctly");
            return false;
        }

        StringBuilder buildinfo = new StringBuilder();
        String info[] = RealVoltDB.extractBuildInfo();
        buildinfo.append(info[0]).append('\n');
        buildinfo.append(info[1]).append('\n');
        buildinfo.append(System.getProperty("user.name")).append('\n');
        buildinfo.append(System.getProperty("user.dir")).append('\n');
        buildinfo.append(Long.toString(System.currentTimeMillis())).append('\n');

        try {
            byte buildinfoBytes[] = buildinfo.toString().getBytes("UTF-8");
            m_jarOutput.put("buildinfo.txt", buildinfoBytes);
            m_jarOutput.put("catalog.txt", catalogBytes);
            if (projectFileURL != null) {
                File projectFile = new File(projectFileURL);
                if (projectFile.exists()) {
                    m_jarOutput.put("project.xml", projectFile);
                }
            }
            for (final Entry<String, String> e : m_ddlFilePaths.entrySet())
                m_jarOutput.put(e.getKey(), new File(e.getValue()));
            // put the compiler report into the jarfile
            m_jarOutput.put("catalog-report.html", m_report.getBytes(Constants.UTF8ENCODING));
            m_jarOutput.writeToFile(new File(jarOutputPath)).run();
        }
        catch (final Exception e) {
            e.printStackTrace();
            return false;
        }

        assert(!hasErrors());

        if (hasErrors()) {
            return false;
        }

        return true;
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
                byte[] b = null;
                try {
                    b = s.getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    assert(false);
                }
                retval.put(proc.getTypeName() + "_" + stmt.getTypeName() + ".txt", b);
            }
        }
        return retval;
    }

    public Catalog compileCatalog(final String projectFileURL, final String... ddlFilePaths)
    {
        // Compiler instance is reusable. Clear the cache.
        cachedAddedClasses.clear();
        m_currentFilename = (projectFileURL != null ? new File(projectFileURL).getName() : "null");
        m_jarOutput = new InMemoryJarfile();
        ProjectType project = null;

        if (projectFileURL != null && !projectFileURL.isEmpty()) {
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
                JAXBElement<ProjectType> result = (JAXBElement<ProjectType>) unmarshaller.unmarshal(new File(projectFileURL));
                project = result.getValue();
            }
            catch (JAXBException e) {
                // Convert some linked exceptions to more friendly errors.
                if (e.getLinkedException() instanceof java.io.FileNotFoundException) {
                    addErr(e.getLinkedException().getMessage());
                    compilerLog.error(e.getLinkedException().getMessage());
                    return null;
                }

                DeprecatedProjectElement deprecated = DeprecatedProjectElement.valueOf(e);
                if( deprecated != null) {
                    addErr("Found deprecated XML element \"" + deprecated.name() + "\" in project.xml file, "
                            + deprecated.getSuggestion());
                    addErr("Error schema validating project.xml file. " + e.getLinkedException().getMessage());
                    compilerLog.error("Found deprecated XML element \"" + deprecated.name() + "\" in project.xml file");
                    compilerLog.error(e.getMessage());
                    compilerLog.error(projectFileURL);
                    return null;
                }

                if (e.getLinkedException() instanceof org.xml.sax.SAXParseException) {
                    addErr("Error schema validating project.xml file. " + e.getLinkedException().getMessage());
                    compilerLog.error("Error schema validating project.xml file: " + e.getLinkedException().getMessage());
                    compilerLog.error(e.getMessage());
                    compilerLog.error(projectFileURL);
                    return null;
                }

                throw new RuntimeException(e);
            }
            catch (SAXException e) {
                addErr("Error schema validating project.xml file. " + e.getMessage());
                compilerLog.error("Error schema validating project.xml file. " + e.getMessage());
                return null;
            }
        }
        else {
            // No project.xml - create a stub object.
            project = new ProjectType();
            project.setInfo(new InfoType());
            project.setDatabase(new DatabaseType());
        }

        m_catalog = new Catalog();
        // Initialize the catalog for one cluster
        m_catalog.execute("add / clusters cluster");
        m_catalog.getClusters().get("cluster").setSecurityenabled(false);

        DatabaseType database = project.getDatabase();
        if (database != null) {
            final String databaseName = database.getName();
            // schema does not verify that the database is named "database"
            if (databaseName.equals("database") == false) {
                compilerLog.l7dlog(Level.ERROR,
                                   LogKeys.compiler_VoltCompiler_FailedToCompileXML.name(),
                                   null);
                return null;
            }
            // shutdown and make a new hsqldb
            try {
                compileDatabaseNode(database, ddlFilePaths);
            } catch (final VoltCompilerException e) {
                compilerLog.l7dlog( Level.ERROR, LogKeys.compiler_VoltCompiler_FailedToCompileXML.name(), null);
                return null;
            }
        }
        assert(m_catalog != null);

        // add epoch info to catalog
        final int epoch = (int)(TransactionIdManager.getEpoch() / 1000);
        m_catalog.getClusters().get("cluster").setLocalepoch(epoch);

        // generate the catalog report and write it to disk
        try {
            m_report = ReportMaker.report(m_catalog, m_warnings);
            File file = new File("catalog-report.html");
            FileWriter fw = new FileWriter(file);
            fw.write(m_report);
            fw.close();
            m_reportPath = file.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return m_catalog;
    }

    ProcInfoData getProcInfoOverride(final String procName) {
        if (m_procInfoOverrides == null)
            return null;
        return m_procInfoOverrides.get(procName);
    }

    public Catalog getCatalog() {
        return m_catalog;
    }

    public Database getCatalogDatabase() {
        return m_catalog.getClusters().get("cluster").getDatabases().get("database");
    }

    private Database initCatalogDatabase() {
        // create the database in the catalog
        m_catalog.execute("add /clusters[cluster] databases database");
        return getCatalogDatabase();
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
        final List<String> schemas = new ArrayList<String>(Arrays.asList(ddlFilePaths));
        final VoltDDLElementTracker voltDdlTracker = new VoltDDLElementTracker(this);
        compileDatabase(db, hsql, voltDdlTracker, schemas, null, null, whichProcs);
        return m_catalog;
    }

    /**
     * Legacy interface for loading a ddl file with full support for VoltDB
     * extensions (partitioning, procedures, export),
     * AND full support for input via a project xml file's "database" node.
     * @param database catalog-related info parsed from a project file
     * @param ddlFilePaths schema file paths
     * @throws VoltCompilerException
     */
    void compileDatabaseNode(DatabaseType database, String... ddlFilePaths) throws VoltCompilerException {
        final List<String> schemas = new ArrayList<String>(Arrays.asList(ddlFilePaths));
        final ArrayList<Class<?>> classDependencies = new ArrayList<Class<?>>();
        final VoltDDLElementTracker voltDdlTracker = new VoltDDLElementTracker(this);

        Database db = initCatalogDatabase();

        // schemas/schema
        if (database.getSchemas() != null) {
            for (SchemasType.Schema schema : database.getSchemas().getSchema()) {
                compilerLog.l7dlog( Level.INFO, LogKeys.compiler_VoltCompiler_CatalogPath.name(),
                                    new Object[] {schema.getPath()}, null);
                schemas.add(schema.getPath());
            }
        }

        // groups/group (alias for roles/role).
        if (database.getGroups() != null) {
            for (GroupsType.Group group : database.getGroups().getGroup()) {
                org.voltdb.catalog.Group catGroup = db.getGroups().add(group.getName());
                catGroup.setAdhoc(group.isAdhoc());
                catGroup.setSysproc(group.isSysproc());
                catGroup.setDefaultproc(group.isDefaultproc());
            }
        }

        // roles/role (alias for groups/group).
        if (database.getRoles() != null) {
            for (RolesType.Role role : database.getRoles().getRole()) {
                org.voltdb.catalog.Group catGroup = db.getGroups().add(role.getName());
                catGroup.setAdhoc(role.isAdhoc());
                catGroup.setSysproc(role.isSysproc());
                catGroup.setDefaultproc(role.isDefaultproc());
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
                voltDdlTracker.put(table.getTable(), table.getColumn());
            }
        }

        // shutdown and make a new hsqldb
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        compileDatabase(db, hsql, voltDdlTracker, schemas, database.getExport(), classDependencies,
                        DdlProceduresToLoad.ALL_DDL_PROCEDURES);
    }

    /**
     * Common code for schema loading shared by loadSchema and compileDatabaseNode
     * @param db the database entry in the catalog
     * @param hsql an interface to the hsql frontend, initialized and potentially reused by the caller.
     * @param voltDdlTracker non-standard VoltDB schema annotations, initially those from a project file
     * @param schemas the ddl input files
     * @param export optional export connector configuration (from the project file)
     * @param classDependencies optional additional jar files required by procedures
     * @param whichProcs indicates which ddl-defined procedures to load: none, single-statement, or all
     */
    private void compileDatabase(Database db, HSQLInterface hsql,
                                 VoltDDLElementTracker voltDdlTracker, List<String> schemas,
                                 ExportType export, Collection<Class<?>> classDependencies,
                                 DdlProceduresToLoad whichProcs)
        throws VoltCompilerException
    {
        // Actually parse and handle all the DDL
        // DDLCompiler also provides partition descriptors for DDL PARTITION
        // and REPLICATE statements.
        final DDLCompiler ddlcompiler = new DDLCompiler(this, hsql, voltDdlTracker);

        for (final String schemaPath : schemas) {
            File schemaFile = null;

            if (schemaPath.contains(".jar!")) {
                String ddlText = null;
                try {
                    ddlText = readFileFromJarfile(schemaPath);
                } catch (final Exception e) {
                    throw new VoltCompilerException(e);
                }
                schemaFile = VoltProjectBuilder.writeStringToTempFile(ddlText);
            }
            else {
                schemaFile = new File(schemaPath);
            }

            if (!schemaFile.isAbsolute()) {
                // Resolve schemaPath relative to either the database definition xml file
                // or the working directory.
                if (m_projectFileURL != null) {
                    schemaFile = new File(new File(m_projectFileURL).getParent(), schemaPath);
                }
                else {
                    schemaFile = new File(schemaPath);
                }
            }

            // add the file object's path to the list of files for the jar
            m_ddlFilePaths.put(schemaFile.getName(), schemaFile.getPath());

            ddlcompiler.loadSchema(schemaFile.getAbsolutePath(), db, whichProcs);
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
            } else {
                // Replicated tables case.
                for (Index index: table.getIndexes()) {
                    if (index.getAssumeunique()) {
                        String exceptionMsg = String.format(
                                "ASSUMEUNIQUE is not valid for replicated tables. Please use UNIQUE instead");
                        throw new VoltCompilerException(exceptionMsg);
                    }
                }

            }
        }

        // add database estimates info
        addDatabaseEstimatesInfo(m_estimates, db);

        // Process DDL exported tables
        for( String exportedTableName: voltDdlTracker.getExportedTables()) {
            addExportTableToConnector(exportedTableName, db);
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
            compileProcedures(db, hsql, allProcs, classDependencies, whichProcs);
        }

        // add extra classes from the DDL
        m_addedClasses = voltDdlTracker.m_extraClassses;
        addExtraClasses();
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
            if (indexName.startsWith("SYS_IDX_PK_") || indexName.startsWith("SYS_IDX_SYS_PK_") ) {
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
    private void addExtraClasses() throws VoltCompilerException {

        List<String> addedClasses = new ArrayList<String>();

        for (String className : m_addedClasses) {
            try {
                Class<?> clz = Class.forName(className);

                if (addClassToJar(clz)) {
                    addedClasses.add(className);
                }

            } catch (Exception e) {
                String msg = "Class %s could not be loaded/found/added to the jar. " +
                             "";
                msg = String.format(msg, className);
                throw new VoltCompilerException(msg);
            }

            // reset the added classes to the actual added classes
            m_addedClasses = addedClasses.toArray(new String[0]);
        }
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
                                   DdlProceduresToLoad whichProcs) throws VoltCompilerException
    {
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
                addClassToJar(classDependency);
            }
        }
        // Generate the auto-CRUD procedure descriptors. This creates
        // procedure descriptors to insert, delete, select and update
        // tables, with some caveats. (See ENG-1601).
        final List<ProcedureDescriptor> procedures = generateCrud();

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
            ProcedureCompiler.compile(this, hsql, m_estimates, m_catalog, db, procedureDescriptor);
        }
        // done handling files
        m_currentFilename = null;
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

    /**
     * Create INSERT, UPDATE, DELETE and SELECT procedure descriptors for all partitioned,
     * non-export tables with primary keys that include the partitioning column.
     *
     * @param catalog
     * @return a list of new procedure descriptors
     */
    private List<ProcedureDescriptor> generateCrud() {
        final LinkedList<ProcedureDescriptor> crudprocs = new LinkedList<ProcedureDescriptor>();

        final Database db = getCatalogDatabase();
        for (Table table : db.getTables()) {
            if (CatalogUtil.isTableExportOnly(db, table)) {
                compilerLog.debug("Skipping creation of CRUD procedures for export-only table " +
                        table.getTypeName());
                continue;
            }

            if (table.getMaterializer() != null) {
                compilerLog.debug("Skipping creation of CRUD procedures for view " +
                        table.getTypeName());
                continue;
            }

            // select/delete/update crud requires pkey. Pkeys are stored as constraints.
            final CatalogMap<Constraint> constraints = table.getConstraints();
            final Iterator<Constraint> it = constraints.iterator();
            Constraint pkey = null;
            while (it.hasNext()) {
                Constraint constraint = it.next();
                if (constraint.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                    pkey = constraint;
                    break;
                }
            }

            if (table.getIsreplicated()) {
                if (pkey != null) {
                    compilerLog.debug("Creating multi-partition insert/delete/update procedures for replicated table " +
                            table.getTypeName());
                    crudprocs.add(generateCrudReplicatedInsert(table));
                    crudprocs.add(generateCrudReplicatedDelete(table, pkey));
                    crudprocs.add(generateCrudReplicatedUpdate(table, pkey));
                }
                else {
                    compilerLog.debug("Creating multi-partition insert procedures for replicated table " +
                            table.getTypeName());
                    crudprocs.add(generateCrudReplicatedInsert(table));
                }
                continue;
            }

            // get the partition column
            final Column partitioncolumn = table.getPartitioncolumn();

            // all partitioned tables get insert crud procs
            crudprocs.add(generateCrudInsert(table, partitioncolumn));

            if (pkey == null) {
                compilerLog.debug("Skipping creation of CRUD select/delete/update for partitioned table " +
                        table.getTypeName() + " because no primary key is declared.");
                continue;
            }

            // Primary key must include the partition column for the table
            // for select/delete/update
            boolean pkeyHasPartitionColumn = false;
            CatalogMap<ColumnRef> pkeycols = pkey.getIndex().getColumns();
            Iterator<ColumnRef> pkeycolsit = pkeycols.iterator();
            while (pkeycolsit.hasNext()) {
                ColumnRef colref = pkeycolsit.next();
                if (colref.getColumn().equals(partitioncolumn)) {
                    pkeyHasPartitionColumn = true;
                    break;
                }
            }

            if (!pkeyHasPartitionColumn) {
                compilerLog.debug("Skipping creation of CRUD select/delete/update for partitioned table " +
                        table.getTypeName() + " because primary key does not include the partitioning column.");
                continue;
            }

            // select, delete and updarte here (insert generated above)
            crudprocs.add(generateCrudSelect(table, partitioncolumn, pkey));
            crudprocs.add(generateCrudDelete(table, partitioncolumn, pkey));
            crudprocs.add(generateCrudUpdate(table, partitioncolumn, pkey));
        }

        return crudprocs;
    }

    /** Helper to sort table columns by table column order */
    private static class TableColumnComparator implements Comparator<Column> {
        public TableColumnComparator() {
        }

        @Override
        public int compare(Column o1, Column o2) {
            return o1.getIndex() - o2.getIndex();
        }
    }

    /** Helper to sort index columnrefs by index column order */
    private static class ColumnRefComparator implements Comparator<ColumnRef> {
        public ColumnRefComparator() {
        }

        @Override
        public int compare(ColumnRef o1, ColumnRef o2) {
            return o1.getIndex() - o2.getIndex();
        }
    }

    /**
     * Helper to generate a WHERE pkey_col1 = ?, pkey_col2 = ? ...; clause.
     * @param partitioncolumn partitioning column for the table
     * @param pkey constraint from the catalog
     * @param paramoffset 0-based counter of parameters in the full sql statement so far
     * @param sb string buffer accumulating the sql statement
     * @return offset in the index of the partition column
     */
    private int generateCrudPKeyWhereClause(Column partitioncolumn,
            Constraint pkey, StringBuilder sb)
    {
        // Sort the catalog index columns by index column order.
        ArrayList<ColumnRef> indexColumns = new ArrayList<ColumnRef>(pkey.getIndex().getColumns().size());
        for (ColumnRef c : pkey.getIndex().getColumns()) {
            indexColumns.add(c);
        }
        Collections.sort(indexColumns, new ColumnRefComparator());

        boolean first = true;
        int partitionOffset = -1;

        sb.append(" WHERE ");
        for (ColumnRef pkc : indexColumns) {
            if (!first) sb.append(" AND ");
            first = false;
            sb.append("(" + pkc.getColumn().getName() + " = ?" + ")");
            if (pkc.getColumn() == partitioncolumn) {
                partitionOffset = pkc.getIndex();
            }
        }
        sb.append(";");
        return partitionOffset;

    }

    /**
     * Helper to generate a full col1 = ?, col2 = ?... clause.
     * @param table
     * @param sb
     */
    private void generateCrudExpressionColumns(Table table, StringBuilder sb) {
        boolean first = true;

        // Sort the catalog table columns by column order.
        ArrayList<Column> tableColumns = new ArrayList<Column>(table.getColumns().size());
        for (Column c : table.getColumns()) {
            tableColumns.add(c);
        }
        Collections.sort(tableColumns, new TableColumnComparator());

        for (Column c : tableColumns) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(c.getName() + " = ?");
        }
    }

    /**
     * Helper to generate a full col1, col2, col3 list.
     */
    private void generateCrudColumnList(Table table, StringBuilder sb) {
        boolean first = true;
        sb.append("(");

        // Sort the catalog table columns by column order.
        ArrayList<Column> tableColumns = new ArrayList<Column>(table.getColumns().size());
        for (Column c : table.getColumns()) {
            tableColumns.add(c);
        }
        Collections.sort(tableColumns, new TableColumnComparator());

        // Output the SQL column list.
        for (Column c : tableColumns) {
            assert (c.getIndex() >= 0);  // mostly mask unused 'c'.
            if (!first) sb.append(", ");
            first = false;
            sb.append("?");
        }
        sb.append(")");
    }

    /**
     * Create a statement like:
     *  "delete from <table> where {<pkey-column =?>...}"
     */
    private ProcedureDescriptor generateCrudDelete(Table table,
            Column partitioncolumn, Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM " + table.getTypeName());

        int partitionOffset =
            generateCrudPKeyWhereClause(partitioncolumn, pkey, sb);

        String partitioninfo =
            table.getTypeName() + "." + partitioncolumn.getName() + ":" + partitionOffset;

        ProcedureDescriptor pd =
            new ProcedureDescriptor(
                    new ArrayList<String>(),  // groups
                    table.getTypeName() + ".delete",        // className
                    sb.toString(),            // singleStmt
                    null,                     // joinOrder
                    partitioninfo,            // table.column:offset
                    true,                     // builtin statement
                    null,                     // language type for embedded scripts
                    null);                    // code block script class

        return pd;
    }

    /**
     * Create a statement like:
     * "update <table> set {<each-column = ?>...} where {<pkey-column = ?>...}
     */
    private ProcedureDescriptor generateCrudUpdate(Table table,
            Column partitioncolumn, Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE " + table.getTypeName() + " SET ");

        generateCrudExpressionColumns(table, sb);
        generateCrudPKeyWhereClause(partitioncolumn, pkey, sb);

        String partitioninfo =
            table.getTypeName() + "." + partitioncolumn.getName() + ":" + partitioncolumn.getIndex();

        ProcedureDescriptor pd =
            new ProcedureDescriptor(
                    new ArrayList<String>(),  // groups
                    table.getTypeName() + ".update",        // className
                    sb.toString(),            // singleStmt
                    null,                     // joinOrder
                    partitioninfo,            // table.column:offset
                    true,                     // builtin statement
                    null,                     // language type for embedded scripts
                    null);                    // code block script class

        return pd;
    }

    /**
     * Create a statement like:
     *  "insert into <table> values (?, ?, ...);"
     */
    private ProcedureDescriptor generateCrudInsert(Table table,
            Column partitioncolumn)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + table.getTypeName() + " VALUES ");

        generateCrudColumnList(table, sb);
        sb.append(";");

        String partitioninfo =
            table.getTypeName() + "." + partitioncolumn.getName() + ":" + partitioncolumn.getIndex();

        ProcedureDescriptor pd =
            new ProcedureDescriptor(
                    new ArrayList<String>(),  // groups
                    table.getTypeName() + ".insert",        // className
                    sb.toString(),            // singleStmt
                    null,                     // joinOrder
                    partitioninfo,            // table.column:offset
                    true,                     // builtin statement
                    null,                     // language type for embedded scripts
                    null);                    // code block script class

        return pd;
    }

    /**
     * Create a statement like:
     *  "insert into <table> values (?, ?, ...);"
     *  for a replicated table.
     */
    private ProcedureDescriptor generateCrudReplicatedInsert(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + table.getTypeName() + " VALUES ");

        generateCrudColumnList(table, sb);
        sb.append(";");

        ProcedureDescriptor pd =
            new ProcedureDescriptor(
                    new ArrayList<String>(),  // groups
                    table.getTypeName() + ".insert",        // className
                    sb.toString(),            // singleStmt
                    null,                     // joinOrder
                    null,                     // table.column:offset
                    true,                     // builtin statement
                    null,                     // language type for embedded scripts
                    null);                    // code block script class

        return pd;
    }

    /**
     * Create a statement like:
     *  "update <table> set {<each-column = ?>...} where {<pkey-column = ?>...}
     *  for a replicated table.
     */
    private ProcedureDescriptor generateCrudReplicatedUpdate(Table table,
            Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE " + table.getTypeName() + " SET ");

        generateCrudExpressionColumns(table, sb);
        generateCrudPKeyWhereClause(null, pkey, sb);

        ProcedureDescriptor pd =
            new ProcedureDescriptor(
                    new ArrayList<String>(),  // groups
                    table.getTypeName() + ".update",        // className
                    sb.toString(),            // singleStmt
                    null,                     // joinOrder
                    null,                     // table.column:offset
                    true,                     // builtin statement
                    null,                     // language type for embedded scripts
                    null);                    // code block script class

        return pd;
    }

    /**
     * Create a statement like:
     *  "delete from <table> where {<pkey-column =?>...}"
     * for a replicated table.
     */
    private ProcedureDescriptor generateCrudReplicatedDelete(Table table,
            Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM " + table.getTypeName());

        generateCrudPKeyWhereClause(null, pkey, sb);

        ProcedureDescriptor pd =
            new ProcedureDescriptor(
                    new ArrayList<String>(),  // groups
                    table.getTypeName() + ".delete",        // className
                    sb.toString(),            // singleStmt
                    null,                     // joinOrder
                    null,                     // table.column:offset
                    true,                     // builtin statement
                    null,                     // language type for embedded scripts
                    null);                    // code block script class

        return pd;
    }

    /**
     * Create a statement like:
     *  "select * from <table> where pkey_col1 = ?, pkey_col2 = ? ... ;"
     */
    private ProcedureDescriptor generateCrudSelect(Table table,
            Column partitioncolumn, Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM " + table.getTypeName());

        int partitionOffset =
            generateCrudPKeyWhereClause(partitioncolumn, pkey, sb);

        String partitioninfo =
            table.getTypeName() + "." + partitioncolumn.getName() + ":" + partitionOffset;

        ProcedureDescriptor pd =
            new ProcedureDescriptor(
                    new ArrayList<String>(),  // groups
                    table.getTypeName() + ".select",        // className
                    sb.toString(),            // singleStmt
                    null,                     // joinOrder
                    partitioninfo,            // table.column:offset
                    true,                     // builtin statement
                    null,                     // language type for embedded scripts
                    null);                    // code block script class

        return pd;
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
                                           partattr, false, null, null);
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
                clazz = Class.forName(classattr);
            } catch (ClassNotFoundException e) {
                throw new VoltCompilerException(String.format(
                        "Cannot load class for procedure: %s",
                        classattr));
            }

            return new ProcedureDescriptor(groups, Language.JAVA, clazz);
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
            cls = Class.forName(className);
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

        // Catalog Connector
        // Relying on schema's enforcement of at most 1 connector
        //
        // This check is also done here to mimic the same behavior of the
        // previous implementation of this method, where the connector is created as
        // long as the export element is present in project XML. Now that we are
        // deprecating project.xml, we won't be able to mimic in DDL, what an
        // empty <export/> element currently implies.
        org.voltdb.catalog.Connector catconn = catdb.getConnectors().getIgnoreCase("0");
        if (catconn == null) {
            catconn = catdb.getConnectors().add("0");
        }

        // Catalog Connector.ConnectorTableInfo
        if (export.getTables() != null) {
            for (Tables.Table xmltable : export.getTables().getTable()) {
                addExportTableToConnector(xmltable.getName(), catdb);
            }
            if (export.getTables().getTable().isEmpty()) {
                compilerLog.warn("Export defined with an empty <tables> element");
            }
        } else {
            compilerLog.warn("Export defined with no <tables> element");
        }
    }

    void addExportTableToConnector( final String tableName, final Database catdb)
            throws VoltCompilerException
    {
        assert tableName != null && ! tableName.trim().isEmpty() && catdb != null;

        // Catalog Connector
        // Relying on schema's enforcement of at most 1 connector
        org.voltdb.catalog.Connector catconn = catdb.getConnectors().getIgnoreCase("0");
        if (catconn == null) {
            catconn = catdb.getConnectors().add("0");
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
        final VoltCompiler compiler = new VoltCompiler();
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
        else if (args.length > 0 && args[0].toLowerCase().endsWith(".xml")) {
            // The first argument is *.xml for the legacy syntax.
            if (args.length == 2) {
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

        // Exit with error code if we failed
        if (!success) {
            compiler.summarizeErrors(System.out, null);
            System.exit(-1);
        }
        compiler.summarizeSuccess(System.out, null);
    }

    public void summarizeSuccess(PrintStream outputStream, PrintStream feedbackStream) {
        if (outputStream != null) {

            Database database = getCatalogDatabase();

            outputStream.println("------------------------------------------");
            outputStream.println("Successfully created " + m_jarOutputPath);

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
                    "\tSimple insert, update, delete and select procedures are created\n" +
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
            outputStream.println("------------------------------------------\n");
            outputStream.println("Full catalog report can be found at file://" + m_reportPath + "\n" +
                        "\t or can be viewed at \"http://localhost:8080\" when the server is running.\n");
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
            outputStream.println("Project compilation failed. See log for errors.");
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

    private byte[] getClassAsBytes(final Class<?> c) throws IOException {

        ClassLoader cl = c.getClassLoader();
        if (cl == null) {
            cl = Thread.currentThread().getContextClassLoader();
        }
        String classAsPath = c.getName().replace('.', '/') + ".class";

        BufferedInputStream   cis = null;
        ByteArrayOutputStream baos = null;
        try {
            cis  = new BufferedInputStream(cl.getResourceAsStream(classAsPath));
            baos =  new ByteArrayOutputStream();

            byte [] buf = new byte[1024];

            int rsize = 0;
            while ((rsize=cis.read(buf)) != -1) {
                baos.write(buf, 0, rsize);
            }

        } finally {
            try { if (cis != null)  cis.close();}   catch (Exception ignoreIt) {}
            try { if (baos != null) baos.close();}  catch (Exception ignoreIt) {}
        }

        return baos.toByteArray();

    }


    public List<Class<?>> getInnerClasses(Class <?> c)
            throws VoltCompilerException {
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        ClassLoader cl = c.getClassLoader();
        if (cl == null) {
            cl = Thread.currentThread().getContextClassLoader();
        }

        String stem = c.getName().replace('.', '/');
        URL curl = cl.getResource(stem+".class");

        if ("jar".equals(curl.getProtocol())) {
            Pattern nameRE = Pattern.compile("\\A(" + stem + "\\$[^/]+).class\\z");
            String jarFN;
            try {
                jarFN = URLDecoder.decode(curl.getFile(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
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
            } catch (IOException e) {
                String msg = "Cannot access class " + c + " source code location of " + jarFN;
                throw new VoltCompilerException(msg);
            } finally {
                if ( jar != null) try {jar.close();} catch (Exception ignoreIt) {};
            }
        } else if ("file".equals(curl.getProtocol())) {
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
        return builder.build();
    }

    public boolean addClassToJar(final Class<?> cls)
    throws VoltCompiler.VoltCompilerException {

        if (cachedAddedClasses.contains(cls)) {
            return false;
        } else {
            cachedAddedClasses.add(cls);
        }

        for (final Class<?> nested : getInnerClasses(cls)) {
            addClassToJar(nested);
        }

        String packagePath = cls.getName();
        packagePath = packagePath.replace('.', '/');
        packagePath += ".class";

        String realName = cls.getName();
        realName = realName.substring(realName.lastIndexOf('.') + 1);
        realName += ".class";

        byte [] classBytes = null;
        try {
            classBytes = getClassAsBytes(cls);
        } catch (Exception e) {
            final String msg = "Unable to locate classfile for " + realName;
            throw new VoltCompilerException(msg);
        }

        m_jarOutput.put(packagePath, classBytes);
        return true;
    }

    /**
     * Read a file from a jar in the form path/to/jar.jar!/path/to/file.ext
     */
    static String readFileFromJarfile(String fulljarpath) throws IOException {
        assert (fulljarpath.contains(".jar!"));

        String[] paths = fulljarpath.split("!");
        if (paths[0].startsWith("file:"))
            paths[0] = paths[0].substring("file:".length());
        paths[1] = paths[1].substring(1);

        return readFileFromJarfile(paths[0], paths[1]);
    }

    static String readFileFromJarfile(String jarfilePath, String entryPath) throws IOException {
        InputStream fin = null;
        try {
            URL jar_url = new URL(jarfilePath);
            fin = jar_url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            fin = new FileInputStream(jarfilePath);
        }
        JarInputStream jarIn = new JarInputStream(fin);

        JarEntry catEntry = jarIn.getNextJarEntry();
        while ((catEntry != null) && (catEntry.getName().equals(entryPath) == false)) {
            catEntry = jarIn.getNextJarEntry();
        }
        if (catEntry == null) {
            jarIn.close();
            return null;
        }

        byte[] bytes = InMemoryJarfile.readFromJarEntry(jarIn, catEntry);

        return new String(bytes, "UTF-8");
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
}
