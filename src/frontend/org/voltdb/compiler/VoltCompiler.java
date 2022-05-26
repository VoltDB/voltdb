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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.voltcore.TransactionIdManager;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.RealVoltDB;
import org.voltdb.SQLStmt;
import org.voltdb.TableType;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltNonTransactionalProcedure;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.FilteredCatalogDiffEngine;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.common.Permission;
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.compilereport.ReportMaker;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.parser.SQLParser;
import org.voltdb.planner.ParameterizationInfo;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannerv2.utils.CreateTableUtils;
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

/**
 * Compiles a project XML file and some metadata into a Jar file
 * containing stored procedure code and a serialized catalog.
 *
 * The compiling algorithm is somewhat confusing.  We use a combination of HSQLDB, java regular expressions,
 * stone knives and bear skins to parse SQL into an internal form we can use.  This internal form is mostly
 * a VoltXMLElement object.  However, the result can also be a Catalog, if we are called upon to compile
 * DDL.  There is some other, static state which must be made correct as well.
 *
 * SQL statements are either DML, DDL, or DQL.  The DML statements are insert and delete.  The DQL
 * statements are either select or set operations applied to select statements.  Neither of these,
 * DML and DQL statements, changes the catalog.  They don't define new tables or indexes, and they
 * don't change table representations.  The DDL statements make all the catalog changes.  These
 * DDL commands include create table, create index, create view, create function, drop table,
 * drop index, drop view, partition table, alter table, and perhaps some others.  Some are standard
 * SQL commands, and can be processed by HSQL, perhaps with some massaging of the text.  Others are
 * completely VoltDB syntax, and HSQL knows nothing about them.
 *
 * <h3>DML and DQL</h3>
 * When we compile a DML or DQL statement it's in the context of a catalog and set of user
 * defined function definitions.  The user defined function definitions are stored in static
 * data in the HSQL compiler.  They must be correct.
 * <strong>Note:</strong> This should really be stored in the HSQL session object, along with the database.
 *
 * <h3>DDL</h3>
 * When we compile a DDL statement it always affects a single table.  There may also be other
 * artifacts as well.  For example, a create index command affects a single table, but it
 * creates an index, which is another artifact internally.
 *
 * The input to the compiler for DDL is a catalog jar file and a DDL string.  The DDL string could be a
 * sequence of DDL statements.  The result should be a catalog object with the new DDL added.  However,
 * there is also a static set of function definitions maintained in the compiler.  This static set of
 * function definitions needs to reflect the new catalog as well.  <strong>Note:</strong> This is very
 * fragile.  We should really fix this.
 *
 * <ol>
 *   <li>We first start with an empty catalog.  From the catalog jar file we extract a context.  This
 *       is a catalog object and the DDL string used to create the catalog object.  We call this DDL
 *       string the <em>Canonical DDL.</em> </li>
 *   <li>We process the canonical DDL string.
 *       <ol>
 *         <li>The canonical DDL string is broken up into individual statements.</li>
 *         <li>Each statement is pre-processed to find out what table or index it creates, and, for
 *             indexes, what table the index is on.</li>
 *         <li>If a statement is one which we can process by matching regular expressions (see S.K. & B.S. above)
 *             we extract substrings and process the statement in the front end, without calling HSQL.  So,
 *             HSQLDB doesn't know anything about these kind of VoltDB statements.</li>
 *         <li>If a statement is not one that VoltDB knows how to process, we send it to HSQL.  This creates a table
 *             or an index internally, in HSQL's symbol table.  We have built into to HSQL the ability to
 *             query for the VoltXML of a table or index.  So we can extract VoltXML from HSQL for
 *             these statements.</li>
 *         <li>Note that in this stage we are just processing canonical DDL.</li>
 *         <li>Also note that we need the VoltXML because we may mix VoltDB processing and HSQL processing
 *             for a single table.  Consider the strings:
 *             <pre>
 *               {@code
 *               create table aaa ( id integer );
 *               partition table aaa on column id;
 *               create index aaaidx on aaa ( id + id );}
 *             </pre>
 *             In this case we need to create the table aaa, partition it and create an index.  The index will
 *             be a child of the table's VoltXML.  This mixes partition information and index definitioning
 *             both in the same VoltXML definition for the same table.  So, when we create the index we process
 *             it with HSQL, fetch out the VoltXML for the new table, calculate the difference between the existing
 *             table VoltXML and add the new elements.</li>
 *         <li>Note also that {@code CREATE PROCEDURE} is a DDL statement.  But we don't process it here.  We
 *             just buffer it up here in a tracker, along with some other information we track.</li>
 *         <li>Note that user defined functions are only called in stored procedures and in DML and DQL.  Since we don't
 *             care about DQL and DML here, as we are discussing DDL, we only care about stored procedures.  These
 *             have been buffered up in the tracker, and will not be compiled here.  So it doesn't really
 *             matter what order user defined functions are processed, or if HSQL knows about them.  So
 *             we just add them to the VoltXML and nowhere else.</li>
 *       </ol>
 *       The result of this processing is not a new catalog, but a new VoltXML object.  We can't just reuse
 *       the old catalog because it has the form of a set of commands for the EE, and we need the VoltXML tree
 *       to do the VoltXML differencing discussed above.  Note that procedures in the canonical DDL
 *       still have not been compiled to the catalog.  They are in the tracker, so the contents of the
 *       tracker is, perhaps, a result of this processing as well.  Since these are for the existing
 *       catalog, all function signatures, including function ids, should match the existing catalog's
 *       definition exactly.</li>
 *   </li>
 *   <li>After all the canonical DDL has been compiled to XML, we process the new DDL in the same way as the
 *       canonical DDL. But this processing is done in the context of the canonical DDL.  Since we will just
 *       buffer up stored procedures here, and not compile them, the order that user defined functions are
 *       stored in the VoltXML does not matter here either, so we just add them to the VoltXML.
 *       <ol>
 *         <li>We can check the VoltXML to see if a user defined function is doubly defined.</li>
 *         <li>We can just drop the functions in the VoltXML if they are dropped in the DDL.  We
 *             don't have to alter the compiler's function table here.  But see below to tell how
 *             we keep these definitions transactional.</li>
 *       </ol>
 *   <li>After all the DDL has been processed, we compile the VoltXML to a proper catalog object.  This is
 *       the internal catalog object, not the catalog command string we send to the EE.  We will need to add
 *       the stored procedures.</li>
 *   <li>Before we can add the stored procedures we need to make sure HSQL knows about the user defined functions.
 *       These definitions are in a static table in FunctionForVoltDB.FunctionDescriptor.  We first disable all user defined
 *       functions from the static table.  They are not deleted, they are just set to the side.  We then traverse the
 *       VoltXML for the new catalog and define the user defined functions in the static table.  We now have the
 *       old function definitions stored away and the new function definitions active.</br>
 *       <strong>Note:</strong> Keeping this static data definition correct causes no end of problem for us, and really should be
 *       fixed some day.</li>
 *   <li>We then process the stored procedures, which are stored in the tracker.  These compilations can either
 *       succeed or fail.  If they fail they throw a VoltCompilerException, which we can catch.
 *       <ol>
 *         <li>If the stored procedure compilation succeeds, we discard all the old user defined function
 *             definitions, and commit to the new set in the static FunctionId table.</li>
 *         <li>If the stored procedure compilation fails, we delete all the new user defined function definitions
 *             and restore the old ones.
 *       </ol>
 *   </li>
 * </ol>
 */
public class VoltCompiler {
    /** Represents the level of severity for a Feedback message generated during compiling. */
    public static enum Severity { INFORMATIONAL, WARNING, ERROR, UNEXPECTED }
    public static final int NO_LINE_NUMBER = -1;
    private static final String NO_FILENAME = "null";

    // Causes the "debugoutput" folder to be generated and populated.
    // Also causes explain plans on disk to include cost.
    public final static boolean DEBUG_MODE
      = Boolean.parseBoolean(System.getProperty("org.voltdb.compilerdebug", "false"));

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

    // Name of DDL file built by the DDL VoltCompiler from the catalog and added to the jar.
    public static final String AUTOGEN_DDL_FILE_NAME = "autogen-ddl.sql";
    public static final String CATLOG_REPORT = "catalog-report.html";
    // Environment variable used to verify that a catalog created from autogen-dll.sql is effectively
    // identical to the original catalog that was used to create the autogen-ddl.sql file.
    public static final boolean DEBUG_VERIFY_CATALOG = Boolean.parseBoolean(System.getenv().get("VERIFY_CATALOG_DEBUG"));

    /// Set this to true to automatically retry a failed attempt to round-trip
    /// a rebuild of a catalog from its canonical ddl. This gives a chance to
    /// set breakpoints and step through a do-over of only the flawed catalogs.
    public static boolean RETRY_FAILED_CATALOG_REBUILD_UNDER_DEBUG = false;

    String m_projectFileURL = null;
    private String m_currentFilename = NO_FILENAME;
    Map<String, String> m_ddlFilePaths = new HashMap<>();
    String[] m_addedClasses = null;

    // generated html text for catalog report
    String m_reportPath = null;
    static String m_canonicalDDL = null;
    Catalog m_catalog = null;

    DatabaseEstimates m_estimates = new DatabaseEstimates();

    private List<String> m_capturedDiagnosticDetail = null;

    private static VoltLogger compilerLog = new VoltLogger("COMPILER");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");
    private static final VoltLogger Log = new VoltLogger("org.voltdb.compiler.VoltCompiler");

    private final static String m_emptyDDLComment = "-- This DDL file is a placeholder for starting without a user-supplied catalog.\n";

    private ClassLoader m_classLoader = ClassLoader.getSystemClassLoader();

    // this needs to be reset in the main compile func
    private final HashSet<Class<?>> m_cachedAddedClasses = new HashSet<>();

    private final boolean m_isXDCR;

    private final String m_user;

    // Whether or not to use SQLCommand as a pre-processor for DDL (in voltdb init --classes). Default is false.
    private boolean m_filterWithSQLCommand = false;

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
            final String retval;
            switch (severityLevel) {
                case INFORMATIONAL:
                    retval = "INFO";
                    break;
                case WARNING:
                    retval = "WARNING";
                    break;
                case ERROR:
                    retval = "ERROR";
                    break;
                case UNEXPECTED:
                    retval = "UNEXPECTED ERROR";
                    break;
                default:
                    retval = "";
            }
            return retval + " " + getLogString();
        }

        public String getLogString() {
            String retval = "";
            if (! fileName.equals(NO_FILENAME)) {
                retval += "[" + fileName;
                if (lineNo != NO_LINE_NUMBER) {
                    retval += ":" + lineNo;
                }
                retval += "]: ";
            }
            retval += message;
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

    public class VoltCompilerException extends Exception {
        private static final long serialVersionUID = -2267780579911448600L;
        private String message = null;

        VoltCompilerException(final Exception e) {
            super(e);
        }

        VoltCompilerException(final String message, final int lineNo) {
            addErr(message, lineNo);
            this.message = message;
        }

        public VoltCompilerException(final String message) {
            addErr(message);
            this.message = message;
        }

        public VoltCompilerException(String message, Throwable cause) {
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

    public static class ProcedureDescriptor {
        public final ArrayList<String> m_authGroups;
        public final String m_className;
        // For DDL procedures. i.e., procedures that are not defined by Java classes.
        public final String m_stmtLiterals;
        public final String m_joinOrder;
        public final ProcedurePartitionData m_partitionData;
        public final boolean m_builtInStmt;    // auto-generated SQL statement
        public final Class<?> m_class;

        ProcedureDescriptor (final ArrayList<String> authGroups, final String className) {
            m_authGroups = authGroups;
            m_className = className;
            m_stmtLiterals = null;
            m_joinOrder = null;
            m_partitionData = null;
            m_builtInStmt = false;
            m_class = null;
        }

        public ProcedureDescriptor(final ArrayList<String> authGroups, final String scriptImpl, Class<?> clazz) {
            m_authGroups = authGroups;
            m_className = clazz.getName();
            m_stmtLiterals = null;
            m_joinOrder = null;
            m_partitionData = null;
            m_builtInStmt = false;
            m_class = clazz;
        }

        ProcedureDescriptor(final ArrayList<String> authGroups, final Class<?> clazz,
                            final ProcedurePartitionData partitionData) {
            m_authGroups = authGroups;
            m_className = clazz.getName();
            m_stmtLiterals = null;
            m_joinOrder = null;
            m_partitionData = partitionData;
            m_builtInStmt = false;
            m_class = clazz;
        }

        public ProcedureDescriptor (final ArrayList<String> authGroups, final String className,
                final String singleStmt, final String joinOrder, final ProcedurePartitionData partitionData,
                boolean builtInStmt, Class<?> clazz) {
            assert(className != null);
            assert(singleStmt != null);

            m_authGroups = authGroups;
            m_className = className;
            m_stmtLiterals = singleStmt;
            m_joinOrder = joinOrder;
            m_partitionData = partitionData;
            m_builtInStmt = builtInStmt;
            m_class = clazz;
        }
    }

    public VoltCompiler(boolean standaloneCompiler, boolean isXDCR, String user) {
        this.standaloneCompiler = standaloneCompiler;
        this.m_isXDCR = isXDCR;
        this.m_user = user;

        // reset the cache
        m_cachedAddedClasses.clear();
    }

    public VoltCompiler(boolean standaloneCompiler, boolean isXDCR) {
        this(standaloneCompiler, isXDCR, null);
    }

    /** Parameterless constructor is for embedded VoltCompiler use only.
     * @param isXDCR*/
    public VoltCompiler(boolean isXDCR) {
        this(isXDCR, null);
    }

    public VoltCompiler(boolean isXDCR, String user) {
        this(false, isXDCR, user);
    }

    public boolean hasErrors() {
        return m_errors.size() > 0;
    }

    public boolean hasErrorsOrWarnings() {
        return (m_warnings.size() > 0) || hasErrors();
    }

    public void addInfo(final String msg) {
        addInfo(msg, NO_LINE_NUMBER);
    }

    public void addWarn(final String msg) {
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
        } else {
            compilerLog.debug(fb.getLogString());
        }
    }

    public void addWarn(final String msg, final int lineNo) {
        final Feedback fb = new Feedback(Severity.WARNING, msg, m_currentFilename, lineNo);
        m_warnings.add(fb);
        compilerLog.warn(fb.getLogString());
    }

    void addErr(final String msg, final int lineNo) {
        final Feedback fb = new Feedback(Severity.ERROR, msg, m_currentFilename, lineNo);
        m_errors.add(fb);
        compilerLog.error(fb.getLogString());
    }

    public static void setVoltLogger(VoltLogger vl) {
        compilerLog = vl;
    }

    /**
     * Compile from a set of DDL files.
     *
     * @param jarOutputPath The location to put the finished JAR to.
     * @param ddlFilePaths The array of DDL files to compile (at least one is required).
     * @return true if successful
     * @throws VoltCompilerException
     */
    public boolean compileFromDDL(final String jarOutputPath, final String... ddlFilePaths) {
        if (ddlFilePaths.length == 0) {
            compilerLog.error("At least one DDL file is required.");
            return false;
        }
        List<VoltCompilerReader> ddlReaderList;
        try {
            ddlReaderList = DDLPathsToReaderList(ddlFilePaths);
        } catch (VoltCompilerException e) {
            compilerLog.error("Unable to open DDL file.", e);
            return false;
        }
        // NOTE: batch compiles from DDL sql files, not in AdHoc path.
        return compileInternalToFile(jarOutputPath, null, null, ddlReaderList, null);
    }

    /** Compiles a catalog from a user provided schema and (optional) jar file. */
    public boolean compileFromSchemaAndClasses(
            final List<File> schemaPaths, final List<File> classesJarPaths, final File catalogOutputPath) {
        if (schemaPaths != null && !schemaPaths.stream().allMatch(File::exists)) {
            compilerLog.error("Cannot compile nonexistent or missing schema.");
            return false;
        }

        List<VoltCompilerReader> ddlReaderList;
        try {
            if (schemaPaths == null || schemaPaths.isEmpty()) {
                ddlReaderList = new ArrayList<>(1);
                ddlReaderList.add(new VoltCompilerStringReader(AUTOGEN_DDL_FILE_NAME, m_emptyDDLComment));
            } else {
                ddlReaderList = DDLPathsToReaderList(
                        schemaPaths.stream().map(File::getAbsolutePath).toArray(String[]::new));
            }
        } catch (VoltCompilerException e) {
            compilerLog.error("Unable to open schema file \"" + schemaPaths + "\"", e);
            return false;
        }

        InMemoryJarfile inMemoryUserJar = new InMemoryJarfile();
        ClassLoader originalClassLoader = m_classLoader;
        try {
            m_classLoader = inMemoryUserJar.getLoader();
            if (classesJarPaths != null) {
                // Make user's classes available to the compiler and add all VoltDB artifacts to theirs (overwriting any existing VoltDB artifacts).
                // This keeps all their resources because stored procedures may depend on them.
                for (File classesJarPath: classesJarPaths) {
                    if (classesJarPath.exists()) {
                        InMemoryJarfile jarFile = new InMemoryJarfile(classesJarPath);
                        inMemoryUserJar.putAll(jarFile);
                    }
                }
            }
            // NOTE/TODO: this is not from AdHoc code branch. We use the old code path here, and don't update CalciteSchema from VoltDB catalog.
            if (compileInternal(null, null, ddlReaderList, Collections.emptyList(), inMemoryUserJar) == null) {
                return false;
            }
        } catch (IOException e) {
            compilerLog.error("Could not load classes from user supplied jar file", e);
            return false;
        } finally {
            m_classLoader = originalClassLoader;
        }

        try {
            inMemoryUserJar.writeToFile(catalogOutputPath).run();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            addErr("Error writing catalog jar to disk: " + e.getMessage());
            return false;
        }
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
        } catch (IOException e) {
            compilerLog.error("Failed to add DDL file to empty in-memory jar.");
            return false;
        }
        // NOTE/TODO: this is not from AdHoc code branch. We use the old code path here, and don't update CalciteSchema from VoltDB catalog.
        return compileInternalToFile(jarOutputPath, null, null, ddlReaderList, jarFile);
    }

    private static void addBuildInfo(final InMemoryJarfile jarOutput) {
        StringBuilder buildinfo = new StringBuilder();
        String[] info = RealVoltDB.extractBuildInfo(compilerLog);
        buildinfo.append(info[0]).append('\n');
        buildinfo.append(info[1]).append('\n');
        buildinfo.append(System.getProperty("user.name")).append('\n');
        buildinfo.append(System.getProperty("user.dir")).append('\n');
        buildinfo.append(System.currentTimeMillis()).append('\n');
        jarOutput.put(CatalogUtil.CATALOG_BUILDINFO_FILENAME, buildinfo.toString().getBytes(Constants.UTF8ENCODING));
    }

    /**
     * Internal method that takes the generated DDL from the catalog and builds a new catalog.
     * The generated catalog is diffed with the original catalog to verify compilation and
     * catalog generation consistency.
     */
    private void debugVerifyCatalog(InMemoryJarfile origJarFile, List<SqlNode> sqlNodes, Catalog origCatalog) {
        final VoltCompiler autoGenCompiler = new VoltCompiler(m_isXDCR);
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
                autogenReaderList, sqlNodes, autoGenJarOutput);
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
        } else {
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
            VoltCompiler autoGenCompiler, List<VoltCompilerReader> autogenReaderList, InMemoryJarfile autoGenJarOutput) {
        // Be sure to set RETRY_FAILED_CATALOG_REBUILD_UNDER_DEBUG = true to enable
        // this last ditch retry before crashing.
        // BREAKPOINT HERE!
        // Then step IN to debug the failed rebuild -- or, just as likely, the canonical ddl.
        // Or step OVER to debug just the catalog diff process, retried with verbose output --
        // maybe it's just being too sensitive to immaterial changes?
        Catalog autoGenCatalog = autoGenCompiler.compileCatalogInternal(null, null,
                autogenReaderList, Collections.emptyList(), autoGenJarOutput);
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
            final String jarOutputPath, final VoltCompilerReader canonicalDDLIfAny, final Catalog previousCatalogIfAny,
            final List<VoltCompilerReader> ddlReaderList, final InMemoryJarfile jarOutputRet) {
        if (jarOutputPath == null) {
            addErr("The output jar path is null.");
            return false;
        }

        // NOTE/TODO: All the callers of this is not from AdHoc code branch. We use the old code path here, and don't update CalciteSchema from VoltDB catalog.
        final InMemoryJarfile jarOutput = compileInternal(
                canonicalDDLIfAny, previousCatalogIfAny, ddlReaderList, Collections.emptyList(), jarOutputRet);
        try {
            if (jarOutput == null) {
                return false;
            } else {
                jarOutput.writeToFile(new File(jarOutputPath)).run();
                return true;
            }
        } catch (final Exception e) {
            e.printStackTrace();
            addErr("Error writing catalog jar to disk: " + e.getMessage());
            return false;
        }
    }

    private static void generateCatalogReport(Catalog catalog, String ddl, ArrayList<Feedback> warnings,
            InMemoryJarfile jarOutput) throws IOException {
        VoltDBInterface voltdb = VoltDB.instance();
        // try to get a catalog context
        CatalogContext catalogContext = voltdb != null ? voltdb.getCatalogContext() : null;
        ClusterSettings clusterSettings = catalogContext != null ? catalogContext.getClusterSettings() : null;
        CatalogMap<Table> tables = catalogContext != null ? catalogContext.getTables() : null;
        int tableCount = tables != null ? tables.size() : 0;
        Cluster cluster = catalogContext != null ? catalogContext.getCluster() : null;
        Deployment deployment = cluster != null ?  cluster.getDeployment().get("deployment") : null;
        int hostcount = clusterSettings != null ? clusterSettings.hostcount() : 1;
        int kfactor = deployment != null ? deployment.getKfactor() : 0;
        int sitesPerHost = 8;
        if  (catalogContext != null && catalogContext.getNodeSettings()!= null) {
            sitesPerHost =  voltdb.getCatalogContext().getNodeSettings().getLocalSitesCount();
        }
        boolean isPro = MiscUtils.isPro();
        long minHeapRqt = RealVoltDB.computeMinimumHeapRqt(tableCount, sitesPerHost, kfactor);

        String report = ReportMaker.report(catalog, minHeapRqt, isPro, hostcount, sitesPerHost, kfactor,
                warnings, ddl);
        // put the compiler report into the jarfile
        jarOutput.put(CATLOG_REPORT, report.getBytes(Constants.UTF8ENCODING));
    }

    /**
     * Internal method for compiling with and without a project.xml file or DDL files.
     *
     * @param canonicalDDLIfAny ???
     * @param previousCatalogIfAny
     * @param ddlReaderList The list of DDL files to read and compile ??? (when no project is provided).
     * @param sqlNodes Calcite SqlNodes for DDL stmts
     * @param jarOutputRet The in-memory jar to populate or null if the caller doesn't provide one.
     * @return The InMemoryJarfile containing the compiled catalog if
     * successful, null if not.  If the caller provided an InMemoryJarfile, the
     * return value will be the same object, not a copy.
     */
    private InMemoryJarfile compileInternal(
            final VoltCompilerReader canonicalDDLIfAny, final Catalog previousCatalogIfAny,
            final List<VoltCompilerReader> ddlReaderList, final List<SqlNode> sqlNodes,
            final InMemoryJarfile jarOutputRet) {
        // Expect to have either >1 ddl file or a project file.
        assert(ddlReaderList.size() > 0);
        // Make a temporary local output jar if one wasn't provided.
        final InMemoryJarfile jarOutput = jarOutputRet != null ? jarOutputRet : new InMemoryJarfile();

        if (ddlReaderList == null || ddlReaderList.isEmpty()) {
            addErr("One or more DDL files are required.");
            return null;
        }

        // clear out the warnings and errors
        m_warnings.clear();
        m_infos.clear();
        m_errors.clear();

        // do all the work to get the catalog
        final Catalog catalog = compileCatalogInternal(canonicalDDLIfAny, previousCatalogIfAny, ddlReaderList, sqlNodes, jarOutput);
        if (catalog == null) {
            return null;
        }
        Cluster cluster = catalog.getClusters().get("cluster");
        assert(cluster != null);
        Database database = cluster.getDatabases().get("database");
        assert(database != null);

        // Build DDL from Catalog Data
        String ddlWithBatchSupport = CatalogSchemaTools.toSchema(catalog);
        m_canonicalDDL = CatalogSchemaTools.toSchemaWithoutInlineBatches(ddlWithBatchSupport);

        // generate the catalog report and write it to disk
        try {
            generateCatalogReport(m_catalog, ddlWithBatchSupport, m_warnings, jarOutput);
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        jarOutput.put(AUTOGEN_DDL_FILE_NAME, m_canonicalDDL.getBytes(Constants.UTF8ENCODING));
        if (DEBUG_VERIFY_CATALOG) {
            debugVerifyCatalog(jarOutput, sqlNodes, catalog);
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
        } catch (final Exception e) {
            e.printStackTrace();
            return null;
        }
        assert(!hasErrors());
        if (hasErrors()) {
            return null;
        } else {
            return jarOutput;
        }
    }

    /**
     * Get textual explain plan info for each plan from the
     * catalog to be shoved into the catalog jarfile.
     */
    HashMap<String, byte[]> getExplainPlans(Catalog catalog) {
        HashMap<String, byte[]> retval = new HashMap<>();
        Database db = getCatalogDatabase(m_catalog);
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

    private VoltCompilerFileReader createDDLFileReader(String path) throws VoltCompilerException {
        try {
            return new VoltCompilerFileReader(VoltCompilerFileReader.getSchemaPath(m_projectFileURL, path));
        } catch (IOException e) {
            String msg = String.format("Unable to open schema file \"%s\" for reading: %s", path, e.getMessage());
            throw new VoltCompilerException(msg);
        }
    }

    private List<VoltCompilerReader> DDLPathsToReaderList(final String... ddlFilePaths) throws VoltCompilerException {
        List<VoltCompilerReader> ddlReaderList = new ArrayList<>(ddlFilePaths.length);
        for (String ddlFilePath : ddlFilePaths) {
            ddlReaderList.add(createDDLFileReader(ddlFilePath));
        }
        return ddlReaderList;
    }

    /**
     * Compile from DDL files (only).
     * @param ddlFilePaths  input ddl files
     * @return  compiled catalog
     * @throws VoltCompilerException
     */
    public Catalog compileCatalogFromDDL(final String... ddlFilePaths) throws VoltCompilerException {
        InMemoryJarfile jarOutput = new InMemoryJarfile();
        // NOTE/TODO: this is not from AdHoc code branch. We use the old code path here, and don't update CalciteSchema from VoltDB catalog.
        return compileCatalogInternal(null, null,
                DDLPathsToReaderList(ddlFilePaths), Collections.emptyList(), jarOutput);
    }

    /**
     * Internal method for compiling the catalog.
     *
     * @param canonicalDDLIfAny catalog-related info parsed from a project file ???
     * @param previousCatalogIfAny previous catalog object, null if not exists
     * @param ddlReaderList Reader objects for ddl files.
     * @param sqlNodes Calcite SqlNodes for DDL stmts
     * @param jarOutput The in-memory jar to populate or null if the caller doesn't provide one.
     * @return true if successful
     */
    private Catalog compileCatalogInternal(
            final VoltCompilerReader canonicalDDLIfAny, final Catalog previousCatalogIfAny,
            final List<VoltCompilerReader> ddlReaderList, final List<SqlNode> sqlNodes, final InMemoryJarfile jarOutput) {
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
            compileDatabaseNode(canonicalDDLIfAny, previousDBIfAny, ddlReaderList, sqlNodes, jarOutput);
        } catch (final VoltCompilerException e) {
            return null;
        }

        assert(m_catalog != null);

        // add epoch info to catalog
        final int epoch = (int)(TransactionIdManager.getEpoch() / 1000);
        m_catalog.getClusters().get("cluster").setLocalepoch(epoch);

        return m_catalog;
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

    // TODO: long term to remove it from tests
    public Database getCatalogDatabase() {
        return m_catalog.getClusters().get("cluster").getDatabases().get("database");
    }

    /**
     * @return The name of the user which initiated this compile operation or {@code null} if no user is specified
     */
    public String getUser() {
        return m_user;
    }

    public static Database getCatalogDatabase(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database");
    }

    private static Database initCatalogDatabase(Catalog catalog) {
        // create the database in the catalog
        catalog.execute("add /clusters#cluster databases database");
        addDefaultRoles(catalog);
        return getCatalogDatabase(catalog);
    }

    /**
     * Create default roles. These roles cannot be removed nor overridden in the DDL.
     * Make sure to omit these roles in the generated DDL in {@link org.voltdb.utils.CatalogSchemaTools}
     * Also, make sure to prevent them from being dropped by DROP ROLE in the DDLCompiler
     * !!!
     * IF YOU ADD A THIRD ROLE TO THE DEFAULTS, IT'S TIME TO BUST THEM OUT INTO A CENTRAL
     * LOCALE AND DO ALL THIS MAGIC PROGRAMATICALLY --izzy 11/20/2014
     */
    private static void addDefaultRoles(Catalog catalog) {
        // admin
        catalog.execute("add /clusters#cluster/databases#database groups administrator");
        Permission.setPermissionsInGroup(getCatalogDatabase(catalog).getGroups().get("administrator"),
                                         Permission.getPermissionsFromAliases(Arrays.asList("ADMIN")));

        // user
        catalog.execute("add /clusters#cluster/databases#database groups user");
        Permission.setPermissionsInGroup(getCatalogDatabase(catalog).getGroups().get("user"),
                                         Permission.getPermissionsFromAliases(Arrays.asList("SQL", "ALLPROC")));
    }

    public static enum DdlProceduresToLoad {
        NO_DDL_PROCEDURES, ALL_DDL_PROCEDURES
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
    public Catalog loadSchema(
            HSQLInterface hsql, DdlProceduresToLoad whichProcs, String... ddlFilePaths) throws VoltCompilerException {
        m_catalog = new Catalog(); //
        m_catalog.execute("add / clusters cluster");
        Database db = initCatalogDatabase(m_catalog);
        List<VoltCompilerReader> ddlReaderList = DDLPathsToReaderList(ddlFilePaths);
        final VoltDDLElementTracker voltDdlTracker = new VoltDDLElementTracker(this);
        InMemoryJarfile jarOutput = new InMemoryJarfile();
        // NOTE/TODO: this is not from AdHoc code branch. We use the old code path here, and don't update CalciteSchema from VoltDB catalog.
        compileDatabase(db, hsql, voltDdlTracker, null, null,
                ddlReaderList, Collections.emptyList(), null, whichProcs, jarOutput);
        return m_catalog;
    }

    /**
     * Load a ddl file with full support for VoltDB extensions (partitioning, procedures,
     * export), AND full support for input via a project xml file's "database" node.
     * @param canonicalDDLIfAny catalog-related info parsed from a project file
     * @param ddlReaderList Reader objects for ddl files.
     * @param jarOutput The in-memory jar to populate or null if the caller doesn't provide one.
     * @throws VoltCompilerException
     */
    private void compileDatabaseNode(
            VoltCompilerReader canonicalDDLIfAny, Database previousDBIfAny,
            final List<VoltCompilerReader> ddlReaderList, final List<SqlNode> sqlNodes,
            final InMemoryJarfile jarOutput) throws VoltCompilerException {
        final ArrayList<Class<?>> classDependencies = new ArrayList<>();
        final VoltDDLElementTracker voltDdlTracker = new VoltDDLElementTracker(this);

        Database db = initCatalogDatabase(m_catalog);

        // shutdown and make a new hsqldb <-- NOTE
        HSQLInterface hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());
        compileDatabase(db, hsql, voltDdlTracker, canonicalDDLIfAny, previousDBIfAny,
                ddlReaderList, sqlNodes, classDependencies, DdlProceduresToLoad.ALL_DDL_PROCEDURES, jarOutput);
    }

    /**
     * Common code for schema loading shared by loadSchema and compileDatabaseNode
     *
     * @param db the database entry in the catalog
     * @param hsql an interface to the hsql frontend, initialized and potentially reused by the caller.
     * @param voltDdlTracker non-standard VoltDB schema annotations, initially those from a project file
     * @param canonicalDDLIfAny ???
     * @param previousDBIfAny previous db catalog object, null if not exists???
     * @param sqlNodes Calcite SqlNodes for DDL stmts
     * @param classDependencies optional additional jar files required by procedures
     * @param whichProcs indicates which ddl-defined procedures to load: none, single-statement, or all
     * @param jarOutput The in-memory jar to populate or null if the caller doesn't provide one.
     */
    private void compileDatabase(
            Database db, HSQLInterface hsql, VoltDDLElementTracker voltDdlTracker,
            VoltCompilerReader canonicalDDLIfAny, Database previousDBIfAny, List<VoltCompilerReader> schemaReaders,
            List<SqlNode> sqlNodes, Collection<Class<?>> classDependencies, DdlProceduresToLoad whichProcs,
            InMemoryJarfile jarOutput) throws VoltCompilerException {
        // Actually parse and handle all the DDL
        // DDLCompiler also provides partition descriptors for DDL PARTITION
        // and REPLICATE statements.
        final DDLCompiler ddlcompiler;
        ddlcompiler = new DDLCompiler(this, hsql, voltDdlTracker, m_classLoader);

        // Ugly, ugly hack.
        // If the procedure compilations do not succeed, and we have
        // dropped some UDFs, then we need to restore them.
        try {
            //
            // Save the old user defined functions, if there are any,
            // in case we encounter a compilation error.
            //
            ddlcompiler.saveDefinedFunctions();
            if (canonicalDDLIfAny != null) {
                // add the file object's path to the list of files for the jar
                m_ddlFilePaths.put(canonicalDDLIfAny.getName(), canonicalDDLIfAny.getPath());
                ddlcompiler.loadSchema(canonicalDDLIfAny, db, previousDBIfAny, whichProcs, false);
            }

            m_dirtyTables.clear();

            for (final VoltCompilerReader schemaReader : schemaReaders) {
                String origFilename = m_currentFilename;
                try {
                    if (m_currentFilename.equals(NO_FILENAME)) {
                        m_currentFilename = schemaReader.getName();
                    }

                    // add the file object's path to the list of files for the jar
                    m_ddlFilePaths.put(schemaReader.getName(), schemaReader.getPath());

                    if (m_filterWithSQLCommand) {
                        SQLParser.FileInfo fi = new SQLParser.FileInfo(schemaReader.getPath());
                        ddlcompiler.loadSchemaWithFiltering(schemaReader, db, whichProcs, fi);
                    } else {
                        ddlcompiler.loadSchema(schemaReader, db, previousDBIfAny, whichProcs, true);
                    }
                } finally {
                    m_currentFilename = origFilename;
                }
            }

            // When A/A is enabled, create an export table for every DR table to log possible conflicts
            ddlcompiler.loadAutogenExportTableSchema(db, previousDBIfAny, whichProcs, m_isXDCR);
            sqlNodes.forEach(node -> {
                CreateTableUtils.addTable(node, db);
            });
            ddlcompiler.compileToCatalog(db, m_isXDCR); // NOTE: this is the place catalog gets added for create table.

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
            Map<String, String> persistentExportTables = voltDdlTracker.getPersistentTableTargetMap();
            for (Entry<String, String> e : persistentExportTables.entrySet()) {
                addExportTableToConnector(e.getValue(), e.getKey(), db);
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
            addExtraClasses(jarOutput);
        } catch (Throwable ex) {
            ddlcompiler.restoreSavedFunctions();
            throw ex;
        }
        ddlcompiler.clearSavedFunctions();
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
                } catch (Exception e) {
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
     * @param allProcs ???
     * @param classDependencies ???
     * @param whichProcs indicates which ddl-defined procedures to load: none, single-statement, or all
     * @param prevProcsIfAny ???
     * @param jarOutput The in-memory jar to populate or null if the caller doesn't provide one.
     * @throws VoltCompilerException
     */
    private void compileProcedures(
            Database db, HSQLInterface hsql, Collection<ProcedureDescriptor> allProcs,
            Collection<Class<?>> classDependencies, DdlProceduresToLoad whichProcs,
            CatalogMap<Procedure> prevProcsIfAny, InMemoryJarfile jarOutput) throws VoltCompilerException {
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

        final List<ProcedureDescriptor> procedures = new ArrayList<>(allProcs);

        // Actually parse and handle all the Procedures
        for (final ProcedureDescriptor procedureDescriptor : procedures) {
            final String procedureName = procedureDescriptor.m_className;
            if (procedureDescriptor.m_stmtLiterals == null) {
                m_currentFilename = procedureName.substring(procedureName.lastIndexOf('.') + 1);
                m_currentFilename += ".class";
            } else {
                m_currentFilename = procedureName;
            }
            ProcedureCompiler.compile(this, hsql, m_estimates, db, procedureDescriptor, jarOutput);
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
        if (m_capturedDiagnosticDetail != null) {
            m_capturedDiagnosticDetail.add(planDescription);
        }
    }

    /** Capture plan content in terse json format. */
    public void captureDiagnosticJsonFragment(String json) {
        if (m_capturedDiagnosticDetail != null) {
            m_capturedDiagnosticDetail.add(json);
        }
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
            throws VoltCompilerException {
        assert !StringUtils.isBlank(tableName) && catdb != null;

        // Catalog Connector
        Connector catconn = catdb.getConnectors().getIgnoreCase(targetName);
        if (catconn == null) {
            catconn = catdb.getConnectors().add(targetName);
        }
        Table tableref = catdb.getTables().getIgnoreCase(tableName);
        if (tableref == null) {
            throw new VoltCompilerException("While configuring export, table " + tableName + " was not present in " +
            "the catalog.");
        } else if (TableType.isStream(tableref.getTabletype())) {
            checkExportedStream(tableref, catdb, "export");

            // This is used to enforce default connectors assigned to streams through the Java Property
            // for streams that don't have the Export To Target clause
            if (TableType.isConnectorLessStream(tableref.getTabletype()) && System.getProperty(ExportDataProcessor.EXPORT_TO_TYPE) != null) {
                tableref.setTabletype(TableType.STREAM.get());
            }
        }

        // Reject materialized views except if migrating view
        if (tableref.getMaterializer() != null && !TableType.isPersistentMigrate(tableref.getTabletype())) {
            compilerLog.error("While configuring export, " + tableName + " is a " +
                                        "materialized view.  A view cannot be export source.");
            throw new VoltCompilerException("View configured as export source");
        }
        org.voltdb.catalog.ConnectorTableInfo connTableInfo =
                catconn.getTableinfo().getIgnoreCase(tableName);

        if (connTableInfo == null) {
            connTableInfo = catconn.getTableinfo().add(tableName);
            connTableInfo.setTable(tableref);
            connTableInfo.setAppendonly(true);
        } else  {
            throw new VoltCompilerException(String.format("Table \"%s\" is already exported", tableName));
        }
    }

    // Note: this method has side-effects on the stream being checked, and the views using it
    void checkExportedStream(Table table, final Database catdb, String what) throws VoltCompilerException {
        assert TableType.isStream(table.getTabletype());

        // Check views on stream
        Column pc = table.getPartitioncolumn();
        List<Table> tlist = CatalogUtil.getMaterializeViews(catdb, table);

        if (pc == null && tlist.size() != 0) {
            compilerLog.error("While configuring " + what + ", stream " + table.getTypeName() + " is a source table " +
                    "for a materialized view. Streams support views as long as partitioned column is part of the view.");
            throw new VoltCompilerException("Stream configured with materialized view without partitioned column.");
        }
        if (pc != null && pc.getName() != null && tlist.size() != 0) {
            for (Table t : tlist) {
                if (t.getColumns().get(pc.getName()) == null) {
                    compilerLog.error("While configuring " + what + ", table " + t + " is a source table " +
                            "for a materialized view. Export only tables support views as long as partitioned column is part of the view.");
                    throw new VoltCompilerException("Stream configured with materialized view without partitioned column in the view.");
                } else {
                    //Set partition column of view table to partition column of stream
                    t.setPartitioncolumn(t.getColumns().get(pc.getName()));
                }
            }
        }

        // Other checks
        if (table.getIndexes().size() > 0) {
            compilerLog.error("While configuring " + what + ", stream " + table + " has indexes defined. " +
                    "Streams can't have indexes (including primary keys).");
            throw new VoltCompilerException("Streams cannot be configured with indexes");
        }

        // Streams exporting to topic or target should never be set to replicated even if no partition column
        // exists. The results are "partitioned" streams with no partitioning column (iffy).
        if (table.getIsreplicated()) {
            table.setIsreplicated(false);
            table.setPartitioncolumn(null);
        }
    }

    void compileDRTable(final Entry<String, String> drNode, final Database db) throws VoltCompilerException {
        String tableName = drNode.getKey();
        String action = drNode.getValue();

        Table tableref = db.getTables().getIgnoreCase(tableName);
        if (tableref.getMaterializer() != null) {
            throw new VoltCompilerException("While configuring dr, table " + tableName + " is a materialized view." +
                    " DR does not support materialized view.");
        } else if (action.equalsIgnoreCase("DISABLE")) {
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
    public static void main(final String[] args) {
        // passing true to constructor indicates the compiler is being run in standalone mode
        final VoltCompiler compiler = new VoltCompiler(true, false);

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
            } else {
                System.err.printf("Usage: %s\n", usageNew);
                System.exit(-1);
            }
        } else {
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
                    } else {
                        countMultiPartition++;
                    }
                } else {
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
                        if (!s.getIscontentdeterministic()) {
                            determinismTag = "[NDC] ";
                            nonDetProcs.add(p);
                        } else if (!s.getIsorderdeterministic()) {
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
                    outputStream.printf("Added %d additional classes to the catalog jar.\n\n", m_addedClasses.length);
                } else {
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
            outputStream.printf("Catalog contains %d built-in CRUD procedures.\n" +
                    "\tSimple insert, update, delete, upsert and select procedures are created\n" +
                    "\tautomatically for convenience.\n\n",
                    countDefaultProcs);
            if (countSinglePartition > 0) {
                outputStream.printf("[SP] Catalog contains %d single partition procedures.\n" +
                        "\tSingle partition procedures run in parallel and scale\n" +
                        "\tas partitions are added to a cluster.\n\n",
                        countSinglePartition);
            }
            if (countMultiPartition > 0) {
                outputStream.printf("[MP] Catalog contains %d multi-partition procedures.\n" +
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
                outputStream.print("\n\tTable scans do not use indexes and may become slower as tables grow.\n\n");
            }
            if (!nonDetProcs.isEmpty()) {
                outputStream.println("[NDO][NDC] NON-DETERMINISTIC CONTENT OR ORDER WARNING:\n" +
                        "\tThe procedures listed below contain non-deterministic queries.\n");

                for (Procedure p : nonDetProcs) {
                    outputStream.println("\t\t" + p.getClassname());
                }

                outputStream.print("\n" +
                                "\tUsing the output of these queries as input to subsequent\n" +
                                "\twrite queries can result in differences between replicated\n" +
                                "\tpartitions at runtime, forcing VoltDB to shutdown the cluster.\n" +
                                "\tReview the compiler messages above to identify the offending\n" +
                                "\tSQL statements (marked as \"[NDO] or [NDC]\").  Add a unique\n" +
                                "\tindex to the schema or an explicit ORDER BY clause to the\n" +
                                "\tquery to make these queries deterministic.\n\n");
            }
            if (countSinglePartition == 0 && countMultiPartition > 0) {
                outputStream.print("ALL MULTI-PARTITION WARNING:\n" +
                                "\tAll of the user procedures are multi-partition. This often\n" +
                                "\tindicates that the application is not utilizing VoltDB partitioning\n" +
                                "\tfor best performance. For information on VoltDB partitioning, see:\n" +
                                "\thttp://voltdb.com/docs/UsingVoltDB/ChapAppDesign.php\n\n");
            }
            if (m_reportPath != null) {
                outputStream.println("------------------------------------------\n");
                outputStream.println(String.format("Full catalog report can be found at file://%s.\n", m_reportPath));
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

    public List<Class<?>> getInnerClasses(Class <?> c) throws VoltCompilerException {
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
                } catch (ClassNotFoundException e) {
                    String msg = "Unable to load " + c + " inner class " + innerName +
                            " from in-memory jar representation.";
                    throw new VoltCompilerException(msg);
                }
                assert(clz != null);
                builder.add(clz);
            }
        } else {
            String stem = c.getName().replace('.', '/');
            String cpath = stem + ".class";
            URL curl = cl.getResource(cpath);
            if (curl == null) {
                throw new VoltCompilerException(String.format("Failed to find class file %s in jar.", cpath));
            }

            // load from an on-disk jar
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
                try (JarFile jar = new JarFile(jarFN)) {
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
                }
            } else if ("file".equals(curl.getProtocol())) { // load directly from a classfile
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
            throws VoltCompiler.VoltCompilerException {
        if (m_cachedAddedClasses.contains(cls)) {
            return false;
        }
        m_cachedAddedClasses.add(cls);

        for (final Class<?> nested : getInnerClasses(cls)) {
            addClassToJar(jarOutput, nested);
        }

        try {
            return VoltCompilerUtils.addClassToJar(jarOutput, cls);
        } catch (IOException e) {
            throw new VoltCompilerException(e.getMessage());
        }
    }

    public void setInitializeDDLWithFiltering(boolean flag) {
        m_filterWithSQLCommand = flag;
    }

    /**
     * Helper enum that scans sax exception messages for deprecated xml elements
     *
     * @author ssantoro
     */
    enum DeprecatedProjectElement {
        security("(?i)\\Acvc-[^:]+:\\s+Invalid\\s+content\\s+.+?\\s+element\\s+'security'",
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
        static DeprecatedProjectElement valueOf(JAXBException jxbex) {
            if(jxbex == null || jxbex.getLinkedException() == null ||
                    ! (jxbex.getLinkedException() instanceof org.xml.sax.SAXParseException)) {
                return null;
            }
            org.xml.sax.SAXParseException saxex =
                    org.xml.sax.SAXParseException.class.cast(jxbex.getLinkedException());
            for( DeprecatedProjectElement dpe: DeprecatedProjectElement.values()) {
                Matcher mtc = dpe.messagePattern.matcher(saxex.getMessage());
                if (mtc.find()) {
                    return dpe;
                }
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
    public void compileInMemoryJarfileWithNewDDL(
            InMemoryJarfile jarfile, String newDDL, List<SqlNode> sqlNodes, Catalog oldCatalog)
            throws IOException, VoltCompilerException {
        String oldDDL = new String(jarfile.get(VoltCompiler.AUTOGEN_DDL_FILE_NAME),
                Constants.UTF8ENCODING);
        compilerLog.trace("OLD DDL: " + oldDDL);

        // Use the in-memory jarfile-provided class loader so that procedure
        // classes can be found and copied to the new file that gets written.
        ClassLoader originalClassLoader = m_classLoader;
        try (VoltCompilerStringReader canonicalDDLReader = new VoltCompilerStringReader(VoltCompiler.AUTOGEN_DDL_FILE_NAME, oldDDL);
             VoltCompilerStringReader newDDLReader = new VoltCompilerStringReader("Ad Hoc DDL Input", newDDL)) {

            List<VoltCompilerReader> ddlList = new ArrayList<>();
            ddlList.add(newDDLReader);

            m_classLoader = jarfile.getLoader();
            // Do the compilation work.
            InMemoryJarfile jarOut = compileInternal(canonicalDDLReader, oldCatalog, ddlList, sqlNodes, jarfile);
            // Trim the compiler output to try to provide a concise failure
            // explanation
            if (jarOut == null) {
                String errString = "Adhoc DDL failed";
                if (m_errors.size() > 0) {
                    errString = m_errors.get(m_errors.size() - 1).getLogString();
                }

                int endtrim = errString.indexOf(" in statement starting");
                if (endtrim < 0) {
                    endtrim = errString.length();
                }
                String trimmed = errString.substring(0, endtrim);
                throw new VoltCompilerException(trimmed);
            }
            compilerLog.debug("Successfully recompiled InMemoryJarfile");
        } finally {
            // Restore the original class loader
            m_classLoader = originalClassLoader;
        }
    }

    /**
     * Compile the provided jarfile.  Basically, treat the jarfile as a staging area
     * for the artifacts to be included in the compile, and then compile it in place.
     *
     * @throws ClassNotFoundException
     * @throws VoltCompilerException
     * @throws IOException
     *
     */
    public void compileInMemoryJarfileForUpdateClasses(InMemoryJarfile jarOutput,
            Catalog currentCatalog, HSQLInterface hsql) throws IOException, ClassNotFoundException, VoltCompilerException {
        // clear out the warnings and errors
        m_warnings.clear();
        m_infos.clear();
        m_errors.clear();

        // do all the work to get the catalog
        Catalog catalog = currentCatalog.deepCopy();
        Cluster cluster = catalog.getClusters().get("cluster");
        Database db = cluster.getDatabases().get("database");

        // Get DDL from InMemoryJar
        byte[] ddlBytes = jarOutput.get(AUTOGEN_DDL_FILE_NAME);
        String canonicalDDL = new String(ddlBytes, Constants.UTF8ENCODING);

        CatalogMap<Procedure> procedures = db.getProcedures();

        // build a cache of previous SQL stmts
        m_previousCatalogStmts.clear();
        for (Procedure prevProc : procedures) {
            for (Statement prevStmt : prevProc.getStatements()) {
                addStatementToCache(prevStmt);
            }
        }

        // Use the in-memory jar-file-provided class loader so that procedure
        // classes can be found and copied to the new file that gets written.
        ClassLoader classLoader = jarOutput.getLoader();

        for (Procedure procedure : procedures) {
            if (!procedure.getHasjava()) {
                // Skip the DDL statement stored procedures as @UpdateClasses does not affect them
                continue;
            }
            // default procedure is also a single statement procedure
            assert (procedure.getDefaultproc() == false);

            if (procedure.getSystemproc()) {
                // UpdateClasses does not need to update system procedures
                continue;
            }

            // clear up the previous procedure contents before recompiling java user procedures
            procedure.getStatements().clear();
            procedure.getParameters().clear();

            final String className = procedure.getClassname();

            // Load the class given the class name
            Class<?> procClass = classLoader.loadClass(className);
            // get the short name of the class (no package)
            String shortName = ProcedureCompiler.deriveShortProcedureName(className);

            ProcedureAnnotation pa = (ProcedureAnnotation) procedure.getAnnotation();
            if (pa == null) {
                pa = new ProcedureAnnotation();
                procedure.setAnnotation(pa);
            }

            // if the procedure is non-transactional, then take this special path here
            if (VoltNonTransactionalProcedure.class.isAssignableFrom(procClass)) {
                ProcedureCompiler.compileNTProcedure(this, procClass, procedure, jarOutput);
                continue;
            }

            // if still here, that means the procedure is transactional
            procedure.setTransactional(true);

            // iterate through the fields and get valid sql statements
            Map<String, SQLStmt> stmtMap = ProcedureCompiler.getSQLStmtMap(this, procClass);
            Map<String, Object> fields = ProcedureCompiler.getFiledsMap(this, stmtMap, procClass, shortName);
            Method procMethod = (Method) fields.get("@run");
            assert (procMethod != null);

            ProcedureCompiler.compileSQLStmtUpdatingProcedureInfomation(this, hsql, m_estimates, db, procedure,
                    procedure.getSinglepartition(), fields);

            // set procedure parameter types
            Class<?>[] paramTypes = ProcedureCompiler.setParameterTypes(this, procedure, shortName, procMethod);

            ProcedurePartitionData partitionData = ProcedurePartitionData.extractPartitionData(procedure);
            ProcedureCompiler.addPartitioningInfo(this, procedure, db, paramTypes, partitionData);

            // put the compiled code for this procedure into the jarFile
            // need to find the outermost ancestor class for the procedure in the event
            // that it's actually an inner (or inner inner...) class.
            // addClassToJar recursively adds all the children, which should include this
            // class
            Class<?> ancestor = procClass;
            while (ancestor.getEnclosingClass() != null) {
                ancestor = ancestor.getEnclosingClass();
            }
            addClassToJar(jarOutput, ancestor);
        }

        ////////////////////////////////////////////
        // allow gc to reclaim any cache memory here
        m_previousCatalogStmts.clear();


        // generate the catalog report and write it to disk
        generateCatalogReport(catalog, canonicalDDL, m_warnings, jarOutput);

        // WRITE CATALOG TO JAR HERE
        final String catalogCommands = catalog.serialize();

        byte[] catalogBytes = catalogCommands.getBytes(Constants.UTF8ENCODING);

        // Don't update buildinfo if it's already present, e.g. while upgrading.
        // Note when upgrading the version has already been updated by the caller.
        if (!jarOutput.containsKey(CatalogUtil.CATALOG_BUILDINFO_FILENAME)) {
            addBuildInfo(jarOutput);
        }
        jarOutput.put(CatalogUtil.CATALOG_FILENAME, catalogBytes);

        assert(!hasErrors());
        if (hasErrors()) {
            StringBuilder sb = new StringBuilder();
            for (Feedback fb : m_errors) {
                sb.append(fb.getLogString());
            }
            throw new VoltCompilerException(sb.toString());
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
    public String upgradeCatalogAsNeeded(InMemoryJarfile outputJar) throws IOException {
        // getBuildInfoFromJar() performs some validation.
        String[] buildInfoLines = CatalogUtil.getBuildInfoFromJar(outputJar);
        String versionFromCatalog = buildInfoLines[0];
        // Set if an upgrade happens.
        String upgradedFromVersion = null;

        // Check if it's compatible (or the upgrade is being forced).
        // getConfig() may return null if it's being mocked for a test.
        if (VoltDB.Configuration.m_forceCatalogUpgrade
            || !versionFromCatalog.equals(VoltDB.instance().getVersionString())) {

            // Patch the buildinfo.
            String versionFromVoltDB = VoltDB.instance().getVersionString();
            buildInfoLines[0] = versionFromVoltDB;
            buildInfoLines[1] = String.format("voltdb-auto-upgrade-to-%s", versionFromVoltDB);
            byte[] buildInfoBytes = StringUtils.join(buildInfoLines, "\n").getBytes();
            outputJar.put(CatalogUtil.CATALOG_BUILDINFO_FILENAME, buildInfoBytes);

            // Gather DDL files for re-compilation
            List<VoltCompilerReader> ddlReaderList = new ArrayList<>();
            Entry<String, byte[]> entry = outputJar.firstEntry();
            while (entry != null) {
                String path = entry.getKey();
                // ENG-12980: only look for auto-gen.ddl on root directory
                if (AUTOGEN_DDL_FILE_NAME.equalsIgnoreCase(path)) {
                    ddlReaderList.add(new VoltCompilerJarFileReader(outputJar, path));
                    break;
                }
                entry = outputJar.higherEntry(entry.getKey());
            }

            if (ddlReaderList.isEmpty()) {
                // did not find auto generated DDL file during upgrade
                throw new IOException("Could not find " + AUTOGEN_DDL_FILE_NAME + " in the catalog "
                        + "compiled by VoltDB " + versionFromCatalog);
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
                boolean success = compileInternalToFile(outputJarPath, null,
                        null, ddlReaderList, outputJar);

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
                            } else {
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
                try (PrintStream outputStream = new PrintStream(outputTextPath)) {
                    if (success) {
                        summarizeSuccess(outputStream, outputStream, outputJarPath);
                        consoleLog.info(String.format("The catalog was automatically upgraded from " +
                                        "version %s to %s and saved to \"%s\". " +
                                        "Compiler output is available in \"%s\".",
                                versionFromCatalog, versionFromVoltDB,
                                outputJarPath, outputTextPath));
                    } else {
                        summarizeErrors(outputStream, outputStream);
                        outputStream.close();
                        compilerLog.error("Catalog upgrade failed.");
                        compilerLog.info(String.format("Had attempted to perform an automatic version upgrade of a " +
                                        "catalog that was compiled by an older %s version of VoltDB, " +
                                        "but the automatic upgrade failed. The cluster  will not be " +
                                        "able to start until the incompatibility is fixed. " +
                                        "Try re-compiling the catalog with the newer %s version " +
                                        "of the VoltDB compiler. Compiler output from the failed " +
                                        "upgrade is available in \"%s\".",
                                versionFromCatalog, versionFromVoltDB, outputTextPath));
                        throw new IOException(String.format(
                                "Catalog upgrade failed. You will need to recompile."));
                    }
                }
            } catch (IOException ioe) { // Do nothing because this could come from the normal failure path
                throw ioe;
            } catch (Exception e) {
                compilerLog.error("Catalog upgrade failed with error:");
                compilerLog.error(e.getMessage());
                compilerLog.info(String.format("Had attempted to perform an automatic version upgrade of a " +
                                "catalog that was compiled by an older %s version of VoltDB, " +
                                "but the automatic upgrade failed. The cluster  will not be " +
                                "able to start until the incompatibility is fixed. " +
                                "Try re-compiling the catalog with the newer %s version " +
                                "of the VoltDB compiler. Compiler output from the failed " +
                                "upgrade is available in \"%s\".",
                        versionFromCatalog, versionFromVoltDB, outputTextPath));
                throw new IOException(String.format(
                        "Catalog upgrade failed. You will need to recompile."));
            } finally {
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
    public void markTableAsDirty(String tableName) {
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
            if (isDirtyTable(tableName)) {
                ++m_stmtCacheMisses;
                return null;
            }
        }
        tablesTouched = candidate.getTablesupdated().split(",");
        for (String tableName : tablesTouched) {
            if (isDirtyTable(tableName)) {
                ++m_stmtCacheMisses;
                return null;
            }
        }

        ++m_stmtCacheHits;
        // easy debugging stmt
        //printStmtCacheStats();
        return candidate;
    }

    private boolean isDirtyTable(String tableName) {
        return m_dirtyTables.contains(tableName.toLowerCase());
    }

    @SuppressWarnings("unused")
    private void printStmtCacheStats() {
        System.out.printf("Hits: %d, Misses %d, Percent %.2f\n",
                m_stmtCacheHits, m_stmtCacheMisses,
                (m_stmtCacheHits * 100.0) / (m_stmtCacheHits + m_stmtCacheMisses));
        System.out.flush();
    }

}
