/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hsqldb.HSQLInterface;
import org.voltdb.ProcInfo;
import org.voltdb.ProcInfoData;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.compiler.projectfile.SnapshotType;
import org.voltdb.compiler.projectfile.DatabaseType;
import org.voltdb.compiler.projectfile.GroupsType;
import org.voltdb.compiler.projectfile.ProceduresType;
import org.voltdb.compiler.projectfile.ProjectType;
import org.voltdb.compiler.projectfile.SchemasType;
import org.voltdb.compiler.projectfile.SecurityType;
import org.voltdb.compiler.projectfile.UsersType;
import org.voltdb.compiler.projectfile.ClassdependenciesType.Classdependency;
import org.voltdb.compiler.projectfile.ExportsType.Connector;
import org.voltdb.compiler.projectfile.ExportsType.Connector.Tables;
import org.voltdb.compiler.projectfile.ExportsType.Connector.Destinations.Destination;
import org.voltdb.utils.HexEncoder;
import org.voltdb.utils.JarReader;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.StringInputStream;
import org.voltdb.utils.VoltLoggerFactory;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Compiles a project XML file and some metadata into a Jarfile
 * containing stored procedure code and a serialzied catalog.
 *
 */
public class VoltCompiler {
    /** Represents the level of severity for a Feedback message generated during compiling. */
    public static enum Severity { INFORMATIONAL, WARNING, ERROR, UNEXPECTED };
    public static final int NO_LINE_NUMBER = -1;

    // feedback by filename
    ArrayList<Feedback> m_allFeedback = new ArrayList<Feedback>();
    ArrayList<Feedback> m_infos = new ArrayList<Feedback>();
    ArrayList<Feedback> m_warnings = new ArrayList<Feedback>();
    ArrayList<Feedback> m_errors = new ArrayList<Feedback>();

    // set of annotations by procedure name
    Map<String, ProcInfoData> m_procInfoOverrides;

    String m_projectFileURL = null;
    String m_jarOutputPath = null;
    PrintStream m_outputStream = null;
    String m_currentFilename = null;
    Map<String, String> m_ddlFilePaths = new HashMap<String, String>();

    JarBuilder m_jarBuilder = null;
    Catalog m_catalog = null;
    //Cluster m_cluster = null;
    HSQLInterface m_hsql = null;

    DatabaseEstimates m_estimates = new DatabaseEstimates();

    private static final Logger compilerLog = Logger.getLogger("COMPILER", VoltLoggerFactory.instance());
    @SuppressWarnings("unused")
    private static final Logger Log = Logger.getLogger("org.voltdb.compiler.VoltCompiler", VoltLoggerFactory.instance());

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

            if (fileName != null) {
                retval += " [" + fileName;
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
        public void error(final SAXParseException exception) throws SAXException {
            addErr(exception.getMessage(), exception.getLineNumber());
        }

        public void fatalError(final SAXParseException exception) throws SAXException {
            //addErr(exception.getMessage(), exception.getLineNumber());
        }

        public void warning(final SAXParseException exception) throws SAXException {
            addWarn(exception.getMessage(), exception.getLineNumber());
        }
    }

    class ProcedureDescriptor {
        final ArrayList<String> m_authUsers;
        final ArrayList<String> m_authGroups;
        final String m_className;
        // for single-stmt procs
        final String m_singleStmt;
        final String m_partitionString;

        ProcedureDescriptor (final ArrayList<String> authUsers, final ArrayList<String> authGroups, final String className) {
            assert(className != null);

            m_authUsers = authUsers;
            m_authGroups = authGroups;
            m_className = className;
            m_singleStmt = null;
            m_partitionString = null;
        }

        ProcedureDescriptor (final ArrayList<String> authUsers, final ArrayList<String> authGroups, final String className, final String singleStmt, final String partitionString) {
            assert(className != null);
            assert(singleStmt != null);

            m_authUsers = authUsers;
            m_authGroups = authGroups;
            m_className = className;
            m_singleStmt = singleStmt;
            m_partitionString = partitionString;
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
        addFeedback(fb);
    }

    void addWarn(final String msg, final int lineNo) {
        final Feedback fb = new Feedback(Severity.WARNING, msg, m_currentFilename, lineNo);
        m_warnings.add(fb);
        addFeedback(fb);
    }

    void addErr(final String msg, final int lineNo) {
        final Feedback fb = new Feedback(Severity.ERROR, msg, m_currentFilename, lineNo);
        m_errors.add(fb);
        addFeedback(fb);
    }

    void addFeedback(final Feedback fb) {
        m_allFeedback.add(fb);

        if (m_outputStream != null) m_outputStream.println(fb.getStandardFeedbackLine());
    }

    /**
     * @param projectFileURL URL of the project file.
     * @param clusterConfig Object containing desired physical cluster parameters
     * @param jarOutputPath The location to put the finished JAR to.
     * @param output Where to print status/errors to, usually stdout.
     * @param procInfoOverrides Optional overridden values for procedure annotations.
     */

    public boolean compile(final String projectFileURL,
                           final ClusterConfig clusterConfig,
                           final String jarOutputPath, final PrintStream output,
                           final Map<String, ProcInfoData> procInfoOverrides)
    {
        m_hsql = null;
        m_projectFileURL = projectFileURL;
        m_jarOutputPath = jarOutputPath;
        m_outputStream = output;
        // use this map as default annotation values
        m_procInfoOverrides = procInfoOverrides;

        compilerLog.l7dlog( Level.INFO, LogKeys.compiler_VoltCompiler_LeaderAndHostCountAndSitesPerHost.name(),
                new Object[] { clusterConfig.getLeaderAddress(),
                               clusterConfig.getHostCount(),
                               clusterConfig.getSitesPerHost() }, null);

        // do all the work to get the catalog
        final Catalog catalog = compileCatalog(projectFileURL, clusterConfig);
        if (catalog == null)
            return false;

        // WRITE CATALOG TO JAR HERE
        final String catalogCommands = catalog.serialize();
        byte[] catalogBytes = null;
        try {
            catalogBytes =  catalogCommands.getBytes("UTF-8");
        } catch (final UnsupportedEncodingException e1) {
            addErr("Can't encode the compiled catalog file correctly");
            return false;
        }

        try {
            m_jarBuilder.addEntry("catalog.txt", catalogBytes);
            m_jarBuilder.addEntry("project.xml", new File(projectFileURL));
            for (final Entry<String, String> e : m_ddlFilePaths.entrySet())
                m_jarBuilder.addEntry(e.getKey(), new File(e.getValue()));
            m_jarBuilder.writeJarToDisk(jarOutputPath);
        } catch (final VoltCompilerException e) {
            return false;
        }

        assert(!hasErrors());

        if (hasErrors()) {
            return false;
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    public Catalog compileCatalog(final String projectFileURL,
                                  final ClusterConfig clusterConfig)
    {
        if (!clusterConfig.validate())
        {
            addErr(clusterConfig.getErrorMsg());
            return null;
        }

        // Compiler instance is reusable. Clear the cache.
        cachedAddedClasses.clear();
        m_currentFilename = new File(projectFileURL).getName();
        m_jarBuilder = new JarBuilder(this);

        if (m_outputStream != null) {
            m_outputStream.println("\n** BEGIN PROJECT COMPILE: " + m_currentFilename + " **");
        }

        ProjectType project = null;

        try {
            JAXBContext jc = JAXBContext.newInstance("org.voltdb.compiler.projectfile");
            // This schema shot the sheriff.
            SchemaFactory sf = SchemaFactory.newInstance(
              javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(this.getClass().getResource("ProjectFileSchema.xsd"));
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            // But did not shoot unmarshaller!
            unmarshaller.setSchema(schema);
            JAXBElement<ProjectType> result = (JAXBElement<ProjectType>) unmarshaller.unmarshal(new File(projectFileURL));
            project = result.getValue();
        }
        catch (JAXBException e) {
            // Convert some linked exceptions to more friendly errors.
            if (e.getLinkedException() instanceof java.io.FileNotFoundException) {
                addErr(e.getLinkedException().getMessage());
                return null;
            }
            if (e.getLinkedException() instanceof org.xml.sax.SAXParseException) {
                addErr("Error schema validating project.xml file. " + e.getLinkedException().getMessage());
                return null;
            }
            throw new RuntimeException(e);
        }
        catch (SAXException e) {
            addErr("Error schema validating project.xml file. " + e.getMessage());
            return null;
        }

        try {
            compileXMLRootNode(project);
        } catch (final VoltCompilerException e) {
            compilerLog.l7dlog( Level.ERROR, LogKeys.compiler_VoltCompiler_FailedToCompileXML.name(), null);
            compilerLog.error(e.getMessage());
            return null;
        }
        assert(m_catalog != null);

        try
        {
            ClusterCompiler.compile(m_catalog, clusterConfig);
        }
        catch (RuntimeException e)
        {
            addErr(e.getMessage());
            return null;
        }

        // add epoch info to catalog
        final int epoch = (int)(TransactionIdManager.getEpoch() / 1000);
        m_catalog.getClusters().get("cluster").setLocalepoch(epoch);

        // done handling files
        m_currentFilename = null;
        return m_catalog;
    }

    ProcInfoData getProcInfoOverride(final String procName) {
        if (m_procInfoOverrides == null)
            return null;
        return m_procInfoOverrides.get(procName);
    }

    void addEntryToJarOutput(final String key, final byte[] bytes)
    throws VoltCompilerException {
        m_jarBuilder.addEntry(key, bytes);
    }

    public Catalog getCatalog() {
        return m_catalog;
    }

    void compileXMLRootNode(ProjectType project) throws VoltCompilerException {
        m_catalog = new Catalog();
        temporaryCatalogInit();

        SecurityType security = project.getSecurity();
        if (security != null) {
            m_catalog.getClusters().get("cluster").
                setSecurityenabled(security.isEnabled());

        }

        DatabaseType database = project.getDatabase();
        if (database != null) {
            compileDatabaseNode(database);
        }
    }

    /**
     * Initialize the catalog for one cluster
     */
    void temporaryCatalogInit() {
        m_catalog.execute("add / clusters cluster");
        m_catalog.getClusters().get("cluster").setSecurityenabled(false);
    }

    void compileDatabaseNode(DatabaseType database) throws VoltCompilerException {
        final ArrayList<String> programs = new ArrayList<String>();
        final ArrayList<String> schemas = new ArrayList<String>();
        final ArrayList<ProcedureDescriptor> procedures = new ArrayList<ProcedureDescriptor>();
        final ArrayList<Class<?>> classDependencies = new ArrayList<Class<?>>();
        final ArrayList<String[]> partitions = new ArrayList<String[]>();

        final String databaseName = database.getName();

        // schema does not verify that the database is named "database"
        if (databaseName.equals("database") == false) {
            final String msg = "VoltDB currently requires all database elements to be named "+
                         "\"database\" (found: \"" + databaseName + "\")";
            throw new VoltCompilerException(msg);
        }

        // create the database in the catalog
        m_catalog.execute("add /clusters[cluster] databases " + databaseName);
        Database db = m_catalog.getClusters().get("cluster").getDatabases().get(databaseName);

        SnapshotType snapshotSettings = database.getSnapshot();
        if (snapshotSettings != null) {
            SnapshotSchedule schedule = db.getSnapshotschedule().add("default");
            String frequency = snapshotSettings.getFrequency();
            if (!frequency.endsWith("s") &&
                    !frequency.endsWith("m") &&
                    !frequency.endsWith("h")) {
                throw new VoltCompilerException(
                        "Snapshot frequency needs to end with time unit specified " +
                        " that is one of [s, m, h] (seconds, minutes, hours)");
            }

            int frequencyInt = 0;
            String frequencySubstring = frequency.substring(0, frequency.length() - 1);
            try {
                frequencyInt = Integer.parseInt(frequencySubstring);
            } catch (Exception e) {
                throw new VoltCompilerException("Frequency " + frequencySubstring +
                        " is not an integer ");
            }

            String prefix = snapshotSettings.getPrefix();
            if (prefix == null || prefix.isEmpty()) {
                throw new VoltCompilerException("Snapshot prefix " + prefix +
                " is not a valid prefix ");
            }

            if (prefix.contains("-") || prefix.contains(",")) {
                throw new VoltCompilerException("Snapshot prefix " + prefix +
                " cannot include , or - ");
            }

            String path = snapshotSettings.getPath();
            if (path == null || path.isEmpty()) {
                throw new VoltCompilerException("Snapshot path " + path +
                " is not a valid path ");
            }

            if (snapshotSettings.getRetain() == null) {
                throw new VoltCompilerException("Snapshot retain value not provided");
            }

            int retain = snapshotSettings.getRetain().intValue();
            if (retain < 1) {
                throw new VoltCompilerException("Snapshot retain value " + retain +
                        " is not a valid value. Must be 1 or greater.");
            }

            schedule.setFrequencyunit(
                    frequency.substring(frequency.length() - 1, frequency.length()));
            schedule.setFrequencyvalue(frequencyInt);
            schedule.setPath(path);
            schedule.setPrefix(prefix);
            schedule.setRetain(retain);
        }

        // schemas/schema
        for (SchemasType.Schema schema : database.getSchemas().getSchema()) {
            compilerLog.l7dlog( Level.INFO, LogKeys.compiler_VoltCompiler_CatalogPath.name(),
                                new Object[] {schema.getPath()}, null);
            schemas.add(schema.getPath());
        }

        // groups/group.
        if (database.getGroups() != null) {
            for (GroupsType.Group group : database.getGroups().getGroup()) {
                org.voltdb.catalog.Group catGroup = db.getGroups().add(group.getName());
                catGroup.setAdhoc(group.isAdhoc());
                catGroup.setSysproc(group.isSysproc());
            }
        }

        // users/user
        if (database.getUsers() != null) {
            for (UsersType.User user : database.getUsers().getUser()) {
                org.voltdb.catalog.User catUser = db.getUsers().add(user.getName());
                catUser.setAdhoc(user.isAdhoc());
                catUser.setSysproc(user.isSysproc());
                byte passwordHash[] = extractPassword(user.getPassword());
                catUser.setShadowpassword(HexEncoder.hexEncode(passwordHash));

                // process the @groups comma separated list
                if (user.getGroups() != null) {
                    String grouplist[] = user.getGroups().split(",");
                    for (final String group : grouplist) {
                        final GroupRef groupRef = catUser.getGroups().add(group);
                        final Group catalogGroup = db.getGroups().get(group);
                        if (catalogGroup != null) {
                            groupRef.setGroup(catalogGroup);
                        }
                    }
                }
            }
        }

        // procedures/procedure
        for (ProceduresType.Procedure proc : database.getProcedures().getProcedure()) {
            procedures.add(getProcedure(proc));
        }

        // classdependencies/classdependency
        if (database.getClassdependencies() != null) {
            for (Classdependency dep : database.getClassdependencies().getClassdependency()) {
                classDependencies.add(getClassDependency(dep));
            }
        }

        // partitions/table
        if (database.getPartitions() != null) {
            for (org.voltdb.compiler.projectfile.PartitionsType.Partition table : database.getPartitions().getPartition()) {
                partitions.add(getPartition(table));
            }
        }

        String msg = "Database \"" + databaseName + "\" ";
        // TODO: schema allows 0 procedures. Testbase relies on this.
        if (procedures.size() == 0) {
            msg += "needs at least one \"procedure\" element " +
                    "(currently has " + String.valueOf(procedures.size()) + ")";
            throw new VoltCompilerException(msg);
        }
        if (procedures.size() < 1) {
            msg += "is missing the \"procedures\" element";
            throw new VoltCompilerException(msg);
        }

        // shutdown and make a new hsqldb
        m_hsql = HSQLInterface.loadHsqldb();

        // Actually parse and handle all the programs
        for (final String programName : programs) {
            m_catalog.execute("add " + db.getPath() + " programs " + programName);
        }

        // Actually parse and handle all the DDL
        final DDLCompiler ddlcompiler = new DDLCompiler(this, m_hsql);

        for (final String schemaPath : schemas) {
            File schemaFile = null;

            if (schemaPath.contains(".jar!")) {
                String ddlText = null;
                try {
                    ddlText = JarReader.readFileFromJarfile(schemaPath);
                } catch (final Exception e) {
                    throw new VoltCompilerException(e);
                }
                schemaFile = VoltProjectBuilder.writeStringToTempFile(ddlText);
            }
            else {
                schemaFile = new File(schemaPath);
            }

            if (!schemaFile.isAbsolute()) {
                // Resolve schemaPath relative to the database definition xml file
                schemaFile = new File(new File(m_projectFileURL).getParent(), schemaPath);
            }

            // add the file object's path to the list of files for the jar
            m_ddlFilePaths.put(schemaFile.getName(), schemaFile.getPath());

            ddlcompiler.loadSchema(schemaFile.getAbsolutePath());
        }
        ddlcompiler.compileToCatalog(m_catalog, db);

        // Actually parse and handle all the partitions
        // this needs to happen before procedures are compiled
        msg = "In database \"" + databaseName + "\", ";
        final CatalogMap<Table> tables = db.getTables();
        for (final String[] partition : partitions) {
            final String tableName = partition[0];
            final String colName = partition[1];
            final Table t = tables.getIgnoreCase(tableName);
            if (t == null) {
                msg += "\"partition\" element has unknown \"table\" attribute '" + tableName + "'";
                throw new VoltCompilerException(msg);
            }
            final Column c = t.getColumns().getIgnoreCase(colName);
            // make sure the column exists
            if (c == null) {
                msg += "\"partition\" element has unknown \"column\" attribute '" + colName + "'";
                throw new VoltCompilerException(msg);
            }
            // make sure the column is marked not-nullable
            if (c.getNullable() == true) {
                msg += "Partition column '" + tableName + "." + colName + "' is nullable. " +
                    "Partition columns must be constrained \"NOT NULL\".";
                throw new VoltCompilerException(msg);
            }
            t.setPartitioncolumn(c);
            t.setIsreplicated(false);

            // Set the destination tables of associated views non-replicated.
            // If a view's source table is replicated, then a full scan of the
            // associated view is singled-sited. If the source is partitioned,
            // a full scan of the view must be distributed.
            final CatalogMap<MaterializedViewInfo> views = t.getViews();
            for (final MaterializedViewInfo mvi : views) {
                mvi.getDest().setIsreplicated(false);
            }
        }

        // this should reorder the tables and partitions all alphabetically
        String catData = m_catalog.serialize();
        m_catalog = new Catalog();
        m_catalog.execute(catData);
        db = m_catalog.getClusters().get("cluster").getDatabases().get(databaseName);

        // add database estimates info
        addDatabaseEstimatesInfo(m_estimates, db);
        try {
            addSystemProcsToCatalog(m_catalog, db);
        } catch (final VoltCompilerException ex) {
            throw new RuntimeException(ex);
        }

        // Actually parse and handle all the Procedures
        for (final ProcedureDescriptor procedureDescriptor : procedures) {
            final String procedureName = procedureDescriptor.m_className;
            m_currentFilename = procedureName.substring(procedureName.lastIndexOf('.') + 1);
            m_currentFilename += ".class";
            ProcedureCompiler.compile(this, m_hsql, m_estimates,
                    m_catalog, db, procedureDescriptor);
        }

        // Process and add exports and connectors to the catalog
        if (database.getExports() != null) {
            // currently, only a single connector is allowed
            Connector conn = database.getExports().getConnector();
            compileConnector(conn, db);
        }

        // Add all the class dependencies to the output jar
        for (final Class<?> classDependency : classDependencies) {
            addClassToJar( classDependency, this );
        }

        m_hsql.close();
    }


    static void addDatabaseEstimatesInfo(final DatabaseEstimates estimates, final Database db) {
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
        final ArrayList<String> users = new ArrayList<String>();
        final ArrayList<String> groups = new ArrayList<String>();

        // @users
        if (xmlproc.getUsers() != null) {
            for (String user : xmlproc.getUsers().split(",")) {
                users.add(user);
            }
        }

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
            return new ProcedureDescriptor(users, groups, classattr,
                                           xmlproc.getSql(), partattr);
        }
        else {
            String partattr = xmlproc.getPartitioninfo();
            if (partattr != null) {
                String msg = "Java procedures must specify partition info using " +
                "@ProcInfo annotation in the Java class implementation " +
                "and may not use the @partitioninfo project file procedure attribute.";
                throw new VoltCompilerException(msg);
            }
            return new ProcedureDescriptor(users, groups, classattr);
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

    String[] getPartition(org.voltdb.compiler.projectfile.PartitionsType.Partition xmltable)
    throws VoltCompilerException
    {
        String msg = "";
        final String tableName = xmltable.getTable();
        final String columnName = xmltable.getColumn();

        // where is table and column validity checked?
        if (tableName.length() == 0) {
            msg += "\"partition\" element has empty \"table\" attribute";
            throw new VoltCompilerException(msg);
        }

        if (columnName.length() == 0) {
            msg += "\"partition\" element has empty \"column\" attribute";
            throw new VoltCompilerException(msg);
        }

        final String[] retval = { tableName, columnName };
        return retval;
    }

    /** Read a hashed password from password. */
    private byte[] extractPassword(String password) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (final NoSuchAlgorithmException e) {
            compilerLog.l7dlog(Level.FATAL, LogKeys.compiler_VoltCompiler_NoSuchAlgorithm.name(), e);
            System.exit(-1);
        }
        final byte passwordHash[] = md.digest(md.digest(password.getBytes()));
        return passwordHash;
    }

    void compileConnector(final Connector conn, final Database catdb)
        throws VoltCompilerException
    {
        Destination dest = conn.getDestinations().getDestination();

        // Test the error paths before touching the catalog
        if (conn == null) {
            return;
        }

        if (dest.getAddress() == null || dest.getAddress().length() <= 1) {
            throw new VoltCompilerException
            ("In parsing project file: export destination address" +
            " must be a valid hostname or IP address.");
        }

        // Figure out if the connector is enabled or disabled
        // Export will be disabled if there is no destination.
        boolean adminstate = conn.isEnabled();

        if (!conn.isEnabled()) {
            compilerLog.info("Export configuration is present and is " +
                             "configured to be disabled. Export will be disabled.");
        }

        if (dest == null) {
            compilerLog.info("Export configuration does not include destination. " +
                             "Export will be disabled.");
            // VoltDBEngine.cpp and ELTManager.java rely on this logic
            adminstate = false;
        }

        // Catalog Connector
        // Relying on schema's enforcement of at most 1 connector
        org.voltdb.catalog.Connector catconn = catdb.getConnectors().add("0");
        catconn.setEnabled(adminstate);
        catconn.setLoaderclass(conn.getClazz());

        // Catalog Connector.ConnectorDestinationInfo
        org.voltdb.catalog.ConnectorDestinationInfo catdest = catconn.getDestinfo().add("0");
        catdest.setIpaddr(dest.getAddress());
        catdest.setUsername(dest.getUsername());
        catdest.setPassword(dest.getPassword());

        // Catalog Connector.ConnectorTableInfo
        Integer i = 0;
        if (conn.getTables() != null) {
            for (Tables.Table xmltable : conn.getTables().getTable()) {
                // verify that the table exists in the catalog
                String tablename = xmltable.getName();
                org.voltdb.catalog.Table tableref = catdb.getTables().get(tablename);
                if (tableref == null) {
                    compilerLog.warn("While configuring export, table " + tablename + " was not present in " +
                    "the catalog. Export will be disabled for this table.");
                    continue;
                }

                org.voltdb.catalog.ConnectorTableInfo cattable = catconn.getTableinfo().add(i.toString());
                cattable.setAppendonly(xmltable.isExportonly());
                cattable.setTable(tableref);
                ++i;
            }
        }
    }

    /**
     * Add the system procedures to the catalog.
     */
    void addSystemProcsToCatalog(final Catalog catalog, final Database database) throws VoltCompilerException {
        assert (catalog != null);
        assert(database != null);

        // if adding a new procedure, just add class name to this list.
        final String[] procedures = new String[] {
            "org.voltdb.sysprocs.AdHocSQL",
            "org.voltdb.sysprocs.LastCommittedTransaction",
            "org.voltdb.sysprocs.LoadMultipartitionTable",
            "org.voltdb.sysprocs.Quiesce",
            "org.voltdb.sysprocs.SnapshotSave",
            "org.voltdb.sysprocs.SnapshotRestore",
            "org.voltdb.sysprocs.SnapshotStatus",
            "org.voltdb.sysprocs.SnapshotScan",
            "org.voltdb.sysprocs.SnapshotDelete",
            "org.voltdb.sysprocs.Shutdown",
            "org.voltdb.sysprocs.StartSampler",
            "org.voltdb.sysprocs.Statistics",
            "org.voltdb.sysprocs.SystemInformation",
            "org.voltdb.sysprocs.UpdateLogging"
        };

        for (final String className : procedures) {
            Class<?> procClass = null;
            try {
                procClass = Class.forName(className);
            }
            catch (final ClassNotFoundException e) {
                final String msg = "Cannot load sysproc " + className;
                throw new VoltCompilerException(msg);
            }
            // short name is "@ClassName" without package
            final String[] parts = className.split("\\.");
            final String shortName = "@" + parts[parts.length - 1];

            // add an entry to the catalog
            final Procedure procedure = database.getProcedures().add(shortName);
            procedure.setClassname(className);
            procedure.setSystemproc(true);
            // assume all sysprocs can write
            // (slower, but safer)
            procedure.setReadonly(false);
            procedure.setHasjava(true);

            final ProcInfo info = procClass.getAnnotation(ProcInfo.class);
            if (info == null) {
                throw new VoltCompilerException("Sysproc " + shortName + " is missing annotation.");
            }
            procedure.setSinglepartition(info.singlePartition());

            // Stored procedure sysproc classes are present in VoltDB.jar
            // and not duplicated in the catalog. This was decided
            // arbitrarily - no one had a strong opinion.
            //
            // VoltCompiler.addClassToJar(procClass, compiler);
        }
    }

    public static void main(final String[] args) {
        // Parse arguments
        if (args.length != 5) {
            System.err.println("VoltCompiler [project file] [hosts] [sites per host] [leader IP] [output JAR]");
            System.exit(1);
        }
        final String projectPath = args[0];
        final int hostCount = Integer.parseInt(args[1]);
        final int siteCount = Integer.parseInt(args[2]);
        final String leaderAddress = args[3];
        final String outputJar = args[4];

        // Compile and exit with error code if we failed
        final ClusterConfig cluster_config =
            new ClusterConfig(hostCount, siteCount, 0, leaderAddress);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 outputJar, System.out, null);
        if (!success) {
            System.exit(-1);
        }
    }

    // this needs to be reset in the main compile func
    private static final HashSet<Class<?>> cachedAddedClasses = new HashSet<Class<?>>();

    public static final void addClassToJar(final Class<?> cls, final VoltCompiler compiler)
    throws VoltCompiler.VoltCompilerException {

        if (cachedAddedClasses.contains(cls)) {
            return;
        } else {
            cachedAddedClasses.add(cls);
        }

        for (final Class<?> nested : cls.getDeclaredClasses()) {
            addClassToJar(nested, compiler);
        }

        String packagePath = cls.getName();
        packagePath = packagePath.replace('.', '/');
        packagePath += ".class";

        String realName = cls.getName();
        realName = realName.substring(realName.lastIndexOf('.') + 1);
        realName += ".class";

        final URL absolutePath = cls.getResource(realName);
        File file = null;

        InputStream fis = null;
        int fileSize = 0;
        try {
            file =
                new File(URLDecoder.decode(absolutePath.getFile(), "UTF-8"));
            fis = new FileInputStream(file);
            assert(file.canRead());
            assert(file.isFile());
            fileSize = (int) file.length();
        } catch (final FileNotFoundException e) {
            try {
                final String contents = JarReader.readFileFromJarfile(absolutePath.getPath());
                fis = new StringInputStream(contents);
                fileSize = contents.length();
            }
            catch (final Exception e2) {
                final String msg = "Unable to locate classfile for " + realName;
                throw compiler.new VoltCompilerException(msg);
            }
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        assert(fileSize > 0);
        int readSize = 0;

        final byte[] fileBytes = new byte[fileSize];

        try {
            while (readSize < fileSize) {
                readSize = fis.read(fileBytes, readSize, fileSize - readSize);
            }
        } catch (final IOException e) {
            final String msg = "Unable to read (or completely read) classfile for " + realName;
            throw compiler.new VoltCompilerException(msg);
        }

        compiler.addEntryToJarOutput(packagePath, fileBytes);
    }
}
