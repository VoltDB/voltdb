/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.voltdb.BackendTarget;
import org.voltdb.ProcInfoData;
import org.voltdb.compiler.deploymentfile.AdminModeType;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.HttpdType.Jsonapi;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType.Snapshot;
import org.voltdb.compiler.deploymentfile.PathEntry;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.PathsType.Voltdbroot;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType.Temptables;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
import org.voltdb.utils.NotImplementedException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

/**
 * Alternate (programmatic) interface to VoltCompiler. Give the class all of
 * the information a user would put in a VoltDB project file and it will go
 * and build the project file and run the compiler on it.
 *
 * It will also create a deployment.xml file and apply its changes to the catalog.
 */
public class VoltProjectBuilder {

    final LinkedHashSet<String> m_schemas = new LinkedHashSet<String>();

    public static final class ProcedureInfo {
        private final String groups[];
        private final Class<?> cls;
        private final String name;
        private final String sql;
        private final String partitionInfo;
        private final String joinOrder;

        public ProcedureInfo(final String groups[], final Class<?> cls) {
            this.groups = groups;
            this.cls = cls;
            this.name = cls.getSimpleName();
            this.sql = null;
            this.joinOrder = null;
            this.partitionInfo = null;
            assert(this.name != null);
        }

        public ProcedureInfo(
                final String groups[],
                final String name,
                final String sql,
                final String partitionInfo) {
            this(groups, name, sql, partitionInfo, null);
        }

        public ProcedureInfo(
                final String groups[],
                final String name,
                final String sql,
                final String partitionInfo,
                final String joinOrder) {
            assert(name != null);
            this.groups = groups;
            this.cls = null;
            this.name = name;
            this.sql = sql;
            this.partitionInfo = partitionInfo;
            this.joinOrder = joinOrder;
            assert(this.name != null);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof ProcedureInfo) {
                final ProcedureInfo oInfo = (ProcedureInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    public static final class UserInfo {
        public final String name;
        public String password;
        private final String groups[];

        public UserInfo (final String name, final String password, final String groups[]){
            this.name = name;
            this.password = password;
            this.groups = groups;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof UserInfo) {
                final UserInfo oInfo = (UserInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    public static final class GroupInfo {
        private final String name;
        private final boolean adhoc;
        private final boolean sysproc;

        public GroupInfo(final String name, final boolean adhoc, final boolean sysproc){
            this.name = name;
            this.adhoc = adhoc;
            this.sysproc = sysproc;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof GroupInfo) {
                final GroupInfo oInfo = (GroupInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    final ArrayList<String> m_exportTables = new ArrayList<String>();

    final LinkedHashSet<UserInfo> m_users = new LinkedHashSet<UserInfo>();
    final LinkedHashSet<GroupInfo> m_groups = new LinkedHashSet<GroupInfo>();
    final LinkedHashSet<ProcedureInfo> m_procedures = new LinkedHashSet<ProcedureInfo>();
    final LinkedHashSet<Class<?>> m_supplementals = new LinkedHashSet<Class<?>>();
    final LinkedHashMap<String, String> m_partitionInfos = new LinkedHashMap<String, String>();

    String m_elloader = null;         // loader package.Classname
    private boolean m_elenabled;      // true if enabled; false if disabled
    List<String> m_elAuthGroups;      // authorized groups

    // zero defaults to first open port >= 8080.
    // negative one means disabled in the deployment file.
    int m_httpdPortNo = -1;
    boolean m_jsonApiEnabled = true;

    BackendTarget m_target = BackendTarget.NATIVE_EE_JNI;
    PrintStream m_compilerDebugPrintStream = null;
    boolean m_securityEnabled = false;
    final Map<String, ProcInfoData> m_procInfoOverrides = new HashMap<String, ProcInfoData>();

    private String m_snapshotPath = null;
    private int m_snapshotRetain = 0;
    private String m_snapshotPrefix = null;
    private String m_snapshotFrequency = null;
    private String m_pathToDeployment = null;
    private String m_voltRootPath = null;

    private boolean m_ppdEnabled = false;
    private String m_ppdPrefix = "none";

    private String m_internalSnapshotPath;
    private String m_commandLogPath;
    private Boolean m_commandLogSync;
    private Boolean m_commandLogEnabled;
    private Integer m_commandLogSize;
    private Integer m_commandLogFsyncInterval;
    private Integer m_commandLogMaxTxnsBeforeFsync;

    private Integer m_snapshotPriority;

    private Integer m_maxTempTableMemory = 100;

    public void configureLogging(String internalSnapshotPath, String commandLogPath, Boolean commandLogSync,
            Boolean commandLogEnabled, Integer fsyncInterval, Integer maxTxnsBeforeFsync, Integer logSize) {
        m_internalSnapshotPath = internalSnapshotPath;
        m_commandLogPath = commandLogPath;
        m_commandLogSync = commandLogSync;
        m_commandLogEnabled = commandLogEnabled;
        m_commandLogFsyncInterval = fsyncInterval;
        m_commandLogMaxTxnsBeforeFsync = maxTxnsBeforeFsync;
        m_commandLogSize = logSize;
    }

    /**
     * Produce all catalogs this project builder knows how to produce.
     * Written to allow BenchmarkController to cause compilation of multiple
     * catalogs for benchmarks that need to update running appplications and
     * consequently need multiple benchmark controlled catalog jars.
     * @param sitesPerHost
     * @param length
     * @param kFactor
     * @param voltRoot  where to put the compiled catalogs
     * @return a list of jar filenames that were compiled. The benchmark will
     * be started using the filename at index 0.
     */
    public String[] compileAllCatalogs(
            int sitesPerHost, int length,
            int kFactor, String voltRoot)
    {
        throw new NotImplementedException("This project builder does not support compileAllCatalogs");
    }


    public void setSnapshotPriority(int priority) {
        m_snapshotPriority = priority;
    }

    public void addAllDefaults() {
        // does nothing in the base class
    }

    public void addUsers(final UserInfo users[]) {
        for (final UserInfo info : users) {
            final boolean added = m_users.add(info);
            if (!added) {
                assert(added);
            }
        }
    }

    public void addGroups(final GroupInfo groups[]) {
        for (final GroupInfo info : groups) {
            final boolean added = m_groups.add(info);
            if (!added) {
                assert(added);
            }
        }
    }

    public void addSchema(final URL schemaURL) {
        assert(schemaURL != null);
        addSchema(schemaURL.getPath());
    }

    /**
     * This is test code written by Ryan, even though it was
     * committed by John.
     */
    public void addLiteralSchema(String ddlText) throws IOException {
        File temp = File.createTempFile("literalschema", "sql");
        temp.deleteOnExit();
        FileWriter out = new FileWriter(temp);
        out.write(ddlText);
        out.close();
        addSchema(URLEncoder.encode(temp.getAbsolutePath(), "UTF-8"));
    }

    public void addSchema(String schemaPath) {
        try {
            schemaPath = URLDecoder.decode(schemaPath, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        assert(m_schemas.contains(schemaPath) == false);
        final File schemaFile = new File(schemaPath);
        assert(schemaFile != null);
        assert(schemaFile.isDirectory() == false);
        // this check below fails in some valid cases (like when the file is in a jar)
        //assert schemaFile.canRead()
        //    : "can't read file: " + schemaPath;

        m_schemas.add(schemaPath);
    }

    public void addStmtProcedure(String name, String sql) {
        addStmtProcedure(name, sql, null, null);
    }

    public void addStmtProcedure(String name, String sql, String partitionInfo) {
        addStmtProcedure( name, sql, partitionInfo, null);
    }

    public void addStmtProcedure(String name, String sql, String partitionInfo, String joinOrder) {
        addProcedures(new ProcedureInfo(new String[0], name, sql, partitionInfo, joinOrder));
    }

    public void addProcedures(final Class<?>... procedures) {
        final ArrayList<ProcedureInfo> procArray = new ArrayList<ProcedureInfo>();
        for (final Class<?> procedure : procedures)
            procArray.add(new ProcedureInfo(new String[0], procedure));
        addProcedures(procArray);
    }

    /*
     * List of groups permitted to invoke the procedure
     */
    public void addProcedures(final ProcedureInfo... procedures) {
        final ArrayList<ProcedureInfo> procArray = new ArrayList<ProcedureInfo>();
        for (final ProcedureInfo procedure : procedures)
            procArray.add(procedure);
        addProcedures(procArray);
    }

    public void addProcedures(final Iterable<ProcedureInfo> procedures) {
        // check for duplicates and existings
        final HashSet<ProcedureInfo> newProcs = new HashSet<ProcedureInfo>();
        for (final ProcedureInfo procedure : procedures) {
            assert(newProcs.contains(procedure) == false);
            assert(m_procedures.contains(procedure) == false);
            newProcs.add(procedure);
        }

        // add the procs
        for (final ProcedureInfo procedure : procedures) {
            m_procedures.add(procedure);
        }
    }

    public void addSupplementalClasses(final Class<?>... supplementals) {
        final ArrayList<Class<?>> suppArray = new ArrayList<Class<?>>();
        for (final Class<?> supplemental : supplementals)
            suppArray.add(supplemental);
        addSupplementalClasses(suppArray);
    }

    public void addSupplementalClasses(final Iterable<Class<?>> supplementals) {
        // check for duplicates and existings
        final HashSet<Class<?>> newSupps = new HashSet<Class<?>>();
        for (final Class<?> supplemental : supplementals) {
            assert(newSupps.contains(supplemental) == false);
            assert(m_supplementals.contains(supplemental) == false);
            newSupps.add(supplemental);
        }

        // add the supplemental classes
        for (final Class<?> supplemental : supplementals)
            m_supplementals.add(supplemental);
    }

    public void addPartitionInfo(final String tableName, final String partitionColumnName) {
        assert(m_partitionInfos.containsKey(tableName) == false);
        m_partitionInfos.put(tableName, partitionColumnName);
    }

    public void setHTTPDPort(int port) {
        m_httpdPortNo = port;
    }

    public void setJSONAPIEnabled(final boolean enabled) {
        m_jsonApiEnabled = enabled;
    }

    public void setSecurityEnabled(final boolean enabled) {
        m_securityEnabled = enabled;
    }

    public void setSnapshotSettings(
            String frequency,
            int retain,
            String path,
            String prefix) {
        assert(frequency != null);
        assert(prefix != null);
        m_snapshotFrequency = frequency;
        m_snapshotRetain = retain;
        m_snapshotPrefix = prefix;
        m_snapshotPath = path;
    }

    public void setPartitionDetectionSettings(final String snapshotPath, final String ppdPrefix)
    {
        m_ppdEnabled = true;
        m_snapshotPath = snapshotPath;
        m_ppdPrefix = ppdPrefix;
    }

    public void addExport(final String loader, boolean enabled, List<String> groups) {
        m_elloader = loader;
        m_elenabled = enabled;
        m_elAuthGroups = groups;
    }

    public void setTableAsExportOnly(String name) {
        assert(name != null);
        m_exportTables.add(name);
    }

    public void setCompilerDebugPrintStream(final PrintStream out) {
        m_compilerDebugPrintStream = out;
    }

    public void setMaxTempTableMemory(int max)
    {
        m_maxTempTableMemory = max;
    }

    /**
     * Override the procedure annotation with the specified values for a
     * specified procedure.
     *
     * @param procName The name of the procedure to override the annotation.
     * @param info The values to use instead of the annotation.
     */
    public void overrideProcInfoForProcedure(final String procName, final ProcInfoData info) {
        assert(procName != null);
        assert(info != null);
        m_procInfoOverrides.put(procName, info);
    }

    public boolean compile(final String jarPath) {
        return compile(jarPath, 1, 1, 0, null);
    }

    public boolean compile(final String jarPath,
                           final int sitesPerHost,
                           final int replication) {
        return compile(jarPath, sitesPerHost, 1,
                       replication, null);
    }

    public boolean compile(final String jarPath,
                           final int sitesPerHost,
                           final int hostCount,
                           final int replication) {
        return compile(jarPath, sitesPerHost, hostCount,
                       replication, null);
    }

    public boolean compile(final String jarPath,
                           final int sitesPerHost,
                           final int hostCount,
                           final int replication,
                           final String voltRoot) {
        VoltCompiler compiler = new VoltCompiler();
        return compile(compiler, jarPath, sitesPerHost, hostCount,
                       replication, voltRoot,
                       m_ppdEnabled, m_snapshotPath, m_ppdPrefix, false, 0, false);
    }

    public boolean compile(
            final String jarPath, final int sitesPerHost,
            final int hostCount, final int replication,
            final String voltRoot, final boolean ppdEnabled, final String snapshotPath, final String ppdPrefix)
    {
        VoltCompiler compiler = new VoltCompiler();
        return compile(compiler, jarPath, sitesPerHost, hostCount, replication,
                       voltRoot, ppdEnabled, snapshotPath, ppdPrefix, false, 0, false);
    }

    public boolean compile(final String jarPath, final int sitesPerHost,
            final int hostCount, final int replication,
            final int adminPort, final boolean adminOnStartup)
    {
        VoltCompiler compiler = new VoltCompiler();
        return compile(compiler, jarPath, sitesPerHost, hostCount, replication,
                       null, m_ppdEnabled, m_snapshotPath, m_ppdPrefix, true, adminPort, adminOnStartup);
    }

    public boolean compile(final VoltCompiler compiler, final String jarPath, final int sitesPerHost,
                           final int hostCount, final int replication,
                           String voltRoot, final boolean ppdEnabled,
                           final String snapshotPath, final String ppdPrefix,
                           final boolean useCustomAdmin, final int adminPort,
                           final boolean adminOnStartup)
    {
        assert(jarPath != null);
        assert(sitesPerHost >= 1);
        assert(hostCount >= 1);

        if (voltRoot == null) {
            String voltRootPath = "/tmp/" + System.getProperty("user.name");
            java.io.File voltRootFile = new java.io.File(voltRootPath);
            if (!voltRootFile.exists()) {
                if (!voltRootFile.mkdir()) {
                    throw new RuntimeException("Unable to create voltdbroot \"" + voltRootPath + "\" for test");
                }
            }
            if (!voltRootFile.isDirectory()) {
                throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not a directory");
            }
            if (!voltRootFile.canRead()) {
                throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not readable");
            }
            if (!voltRootFile.canWrite()) {
                throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not writable");
            }
            if (!voltRootFile.canExecute()) {
                throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not writable");
            }
            voltRoot = voltRootPath;
        }
        m_voltRootPath = voltRoot;

        // this stuff could all be converted to org.voltdb.compiler.projectfile.*
        // jaxb objects and (WE ARE!) marshaled to XML. Just needs some elbow grease.
        // (see the deployment file code below, which has been converted).

        DocumentBuilderFactory docFactory;
        DocumentBuilder docBuilder;
        Document doc;
        try {
            docFactory = DocumentBuilderFactory.newInstance();
            docBuilder = docFactory.newDocumentBuilder();
            doc = docBuilder.newDocument();
        }
        catch (final ParserConfigurationException e) {
            e.printStackTrace();
            return false;
        }

        // <project>
        final Element project = doc.createElement("project");
        doc.appendChild(project);

        // <security>
        final Element security = doc.createElement("security");
        security.setAttribute("enabled", Boolean.valueOf(m_securityEnabled).toString());
        project.appendChild(security);

        // <database>
        final Element database = doc.createElement("database");
        database.setAttribute("name", "database");
        project.appendChild(database);
        buildDatabaseElement(doc, database);

        // boilerplate to write this DOM object to file.
        StreamResult result;
        try {
            final Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            result = new StreamResult(new StringWriter());
            final DOMSource domSource = new DOMSource(doc);
            transformer.transform(domSource, result);
        }
        catch (final TransformerConfigurationException e) {
            e.printStackTrace();
            return false;
        }
        catch (final TransformerFactoryConfigurationError e) {
            e.printStackTrace();
            return false;
        }
        catch (final TransformerException e) {
            e.printStackTrace();
            return false;
        }

        /*String xml = result.getWriter().toString();
        System.out.println(xml);*/

        final File projectFile =
            writeStringToTempFile(result.getWriter().toString());
        final String projectPath = projectFile.getPath();

        boolean success = compiler.compile(projectPath, jarPath, m_compilerDebugPrintStream, m_procInfoOverrides);
        try {
            m_pathToDeployment = writeDeploymentFile(
                    hostCount, sitesPerHost,
                    replication, voltRoot,
                    useCustomAdmin, adminPort, adminOnStartup);
        } catch (Exception e) {
            System.out.println("Failed to create deployment file in testcase.");
            e.printStackTrace();
            System.out.println("hostcount: " + hostCount);
            System.out.println("sitesPerHost: " + sitesPerHost);
            System.out.println("replication: " + replication);
            System.out.println("voltRoot: " + voltRoot);
            System.out.println("ppdEnabled: " + ppdEnabled);
            System.out.println("snapshotPath: " + snapshotPath);
            System.out.println("ppdPrefix: " + ppdPrefix);
            System.out.println("adminEnabled: " + useCustomAdmin);
            System.out.println("adminPort: " + adminPort);
            System.out.println("adminOnStartup: " + adminOnStartup);

            // sufficient to escape and fail test cases?
            throw new RuntimeException(e);
        }

        return success;
    }

    /**
     * After compile() has been called, a deployment file will be written. This method exposes its location so it can be
     * passed to org.voltdb.VoltDB on startup.
     * @return Returns the deployment file location.
     */
    public String getPathToDeployment() {
        if (m_pathToDeployment == null) {
            System.err.println("ERROR: Call compile() before trying to get the deployment path.");
            return null;
        } else {
            return m_pathToDeployment;
        }
    }

    private void buildDatabaseElement(Document doc, final Element database) {

        // /project/database/groups
        final Element groups = doc.createElement("groups");
        database.appendChild(groups);

        // groups/group
        if (m_groups.isEmpty()) {
            final Element group = doc.createElement("group");
            group.setAttribute("name", "default");
            group.setAttribute("sysproc", "true");
            group.setAttribute("adhoc", "true");
            groups.appendChild(group);
        }
        else {
            for (final GroupInfo info : m_groups) {
                final Element group = doc.createElement("group");
                group.setAttribute("name", info.name);
                group.setAttribute("sysproc", info.sysproc ? "true" : "false");
                group.setAttribute("adhoc", info.adhoc ? "true" : "false");
                groups.appendChild(group);
            }
        }

        // /project/database/schemas
        final Element schemas = doc.createElement("schemas");
        database.appendChild(schemas);

        // schemas/schema
        for (final String schemaPath : m_schemas) {
            final Element schema = doc.createElement("schema");
            schema.setAttribute("path", schemaPath);
            schemas.appendChild(schema);
        }

        // /project/database/procedures
        final Element procedures = doc.createElement("procedures");
        database.appendChild(procedures);

        // procedures/procedure
        for (final ProcedureInfo procedure : m_procedures) {
            if (procedure.cls == null)
                continue;
            assert(procedure.sql == null);

            final Element proc = doc.createElement("procedure");
            proc.setAttribute("class", procedure.cls.getName());
            // build up @groups. This attribute should be redesigned
            if (procedure.groups.length > 0) {
                final StringBuilder groupattr = new StringBuilder();
                for (final String group : procedure.groups) {
                    if (groupattr.length() > 0)
                        groupattr.append(",");
                    groupattr.append(group);
                }
                proc.setAttribute("groups", groupattr.toString());
            }
            procedures.appendChild(proc);
        }

        // procedures/procedures (that are stmtprocedures)
        for (final ProcedureInfo procedure : m_procedures) {
            if (procedure.sql == null)
                continue;
            assert(procedure.cls == null);

            final Element proc = doc.createElement("procedure");
            proc.setAttribute("class", procedure.name);
            if (procedure.partitionInfo != null);
                proc.setAttribute("partitioninfo", procedure.partitionInfo);
            // build up @groups. This attribute should be redesigned
            if (procedure.groups.length > 0) {
                final StringBuilder groupattr = new StringBuilder();
                for (final String group : procedure.groups) {
                    if (groupattr.length() > 0)
                        groupattr.append(",");
                    groupattr.append(group);
                }
                proc.setAttribute("groups", groupattr.toString());
            }

            final Element sql = doc.createElement("sql");
            if (procedure.joinOrder != null) {
                sql.setAttribute("joinorder", procedure.joinOrder);
            }
            proc.appendChild(sql);

            final Text sqltext = doc.createTextNode(procedure.sql);
            sql.appendChild(sqltext);

            procedures.appendChild(proc);
        }

        if (m_partitionInfos.size() > 0) {
            // /project/database/partitions
            final Element partitions = doc.createElement("partitions");
            database.appendChild(partitions);

            // partitions/table
            for (final Entry<String, String> partitionInfo : m_partitionInfos.entrySet()) {
                final Element table = doc.createElement("partition");
                table.setAttribute("table", partitionInfo.getKey());
                table.setAttribute("column", partitionInfo.getValue());
                partitions.appendChild(table);
            }
        }

        // /project/database/classdependencies
        final Element classdeps = doc.createElement("classdependencies");
        database.appendChild(classdeps);

        // classdependency
        for (final Class<?> supplemental : m_supplementals) {
            final Element supp= doc.createElement("classdependency");
            supp.setAttribute("class", supplemental.getName());
            classdeps.appendChild(supp);
        }

        // project/database/export
        if (m_elloader != null) {
            final Element export = doc.createElement("export");
            database.appendChild(export);

            // turn list into stupid comma separated attribute list
            String groupsattr = "";
            if (m_elAuthGroups != null) {
                for (String s : m_elAuthGroups) {
                    if (groupsattr.isEmpty()) {
                        groupsattr += s;
                    }
                    else {
                        groupsattr += "," + s;
                    }
                }
                export.setAttribute("groups", groupsattr);
            }

            if (m_exportTables.size() > 0) {
                final Element tables = doc.createElement("tables");
                export.appendChild(tables);

                for (String exportTableName : m_exportTables) {
                    final Element table = doc.createElement("table");
                    table.setAttribute("name", exportTableName);
                    tables.appendChild(table);
                }
            }
        }
    }

    /**
     * Utility method to take a string and put it in a file. This is used by
     * this class to write the project file to a temp file, but it also used
     * by some other tests.
     *
     * @param content The content of the file to create.
     * @return A reference to the file created or null on failure.
     */
    public static File writeStringToTempFile(final String content) {
        File tempFile;
        try {
            tempFile = File.createTempFile("myApp", ".tmp");
            // tempFile.deleteOnExit();

            final FileWriter writer = new FileWriter(tempFile);
            writer.write(content);
            writer.flush();
            writer.close();

            return tempFile;

        } catch (final IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Writes deployment.xml file to a temporary file. It is constructed from the passed parameters and the m_users
     * field.
     * @param hostCount Number of hosts.
     * @param sitesPerHost Sites per host.
     * @param kFactor Replication factor.
     * @return Returns the path the temporary file was written to.
     * @throws IOException
     * @throws JAXBException
     */
    private String writeDeploymentFile(
            int hostCount, int sitesPerHost, int kFactor, String voltRoot,
            boolean useCustomAdmin, int adminPort, boolean adminOnStartup) throws IOException, JAXBException
    {
        org.voltdb.compiler.deploymentfile.ObjectFactory factory =
            new org.voltdb.compiler.deploymentfile.ObjectFactory();

        // <deployment>
        DeploymentType deployment = factory.createDeploymentType();
        JAXBElement<DeploymentType> doc = factory.createDeployment(deployment);

        // <cluster>
        ClusterType cluster = factory.createClusterType();
        deployment.setCluster(cluster);
        cluster.setHostcount(hostCount);
        cluster.setSitesperhost(sitesPerHost);
        cluster.setKfactor(kFactor);

        // <paths>
        PathsType paths = factory.createPathsType();
        deployment.setPaths(paths);
        Voltdbroot voltdbroot = factory.createPathsTypeVoltdbroot();
        paths.setVoltdbroot(voltdbroot);
        voltdbroot.setPath(voltRoot);

        if (m_snapshotPath != null) {
            PathEntry snapshotPathElement = factory.createPathEntry();
            snapshotPathElement.setPath(m_snapshotPath);
            paths.setSnapshots(snapshotPathElement);
        }

        if (m_commandLogPath != null) {
            PathEntry commandLogPathElement = factory.createPathEntry();
            commandLogPathElement.setPath(m_commandLogPath);
            paths.setCommandlog(commandLogPathElement);
        }

        if (m_internalSnapshotPath != null) {
            PathEntry commandLogSnapshotPathElement = factory.createPathEntry();
            commandLogSnapshotPathElement.setPath(m_internalSnapshotPath);
            paths.setCommandlogsnapshot(commandLogSnapshotPathElement);
        }

        if (m_snapshotPrefix != null) {
            SnapshotType snapshot = factory.createSnapshotType();
            deployment.setSnapshot(snapshot);
            snapshot.setFrequency(m_snapshotFrequency);
            snapshot.setPrefix(m_snapshotPrefix);
            snapshot.setRetain(m_snapshotRetain);
        }

        if (m_commandLogSync != null || m_commandLogEnabled != null ||
                m_commandLogFsyncInterval != null || m_commandLogMaxTxnsBeforeFsync != null ||
                m_commandLogSize != null) {
            CommandLogType commandLogType = factory.createCommandLogType();
            if (m_commandLogSync != null) {
                commandLogType.setSynchronous(m_commandLogSync.booleanValue());
            }
            if (m_commandLogEnabled != null) {
                commandLogType.setEnabled(m_commandLogEnabled);
            }
            if (m_commandLogSize != null) {
                commandLogType.setLogsize(m_commandLogSize);
            }
            if (m_commandLogFsyncInterval != null || m_commandLogMaxTxnsBeforeFsync != null) {
                CommandLogType.Frequency frequency = factory.createCommandLogTypeFrequency();
                if (m_commandLogFsyncInterval != null) {
                    frequency.setTime(m_commandLogFsyncInterval);
                }
                if (m_commandLogMaxTxnsBeforeFsync != null) {
                    frequency.setTransactions(m_commandLogMaxTxnsBeforeFsync);
                }
                commandLogType.setFrequency(frequency);
            }
            deployment.setCommandlog(commandLogType);
        }

        // <partition-detection>/<snapshot>
        PartitionDetectionType ppd = factory.createPartitionDetectionType();
        deployment.setPartitionDetection(ppd);
        ppd.setEnabled(m_ppdEnabled);
        Snapshot ppdsnapshot = factory.createPartitionDetectionTypeSnapshot();
        ppd.setSnapshot(ppdsnapshot);
        ppdsnapshot.setPrefix(m_ppdPrefix);

        // <admin-mode>
        // can't be disabled, but only write out the non-default config if
        // requested by a test. otherwise, take the implied defaults (or
        // whatever local cluster overrides on the command line).
        if (useCustomAdmin) {
            AdminModeType admin = factory.createAdminModeType();
            deployment.setAdminMode(admin);
            admin.setPort(adminPort);
            admin.setAdminstartup(adminOnStartup);
        }

        // <systemsettings>
        SystemSettingsType systemSettingType = factory.createSystemSettingsType();
        Temptables temptables = factory.createSystemSettingsTypeTemptables();
        temptables.setMaxsize(m_maxTempTableMemory);
        systemSettingType.setTemptables(temptables);
        if (m_snapshotPriority != null) {
            SystemSettingsType.Snapshot snapshot = factory.createSystemSettingsTypeSnapshot();
            snapshot.setPriority(m_snapshotPriority);
            systemSettingType.setSnapshot(snapshot);
        }
        deployment.setSystemsettings(systemSettingType);

        // <users>
        if (m_users.size() > 0) {
            UsersType users = factory.createUsersType();
            deployment.setUsers(users);

            // <user>
            for (final UserInfo info : m_users) {
                User user = factory.createUsersTypeUser();
                users.getUser().add(user);
                user.setName(info.name);
                user.setPassword(info.password);

                // build up user/@groups.
                if (info.groups.length > 0) {
                    final StringBuilder groups = new StringBuilder();
                    for (final String group : info.groups) {
                        if (groups.length() > 0)
                            groups.append(",");
                        groups.append(group);
                    }
                    user.setGroups(groups.toString());
                }
            }
        }

        // <httpd>. Disabled unless port # is configured by a testcase
        HttpdType httpd = factory.createHttpdType();
        deployment.setHttpd(httpd);
        httpd.setEnabled(m_httpdPortNo != -1);
        httpd.setPort(m_httpdPortNo);
        Jsonapi json = factory.createHttpdTypeJsonapi();
        httpd.setJsonapi(json);
        json.setEnabled(m_jsonApiEnabled);

        // <export>
        ExportType export = factory.createExportType();
        deployment.setExport(export);
        export.setEnabled(m_elenabled);
        if (m_elloader != null) {
            export.setClazz(m_elloader);
        }

        // Have some yummy boilerplate!
        File file = File.createTempFile("myAppDeployment", ".tmp");
        JAXBContext context = JAXBContext.newInstance(DeploymentType.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
                Boolean.TRUE);
        marshaller.marshal(doc, file);
        final String deploymentPath = file.getPath();
        return deploymentPath;
    }


    public File getPathToVoltRoot() {
        return new File(m_voltRootPath);
    }

}
