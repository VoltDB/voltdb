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

import org.voltdb.ProcInfoData;
import org.voltdb.utils.MiscUtils;
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
public class CatalogBuilder {

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

    public static final class GroupInfo {
        private final String name;
        private final boolean adhoc;
        private final boolean sysproc;
        private final boolean defaultproc;

        public GroupInfo(final String name, final boolean adhoc, final boolean sysproc, final boolean defaultproc){
            this.name = name;
            this.adhoc = adhoc;
            this.sysproc = sysproc;
            this.defaultproc = defaultproc;
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

    final LinkedHashSet<GroupInfo> m_groups = new LinkedHashSet<GroupInfo>();
    final LinkedHashSet<ProcedureInfo> m_procedures = new LinkedHashSet<ProcedureInfo>();
    final LinkedHashSet<Class<?>> m_supplementals = new LinkedHashSet<Class<?>>();
    final LinkedHashMap<String, String> m_partitionInfos = new LinkedHashMap<String, String>();

    List<String> m_elAuthGroups;      // authorized groups

    PrintStream m_compilerDebugPrintStream = null;
    final Map<String, ProcInfoData> m_procInfoOverrides = new HashMap<String, ProcInfoData>();

    private List<String> m_diagnostics;

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

    public void addAllDefaults() {
        // does nothing in the base class
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

    /**
     * Add a schema based on a URL.
     * @param schemaURL Schema file URL
     */
    public void addSchema(String schemaURL) {
        try {
            schemaURL = URLDecoder.decode(schemaURL, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        assert(m_schemas.contains(schemaURL) == false);
        final File schemaFile = new File(schemaURL);
        assert(schemaFile != null);
        assert(schemaFile.isDirectory() == false);
        // this check below fails in some valid cases (like when the file is in a jar)
        //assert schemaFile.canRead()
        //    : "can't read file: " + schemaPath;

        m_schemas.add(schemaURL);
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

    public void setTableAsExportOnly(String name) {
        assert(name != null);
        m_exportTables.add(name);
    }

    public void addExport(List<String> groups) {
        m_elAuthGroups = groups;
    }

    public void setCompilerDebugPrintStream(final PrintStream out) {
        m_compilerDebugPrintStream = out;
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

    public byte[] compileToBytes() {
        try {
            File jarFile = File.createTempFile("catalogasbytes", ".jar");

            if (!compile(new VoltCompiler(), jarFile.getAbsolutePath())) {
                return null;
            }
            return MiscUtils.fileToBytes(jarFile);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean compile(final String jarPath) {
        return compile(new VoltCompiler(), jarPath);
    }

    public boolean compile(final VoltCompiler compiler,
                           final String jarPath)
    {
        assert(jarPath != null);

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
        compiler.setProcInfoOverrides(m_procInfoOverrides);
        if (m_diagnostics != null) {
            compiler.enableDetailedCapture();
        }
        boolean success = compiler.compileWithProjectXML(projectPath, jarPath);
        m_diagnostics = compiler.harvestCapturedDetail();
        if (m_compilerDebugPrintStream != null) {
            if (success) {
                compiler.summarizeSuccess(m_compilerDebugPrintStream, m_compilerDebugPrintStream, jarPath);
            } else {
                compiler.summarizeErrors(m_compilerDebugPrintStream, m_compilerDebugPrintStream);
            }
        }
        return success;
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
            group.setAttribute("defaultproc", "true");
            group.setAttribute("adhoc", "true");
            groups.appendChild(group);
        }
        else {
            for (final GroupInfo info : m_groups) {
                final Element group = doc.createElement("group");
                group.setAttribute("name", info.name);
                group.setAttribute("sysproc", info.sysproc ? "true" : "false");
                group.setAttribute("defaultproc", info.defaultproc ? "true" : "false");
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
        if (m_exportTables.size() > 0) {
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

    /** Provide a feedback path to monitor the VoltCompiler's plan output via harvestDiagnostics */
    public void enableDiagnostics() {
        // This empty dummy value enables the feature and provides a default fallback return value,
        // but gets replaced in the normal code path.
        m_diagnostics = new ArrayList<String>();
    }

    /** Access the VoltCompiler's recent plan output, for diagnostic purposes */
    public List<String> harvestDiagnostics() {
        List<String> result = m_diagnostics;
        m_diagnostics = null;
        return result;
    }

}
