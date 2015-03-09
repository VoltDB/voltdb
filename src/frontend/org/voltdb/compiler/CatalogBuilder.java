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
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.voltdb.ProcInfoData;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.NotImplementedException;

/**
 * Alternate (programmatic) interface to VoltCompiler. Give the class all of
 * the information a user would put in a VoltDB project file and it will go
 * and build the project file and run the compiler on it.
 *
 * It will also create a deployment.xml file and apply its changes to the catalog.
 */
public class CatalogBuilder {

    final LinkedHashSet<String> m_schemas = new LinkedHashSet<String>();
    private StringBuffer transformer = new StringBuffer();

    public static final class ProcedureInfo {
        private final String roles[];
        private final Class<?> cls;
        private final String name;
        private final String sql;
        private final String partitionInfo;
        private final String joinOrder;

        public ProcedureInfo(final String roles[], final Class<?> cls) {
            this.roles = roles;
            this.cls = cls;
            this.name = cls.getSimpleName();
            this.sql = null;
            this.joinOrder = null;
            this.partitionInfo = null;
            assert(this.name != null);
        }

        public ProcedureInfo(
                final String roles[],
                final String name,
                final String sql,
                final String partitionInfo) {
            this(roles, name, sql, partitionInfo, null);
        }

        public ProcedureInfo(
                final String roles[],
                final String name,
                final String sql,
                final String partitionInfo,
                final String joinOrder) {
            assert(name != null);
            this.roles = roles;
            this.cls = null;
            this.name = name;
            if(sql.endsWith(";")) {
                this.sql = sql;
            }
            else {
                this.sql = sql + ";";
            }
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

    final LinkedHashSet<Class<?>> m_supplementals = new LinkedHashSet<Class<?>>();

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
     * List of roles permitted to invoke the procedure
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
            newProcs.add(procedure);
        }

        // add the procs
        for (final ProcedureInfo procedure : procedures) {

            // ALLOW clause in CREATE PROCEDURE stmt
            StringBuffer roleInfo = new StringBuffer();
            if(procedure.roles.length != 0) {
                roleInfo.append(" ALLOW ");
                for(int i = 0; i < procedure.roles.length; i++) {
                    roleInfo.append(procedure.roles[i] + ",");
                }
                int length = roleInfo.length();
                roleInfo.replace(length - 1, length, " ");
            }

            if(procedure.cls != null) {
                transformer.append("CREATE PROCEDURE " + roleInfo.toString() + " FROM CLASS " + procedure.cls.getName() + ";");
            }
            else if(procedure.sql != null) {
                transformer.append("CREATE PROCEDURE " + procedure.name + roleInfo.toString() + " AS " + procedure.sql);
            }

            if(procedure.partitionInfo != null) {
                String[] parameter = procedure.partitionInfo.split(":");
                String[] token = parameter[0].split("\\.");
                String position = "";
                if(Integer.parseInt(parameter[1].trim()) > 0) {
                    position = " PARAMETER " + parameter[1];
                }
                transformer.append("PARTITION PROCEDURE " + procedure.name + " ON TABLE " + token[0] + " COLUMN " + token[1] + position + ";");
            }
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
        transformer.append("PARTITION TABLE " + tableName + " ON COLUMN " + partitionColumnName + ";");
    }

    public void setTableAsExportOnly(String name) {
        assert(name != null);
        transformer.append("Export TABLE " + name + ";");
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

        // Add the DDL in the transformer to the schema files before compilation
        try {
            addLiteralSchema(transformer.toString());
            transformer = new StringBuffer();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        String[] schemaPath = m_schemas.toArray(new String[0]);

        compiler.setProcInfoOverrides(m_procInfoOverrides);
        if (m_diagnostics != null) {
            compiler.enableDetailedCapture();
        }

        boolean success = false;
        try {
            success = compiler.compileFromDDL(jarPath, schemaPath);
        } catch (VoltCompilerException e1) {
            e1.printStackTrace();
            return false;
        }

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
