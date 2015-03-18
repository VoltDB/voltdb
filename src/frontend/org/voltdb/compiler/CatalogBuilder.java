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

import org.jfree.util.Log;
import org.voltdb.ProcInfoData;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.utils.MiscUtils;

/// Alternate (programmatic) interface to VoltCompiler. Use add/set methods to provide
/// all of the information a user would normally put into a VoltDB DDL file and then call
/// a "compile" method to generate the catalog in some form.
/// The programmatic interface allows java classes to be added to the catalog by java reference
/// rather than indirectly via URL.
/// The focus of a CatalogBuilder instance is the configuration of a single consistent catalog.
/// Sometimes, add/set methods can be called between one "compile" method call and another to
/// build simple variations of a catalog with certain details added or overridden, but SOME
/// variations may not be acheivable using this "layering" approach -- using separate CatalogBuilder
/// instances with similar sets of initializers may be the better approach.
/// Historically, as more info has migrated into the VoltDB DDL syntax, an increasing number
/// of the methods on this class have gained "stringly-typed" textual equivalents through
/// addLiteralSchema. These methods may become deprecated over time unless they can provide a
/// useful degree of convenience or early validation.
/// CatalogUtil methods must be used post-compile to integrate deployment details into the catalogs
/// compiled here.
public class CatalogBuilder {
    /// Internal builder state
    PrintStream m_compilerDebugPrintStream = null;
    /// Internal builder state
    private List<String> m_diagnostics;


    final LinkedHashSet<String> m_schemas = new LinkedHashSet<>();
    private StringBuffer transformer = new StringBuffer();
    final Map<String, ProcInfoData> m_procInfoOverrides = new HashMap<>();


    public static final class ProcedureInfo {
        private final RoleInfo m_roles[];
        private final Class<?> m_class;
        private final String m_name;
        private final String m_sql;
        private String m_partitionCol;
        private int m_partitionParameter;

        public ProcedureInfo(final Class<?> cls, final RoleInfo... roles) {
            m_roles = roles;
            m_class = cls;
            m_name = cls.getSimpleName();
            m_sql = null;
            assert(m_name != null);
        }

        public ProcedureInfo(
                String name,
                String sql,
                String partitionCol,
                int partitionParameter,
                RoleInfo... roles) {
            this(name, sql, roles);
            assert(partitionCol != null);
            assert(partitionCol.length() > 0);
            m_partitionCol = partitionCol;
            m_partitionParameter = partitionParameter;
        }

        public ProcedureInfo(String name, String sql, RoleInfo... roles) {
            assert(name != null);
            m_roles = roles;
            m_class = null;
            m_name = name;
            if (sql.endsWith(";")) {
                m_sql = sql;
            }
            else {
                m_sql = sql + ";";
            }
        }

        /**
         * @param procedure
         */
        private void appendToDDL(StringBuffer transformer) {
            if (m_class != null) {
                transformer.append("CREATE PROCEDURE ").append(allowedRoleList())
                .append(" FROM CLASS ").append(m_class.getName()).append(';');
            }
            else {
                assert(m_sql != null);
                transformer.append("CREATE PROCEDURE ").append(m_name).append(allowedRoleList())
                .append(" AS ").append(m_sql);
            }

            if (m_partitionCol != null) {
                String[] token = m_partitionCol.split("\\.");
                transformer.append("PARTITION PROCEDURE " + m_name +
                        " ON TABLE " + token[0] + " COLUMN " + token[1] +
                        " PARAMETER " + m_partitionParameter + ";");
            }
        }

        /// optionally generate the ALLOW clause for a CREATE PROCEDURE statement.
        private String allowedRoleList() {
            StringBuffer roleInfo = new StringBuffer();
            String prefix = " ALLOW ";
            for (RoleInfo role : m_roles) {
                roleInfo.append(prefix).append(role.m_name);
                prefix = ",";
            }
            return roleInfo.toString();
        }

        @Override
        public int hashCode() { return m_name.hashCode(); }

        @Override
        public boolean equals(final Object other) {
            return (other instanceof ProcedureInfo) &&
                    m_name.equals(((ProcedureInfo)other).m_name);
        }
    }

    public static final class RoleInfo {
        private final String m_name;
        private final boolean m_sql;
        private final boolean m_sqlread;
        private final boolean m_admin;
        private final boolean m_defaultproc;
        private final boolean m_defaultprocread;
        private final boolean m_allproc;

        public RoleInfo(String name, boolean sql, boolean sqlread, boolean admin,
                boolean defaultproc, boolean defaultprocread, boolean allproc){
            m_name = name;
            m_sql = sql;
            m_sqlread = sqlread;
            m_admin = admin;
            m_defaultproc = defaultproc;
            m_defaultprocread = defaultprocread;
            m_allproc = allproc;
        }

        public static RoleInfo[] fromTemplate(RoleInfo other, final String... names) {
            RoleInfo[] roles = new RoleInfo[names.length];
            for (int i = 0; i < names.length; ++i) {
                roles[i] = new RoleInfo(names[i], other.m_sql, other.m_sqlread, other.m_admin,
                        other.m_defaultproc, other.m_defaultprocread, other.m_allproc);
            }
            return roles;
        }

        @Override
        public int hashCode() { return m_name.hashCode(); }

        @Override
        public boolean equals(final Object other) {
            return (other instanceof RoleInfo) &&
                    m_name.equals(((RoleInfo)other).m_name);
        }
    }

    public CatalogBuilder() { }

    /// A common case convenience constructor.
    public CatalogBuilder(String ddlText) { addLiteralSchema(ddlText); }

    public CatalogBuilder addRoles(final RoleInfo... roles) {
        for (final RoleInfo info : roles) {
            transformer.append("CREATE ROLE " + info.m_name);
            String prefix = " WITH "; // first time, only
            if (info.m_sql) {
                transformer.append(prefix).append("sql");
                prefix = ",";
            }
            if (info.m_sqlread) {
                transformer.append(prefix).append("sqlread");
                prefix = ",";
            }
            if (info.m_defaultproc) {
                transformer.append(prefix).append("defaultproc");
                prefix = ",";
            }
            if (info.m_admin) {
                transformer.append(prefix).append("admin");
                prefix = ",";
            }
            if (info.m_defaultprocread) {
                transformer.append(prefix).append("defaultprocread");
                prefix = ",";
            }
            if (info.m_allproc) {
                transformer.append(prefix).append("allproc");
                prefix = ",";
            }
            transformer.append(";");
        }
        return this;
    }

    public CatalogBuilder addSchema(final URL schemaURL) {
        assert(schemaURL != null);
        addSchema(schemaURL.getPath());
        return this;
    }

    /**
     * This is test code written by Ryan, even though it was
     * committed by John.
     */
    public CatalogBuilder addLiteralSchema(String ddlText) {
        try {
            File temp = File.createTempFile("literalschema", "sql");
            temp.deleteOnExit();
            MiscUtils.writeStringToFile(temp, ddlText);
            addSchema(URLEncoder.encode(temp.getAbsolutePath(), "UTF-8"));
            return this;
        } catch (IOException e) {
            Log.error("Failed write to temporary schema file.");
            e.printStackTrace();
            throw new RuntimeException(e); // good enough for tests
        }
    }

    /**
     * Add a schema based on a URL.
     * @param schemaURL Schema file URL
     */
    public CatalogBuilder addSchema(String schemaURL) {
        try {
            schemaURL = URLDecoder.decode(schemaURL, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        assert(m_schemas.contains(schemaURL) == false);
        final File schemaFile = new File(schemaURL);
        assert(schemaFile.isDirectory() == false);
        // this check below fails in some valid cases (like when the file is in a jar)
        //assert schemaFile.canRead()
        //    : "can't read file: " + schemaPath;

        m_schemas.add(schemaURL);
        return this;
    }

    public CatalogBuilder addStmtProcedure(String name, String sql) {
        addProcedures(new ProcedureInfo(name, sql));
        return this;
    }

    public CatalogBuilder addStmtProcedure(String name, String sql,
            String partitionCol, int partitionParameter) {
        addProcedures(new ProcedureInfo(name, sql, partitionCol, partitionParameter));
        return this;
    }

    public CatalogBuilder addProcedures(final Class<?>... procedures) {
        for (final Class<?> procedure : procedures) {
            addProcedures(new ProcedureInfo(procedure));
        }
        return this;
    }

    public CatalogBuilder addProcedures(final ProcedureInfo... procedures) {
        // check for duplicates in this batch
        // -- existing procs by the same name are not caught by the builder
        final HashSet<ProcedureInfo> newProcs = new HashSet<>();
        for (final ProcedureInfo procedure : procedures) {
            assert(newProcs.contains(procedure) == false);
            newProcs.add(procedure);

            // add the procs
            procedure.appendToDDL(transformer);
        }
        return this;
    }

    public CatalogBuilder addPartitionInfo(String tableName, String partitionColumnName) {
        transformer.append("PARTITION TABLE " + tableName + " ON COLUMN " + partitionColumnName + ";");
        return this;
    }

    public CatalogBuilder setTableAsExportOnly(String name) {
        assert(name != null);
        transformer.append("EXPORT TABLE " + name + ";");
        return this;
    }

    public CatalogBuilder setTableAsExportOnly(String name, String stream) {
        assert(name != null);
        assert(stream != null);
        transformer.append("EXPORT TABLE " + name + " TO STREAM " + stream + ";");
        return this;
    }

    public CatalogBuilder addDRTables(String... tableNames) {
        for (String drTable : tableNames) {
            transformer.append("DR TABLE " + drTable + ";");
        }
        return this;
    }

    public byte[] compileToBytes() {
        File jarFile = null;
        try {
            jarFile = compileToTempJar();
            if (jarFile == null) {
                return null;
            }
            return MiscUtils.fileToBytes(jarFile);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            if (jarFile != null) {
                jarFile.delete();
            }
        }
    }

    public List<String> compileToErrors() {
        File jarFile = null;
        try {
            jarFile = File.createTempFile("badcatalog", ".jar");
            VoltCompiler compiler = new VoltCompiler();
            if (compile(compiler, jarFile.getAbsolutePath())) {
                return null;
            }
            List<String> result = new ArrayList<>();
            for (Feedback fb : compiler.m_errors) {
                result.add(fb.toString());
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (jarFile != null) {
                jarFile.delete();
            }
        }
        return null;
    }

    public File compileToTempJar() {
        File jarFile = null;
        try {
            jarFile = File.createTempFile("catalog", ".jar");
            jarFile.deleteOnExit();
            if (compile(jarFile.getAbsolutePath())) {
                return jarFile;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (jarFile != null) {
            jarFile.delete();
        }
        return null;
    }

    public boolean compile(final String jarPath) {
        VoltCompiler compiler = new VoltCompiler();
        return compile(compiler, jarPath);
    }

    private boolean compile(final VoltCompiler compiler, final String jarPath) {
        assert(jarPath != null);

        // Add the DDL in the transformer to the schema files before compilation
        addLiteralSchema(transformer.toString());
        // Clear the transformer -
        transformer = new StringBuffer();

        String[] schemaPath = m_schemas.toArray(new String[0]);

        compiler.setProcInfoOverrides(m_procInfoOverrides);
        if (m_diagnostics != null) {
            compiler.enableDetailedCapture();
        }

        boolean success = compiler.compileFromDDL(jarPath, schemaPath);

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

    public CatalogBuilder setCompilerDebugPrintStream(final PrintStream out) {
        m_compilerDebugPrintStream = out;
        return this;
    }

    /** Provide a feedback path to monitor the VoltCompiler's plan output via harvestDiagnostics */
    public CatalogBuilder enableDiagnostics() {
        // This empty dummy value enables the feature and provides a default fallback return value,
        // but gets replaced in the normal code path.
        m_diagnostics = new ArrayList<>();
        return this;
    }

    /** Access the VoltCompiler's recent plan output, for diagnostic purposes */
    public List<String> harvestDiagnostics() {
        List<String> result = m_diagnostics;
        m_diagnostics = null;
        return result;
    }

}
