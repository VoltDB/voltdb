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
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.voltdb.ProcInfoData;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.utils.MiscUtils;

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
        private final String m_roles[];
        private final Class<?> m_class;
        private final String m_name;
        private final String m_sql;
        private final String m_partitionInfo;

        public ProcedureInfo(final String roles[], final Class<?> cls) {
            m_roles = roles;
            m_class = cls;
            m_name = cls.getSimpleName();
            m_sql = null;
            m_partitionInfo = null;
            assert(m_name != null);
        }

        public ProcedureInfo(
                final String roles[],
                final String name,
                final String sql,
                final String partitionInfo) {
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
            m_partitionInfo = partitionInfo;
            assert(m_name != null);
        }

        @Override
        public int hashCode() {
            return m_name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof ProcedureInfo) {
                final ProcedureInfo oInfo = (ProcedureInfo)o;
                return m_name.equals(oInfo.m_name);
            }
            return false;
        }
    }

    public static final class RoleInfo {
        private final String name;
        private final boolean sql;
        private final boolean sqlread;
        private final boolean admin;
        private final boolean defaultproc;
        private final boolean defaultprocread;
        private final boolean allproc;

        public RoleInfo(final String name, final boolean sql, final boolean sqlread, final boolean admin, final boolean defaultproc, final boolean defaultprocread, final boolean allproc){
            this.name = name;
            this.sql = sql;
            this.sqlread = sqlread;
            this.admin = admin;
            this.defaultproc = defaultproc;
            this.defaultprocread = defaultprocread;
            this.allproc = allproc;
        }

        public static RoleInfo[] fromTemplate(final RoleInfo other, final String... names) {
            RoleInfo[] roles = new RoleInfo[names.length];
            for (int i = 0; i < names.length; ++i) {
                roles[i] = new RoleInfo(names[i], other.sql, other.sqlread, other.admin,
                                other.defaultproc, other.defaultprocread, other.allproc);
            }
            return roles;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof RoleInfo) {
                final RoleInfo oInfo = (RoleInfo)o;
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

    public void addAllDefaults() {
        // does nothing in the base class
    }

    public void addRoles(final RoleInfo roles[]) {
        for (final RoleInfo info : roles) {
            transformer.append("CREATE ROLE " + info.name);
            if(info.sql || info.sqlread || info.defaultproc || info.admin || info.defaultprocread || info.allproc) {
                transformer.append(" WITH ");
                if(info.sql) {
                    transformer.append("sql,");
                }
                if(info.sqlread) {
                    transformer.append("sqlread,");
                }
                if(info.defaultproc) {
                    transformer.append("defaultproc,");
                }
                if(info.admin) {
                    transformer.append("admin,");
                }
                if(info.defaultprocread) {
                    transformer.append("defaultprocread,");
                }
                if(info.allproc) {
                    transformer.append("allproc,");
                }
                transformer.replace(transformer.length() - 1, transformer.length(), ";");
            }
            else {
                transformer.append(";");
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
        addStmtProcedure(name, sql, null);
    }

    public void addStmtProcedure(String name, String sql, String partitionInfo) {
        addProcedures(new ProcedureInfo(new String[0], name, sql, partitionInfo));
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
        addProcedures(Arrays.asList(procedures));
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
            if(procedure.m_roles.length != 0) {
                roleInfo.append(" ALLOW ");
                for(int i = 0; i < procedure.m_roles.length; i++) {
                    roleInfo.append(procedure.m_roles[i] + ",");
                }
                int length = roleInfo.length();
                roleInfo.replace(length - 1, length, " ");
            }

            if(procedure.m_class != null) {
                transformer.append("CREATE PROCEDURE " + roleInfo.toString() + " FROM CLASS " + procedure.m_class.getName() + ";");
            }
            else if(procedure.m_sql != null) {
                transformer.append("CREATE PROCEDURE " + procedure.m_name + roleInfo.toString() + " AS " + procedure.m_sql);
            }

            if(procedure.m_partitionInfo != null) {
                String[] parameter = procedure.m_partitionInfo.split(":");
                String[] token = parameter[0].split("\\.");
                String position = "";
                if(Integer.parseInt(parameter[1].trim()) > 0) {
                    position = " PARAMETER " + parameter[1];
                }
                transformer.append("PARTITION PROCEDURE " + procedure.m_name + " ON TABLE " + token[0] + " COLUMN " + token[1] + position + ";");
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
