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
import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.CatalogBuilder.ProcedureInfo;
import org.voltdb.compiler.CatalogBuilder.RoleInfo;
import org.voltdb.compiler.DeploymentBuilder.UserInfo;

/**
 * Alternate (programmatic) interface to VoltCompiler. Give the class all of
 * the information a user would put in a VoltDB project file and it will go
 * and build the project file and run the compiler on it.
 *
 * It will also create a deployment.xml file and apply its changes to the catalog.
 */
public class VoltProjectBuilder {
    private CatalogBuilder m_cb = new CatalogBuilder();
    private DeploymentBuilder m_db = new DeploymentBuilder();

    private String m_pathToDeployment = null;

    public VoltProjectBuilder setQueryTimeout(int target) {
        m_db.setQueryTimeout(target);
        return this;
    }

    public VoltProjectBuilder setElasticThroughput(int target) {
        m_db.setElasticThroughput(target);
        return this;
    }

    public VoltProjectBuilder setElasticDuration(int target) {
        m_db.setElasticDuration(target);
        return this;
    }

    public void setDeadHostTimeout(int deadHostTimeout) {
        m_db.setDeadHostTimeout(deadHostTimeout);
    }

    public void setUseDDLSchema(boolean useIt) {
        m_db.setUseDDLSchema(useIt);
    }

    public void configureLogging(String internalSnapshotPath, String commandLogPath, Boolean commandLogSync,
            boolean commandLogEnabled, Integer fsyncInterval, Integer maxTxnsBeforeFsync, Integer logSize) {
        m_db.configureLogging(internalSnapshotPath, commandLogPath, commandLogSync, commandLogEnabled, fsyncInterval, maxTxnsBeforeFsync, logSize);
    }

    public void setSnapshotPriority(int priority) {
        m_db.setSnapshotPriority(priority);
    }

    public void addUsers(final UserInfo users[]) {
        m_db.addUsers(users);
    }

    public void addRoles(final RoleInfo roles[]) {
        m_cb.addRoles(roles);
    }

    /**
     * This is test code written by Ryan, even though it was
     * committed by John.
     */
    public void addLiteralSchema(String ddlText) throws IOException {
        m_cb.addLiteralSchema(ddlText);
    }

    public void addStmtProcedure(String name, String sql) {
        m_cb.addStmtProcedure(name, sql);
    }

    public void addStmtProcedure(String name, String sql, String partitionInfo) {
        m_cb.addStmtProcedure(name, sql, partitionInfo);
    }

    public void addProcedures(final Class<?>... procedures) {
        m_cb.addProcedures(procedures);
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
        m_cb.addProcedures(procedures);
    }

    public void addSupplementalClasses(final Class<?>... supplementals) {
        m_cb.addSupplementalClasses(supplementals);
    }

    public void addSupplementalClasses(final Iterable<Class<?>> supplementals) {
        m_cb.addSupplementalClasses(supplementals);
    }

    public void addPartitionInfo(final String tableName, final String partitionColumnName) {
        m_cb.addPartitionInfo(tableName, partitionColumnName);
    }

    public void setHTTPDPort(int port) {
        m_db.setHTTPDPort(port);
    }

    public void setSnapshotSettings(
            String frequency,
            int retain,
            String path,
            String prefix) {
        m_db.setSnapshotSettings(frequency, retain, path, prefix);
    }

    public void setTableAsExportOnly(String name) {
        m_cb.setTableAsExportOnly(name);
    }

    public void setCompilerDebugPrintStream(final PrintStream out) {
        m_cb.setCompilerDebugPrintStream(out);
    }

    public void setMaxTempTableMemory(int max)
    {
        m_db.setMaxTempTableMemory(max);
    }

    public boolean compile(final String jarPath) {
        if (compileToCatalog(jarPath) == null) {
            return false;
        }
        m_pathToDeployment = compileToDeployment(1, 1, 0);
        return true;
    }

    public boolean compile(final String jarPath,
            final int sitesPerHost,
            final int replication) {
        if (compileToCatalog(jarPath) == null) {
            return false;
        }
        m_pathToDeployment = compileToDeployment(sitesPerHost, 1, replication);
        return true;
    }

    public boolean compile(final String jarPath,
            final int sitesPerHost,
            final int hostCount,
            final int replication) {
        if (compileToCatalog(jarPath) == null) {
            return false;
        }
        m_pathToDeployment = compileToDeployment(sitesPerHost, hostCount, replication);
        return true;
    }

    public Catalog compileToCatalogAndSaveDeployment(final String jarPath,
            final int sitesPerHost,
            final int hostCount,
            final int replication) {
        Catalog catalog = compileToCatalog(jarPath);
        if (catalog == null) {
            return null;
        }
        m_pathToDeployment = compileToDeployment(sitesPerHost, hostCount, replication);
        return catalog;
    }

    public VoltProjectBuilder setVoltRoot(String voltRoot) {
        if (voltRoot != null) {
            m_db.setVoltRoot(voltRoot);
        }
        return this;
    }

    public Catalog compileToCatalog(final String jarPath) {
        assert(jarPath != null);
        VoltCompiler compiler = new VoltCompiler();
        if (m_cb.compile(compiler, jarPath)) {
            return compiler.getCatalog();
        } else {
            return null;
        }
    }

    public String compileToDeployment(int sitesPerHost, int hostCount, int replication) {
        assert(hostCount >= 1);
        assert(sitesPerHost >= 1);
        m_db.resetFromVPB(sitesPerHost, hostCount, replication);
        return m_db.writeXMLToTempFile();
    }

    public void setDeploymentPath(String deploymentPath) {
        m_pathToDeployment = deploymentPath;
    }

    public void useCustomAdmin(int adminPort, boolean adminOnStartup)
    {
        m_db.useCustomAdmin(adminPort, adminOnStartup);
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
            System.out.println("path to deployment is " + m_pathToDeployment);
            return m_pathToDeployment;
        }
    }

    public File getPathToVoltRoot() {
        return m_db.getPathToVoltRoot();
    }

    /** Provide a feedback path to monitor the VoltCompiler's plan output via harvestDiagnostics */
    public void enableDiagnostics() {
        // This empty dummy value enables the feature and provides a default fallback return value,
        // but gets replaced in the normal code path.
        m_cb.enableDiagnostics();
    }

    /** Access the VoltCompiler's recent plan output, for diagnostic purposes */
    public List<String> harvestDiagnostics() {
        return m_cb.harvestDiagnostics();
    }

    public DeploymentBuilder depBuilder() {
        return m_db;
    }

    public CatalogBuilder catBuilder() {
        return m_cb;
    }

}
