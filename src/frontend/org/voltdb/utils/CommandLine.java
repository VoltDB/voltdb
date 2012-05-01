/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
package org.voltdb.utils;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.ReplicationRole;
import org.voltdb.VoltDB;

// VoltDB.Configuration represents all of the VoltDB command line parameters.
// Extend that to include test-only parameters, the JVM parameters
// and a serialization function that produces a legitimate command line.
public class CommandLine extends VoltDB.Configuration
{
    // Copy ctor.
    public CommandLine makeCopy() {
        CommandLine cl = new CommandLine();
        // first copy the base class fields
        cl.m_ipcPorts.addAll(m_ipcPorts);
        cl.m_backend = m_backend;
        cl.m_leader = m_leader;
        cl.m_pathToCatalog = m_pathToCatalog;
        cl.m_pathToDeployment = m_pathToDeployment;
        cl.m_pathToLicense = m_pathToLicense;
        cl.m_noLoadLibVOLTDB = m_noLoadLibVOLTDB;
        cl.m_zkInterface = m_zkInterface;
        cl.m_port = m_port;
        cl.m_adminPort = m_adminPort;
        cl.m_internalPort = m_internalPort;
        cl.m_externalInterface = m_externalInterface;
        cl.m_internalInterface = m_internalInterface;
        cl.m_drAgentPortStart = m_drAgentPortStart;
        cl.m_rejoinToHostAndPort = m_rejoinToHostAndPort;
        cl.m_httpPort = m_httpPort;
        // final in baseclass: cl.m_isEnterprise = m_isEnterprise;
        cl.m_deadHostTimeoutMS = m_deadHostTimeoutMS;
        cl.m_startAction = m_startAction;
        cl.m_startMode = m_startMode;
        cl.m_replicationRole = m_replicationRole;
        cl.m_selectedRejoinInterface = m_selectedRejoinInterface;
        cl.m_quietAdhoc = m_quietAdhoc;
        // final in baseclass: cl.m_commitLogDir = new File("/tmp");
        cl.m_timestampTestingSalt = m_timestampTestingSalt;
        cl.m_isRejoinTest = m_isRejoinTest;
        cl.m_leaderPort = m_leaderPort;

        // second, copy the derived class fields
        cl.includeTestOpts = includeTestOpts;
        cl.debugPort = debugPort;
        cl.ipcPortList = ipcPortList;
        cl.zkport = zkport;
        cl.buildDir = buildDir;
        cl.java_library_path = java_library_path;
        cl.log4j = log4j;
        cl.voltFilePrefix = voltFilePrefix;
        cl.initialHeap = initialHeap;
        cl.maxHeap = maxHeap;
        cl.classPath = classPath;
        cl.javaExecutable = javaExecutable;
        cl.jmxPort = jmxPort;
        cl.jmxHost = jmxHost;

        return cl;
    }

    // PLEASE NOTE The field naming convention: VoltDB.Configuration
    // fields start with "m_". CommandLine fields do not have a
    // prefix. This helps avoid collisions given the raw number
    // of fields at work. In some cases, the VoltDB.Configuration
    // setting is set (for the m_hasLocalServer case) and a CommandLine
    // field is set as well (for the process builder case).

    boolean includeTestOpts = false;
    public CommandLine addTestOptions(boolean addEm)
    {
        includeTestOpts = addEm;
        return this;
    }

    public CommandLine port(int port) {
        m_port = port;
        return this;
    }
    public int port() {
        return m_port;
    }

    public int leaderPort() {
        return m_leaderPort;
    }

    public void leaderPort(int leaderPort) {
        m_leaderPort = leaderPort;
    }

    public int internalPort() {
        return m_internalPort;
    }

    public CommandLine internalPort(int internalPort) {
        m_internalPort = internalPort;
        return this;
    }

    public CommandLine adminPort(int adminPort) {
        m_adminPort = adminPort;
        return this;
    }

    public CommandLine startCommand(String command)
    {
        String upcmd = command.toUpperCase();
        VoltDB.START_ACTION action = VoltDB.START_ACTION.START;
        try {
            action = VoltDB.START_ACTION.valueOf(upcmd);
        }
        catch (IllegalArgumentException iae)
        {
            // command wasn't a valid enum type;  default to START and warn
            // the user
            hostLog.warn("Unknown start command: " + command +
                         ".  CommandLine will default to START");
        }
        m_startAction = action;
        return this;
    }

    public CommandLine rejoinTest(boolean rejoinTest) {
        m_isRejoinTest = rejoinTest;
        return this;
    }

    public CommandLine isReplica(boolean isReplica)
    {
        if (isReplica)
        {
            m_replicationRole = ReplicationRole.REPLICA;
        }
        else
        {
            m_replicationRole = ReplicationRole.NONE;
        }
        return this;
    }

    public CommandLine replicaMode(ReplicationRole replicaMode) {
        m_replicationRole = replicaMode;
        return this;
    }

    public CommandLine leader(String leader)
    {
        m_leader = leader;
        return this;
    }

    public CommandLine rejoinHostAndPort(String rejoinHostAndPort) {
        this.m_rejoinToHostAndPort = rejoinHostAndPort;
        return this;
    }

    public CommandLine timestampSalt(int timestampSalt) {
        m_timestampTestingSalt = timestampSalt;
        return this;
    }

    int debugPort = -1;
    public CommandLine debugPort(int debugPort) {
        this.debugPort = debugPort;
        return this;
    }

    String ipcPortList = "";
    public CommandLine ipcPort(int port) {
        if (!ipcPortList.isEmpty()) {
            ipcPortList += ",";
        }
        ipcPortList += Integer.toString(port);
        m_ipcPorts.add(port);
        return this;
    }

    int zkport = -1;
    public CommandLine zkport(int zkport) {
        this.zkport = zkport;
        m_zkInterface = "127.0.0.1:" + zkport;
        return this;
    }
    public String zkinterface() {
        return m_zkInterface;
    }

    String buildDir = "";
    public CommandLine buildDir(String buildDir) {
        this.buildDir = buildDir;
        return this;
    }
    public String buildDir() {
        return buildDir;
    }

    String java_library_path = "";
    public CommandLine javaLibraryPath(String javaLibraryPath) {
        java_library_path = javaLibraryPath;
        return this;
    }

    String log4j = "";
    public CommandLine log4j(String log4j) {
        this.log4j = log4j;
        return this;
    }

    String voltFilePrefix = "";
    public CommandLine voltFilePrefix(String voltFilePrefix) {
        this.voltFilePrefix = voltFilePrefix;
        return this;
    }

    String initialHeap = "";
    public CommandLine setInitialHeap(int megabytes) {
        initialHeap = "-Xms" + megabytes + "m";
        return this;
    }

    String maxHeap = "-Xmx2048m";
    public CommandLine setMaxHeap(int megabytes) {
        maxHeap = "-Xmx" + megabytes + "m";
        return this;
    }

    String classPath = "";
    public CommandLine classPath(String classPath) {
        this.classPath = classPath;
        return this;
    }

    public CommandLine jarFileName(String jarFileName) {
        m_pathToCatalog = jarFileName;
        return this;
    }
    public String jarFileName() {
        return m_pathToCatalog;
    }

    public CommandLine target(BackendTarget target) {
        m_backend = target;
        m_noLoadLibVOLTDB = (target == BackendTarget.HSQLDB_BACKEND);
        return this;
    }
    public BackendTarget target() {
        return m_backend;
    }

    public CommandLine pathToDeployment(String pathToDeployment) {
        m_pathToDeployment = pathToDeployment;
        return this;
    }
    public String pathToDeployment() {
        return m_pathToDeployment;
    }

    public CommandLine pathToLicense(String pathToLicense) {
        m_pathToLicense = pathToLicense;
        return this;
    }
    public String pathToLicense() {
        return m_pathToLicense;
    }

    public CommandLine drAgentStartPort(int portStart) {
        m_drAgentPortStart = portStart;
        return this;
    }
    public int drAgentStartPort() {
        return m_drAgentPortStart;
    }

    String javaExecutable = "java";
    public CommandLine javaExecutable(String javaExecutable)
    {
        this.javaExecutable = javaExecutable;
        return this;
    }

    int jmxPort = 9090;
    public CommandLine jmxPort(int jmxPort)
    {
        this.jmxPort = jmxPort;
        return this;
    }

    String jmxHost = "127.0.0.1";
    public CommandLine jmxHost(String jmxHost)
    {
        this.jmxHost = jmxHost;
        return this;
    }

    public CommandLine internalInterface(String internalInterface)
    {
        m_internalInterface = internalInterface;
        return this;
    }

    public CommandLine externalInterface(String externalInterface)
    {
        m_externalInterface = externalInterface;
        return this;
    }

    public void dumpToFile(String filename) {
        try {
            FileWriter out = new FileWriter(filename);
            List<String> lns = createCommandLine();
            for (String l : lns) {
                out.write(l.toCharArray());
                out.write('\n');
            }
            out.flush();
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        List<String> lns = createCommandLine();
        for (String l : lns)
        {
            sb.append(l).append(" ");
        }
        return sb.toString();
    }


    // Return a command line list compatible with ProcessBuilder.command()
    public List<String> createCommandLine() {
        List<String> cmdline = new ArrayList<String>(50);
        cmdline.add(javaExecutable);
        cmdline.add("-XX:-ReduceInitialCardMarks");
        cmdline.add("-XX:+HeapDumpOnOutOfMemoryError");
        cmdline.add("-Djava.library.path=" + java_library_path);
        cmdline.add("-Dlog4j.configuration=" + log4j);
        cmdline.add(maxHeap);
        cmdline.add("-classpath"); cmdline.add(classPath);

        if (includeTestOpts)
        {
            cmdline.add("-DLOG_SEGMENT_SIZE=8");
            cmdline.add("-DVoltFilePrefix=" + voltFilePrefix);
            cmdline.add("-ea");
            cmdline.add("-XX:MaxDirectMemorySize=2g");
        }
        else
        {
            cmdline.add("-server");
            cmdline.add("-XX:HeapDumpPath=/tmp");
            cmdline.add(initialHeap);
        }

        if (m_isEnterprise)
        {
            cmdline.add("-Dvolt.rmi.agent.port=" + jmxPort);
            cmdline.add("-Dvolt.rmi.server.hostname=" + jmxHost);
        }

        if (debugPort > -1) {
            cmdline.add("-Xdebug");
            cmdline.add("-agentlib:jdwp=transport=dt_socket,address=" + debugPort + ",server=y,suspend=n");
        }

        //
        // VOLTDB main() parameters
        //
        cmdline.add("org.voltdb.VoltDB");

        if (m_isEnterprise) {
            cmdline.add("license"); cmdline.add(m_pathToLicense);
        }

        cmdline.add("catalog"); cmdline.add(jarFileName());
        cmdline.add("deployment"); cmdline.add(pathToDeployment());

        // rejoin has no startAction or replication role
        if (m_rejoinToHostAndPort == null || m_rejoinToHostAndPort.isEmpty()) {
            if (m_startAction != null) {
                cmdline.add(m_startAction.toString().toLowerCase());
            }
            cmdline.add("leader"); cmdline.add(m_leader);
            cmdline.add("leaderport"); cmdline.add(Integer.toString(m_leaderPort));

            if (m_replicationRole == ReplicationRole.REPLICA) {
                cmdline.add("replica");
            }
        }
        else {
            cmdline.add("rejoinhost"); cmdline.add(m_rejoinToHostAndPort);
        }

        if (includeTestOpts)
        {
            cmdline.add("timestampsalt"); cmdline.add(Long.toString(m_timestampTestingSalt));
        }

        cmdline.add("port"); cmdline.add(Integer.toString(m_port));
        cmdline.add("internalport"); cmdline.add(Integer.toString(m_internalPort));
        if (m_adminPort != -1)
        {
            cmdline.add("adminport"); cmdline.add(Integer.toString(m_adminPort));
        }
        if (zkport != -1)
        {
            cmdline.add("zkport"); cmdline.add(Integer.toString(zkport));
        }
        if (m_drAgentPortStart != -1)
        {
            cmdline.add("replicationport"); cmdline.add(Integer.toString(m_drAgentPortStart));
        }

        if (target().isIPC) {
            cmdline.add("ipcports"); cmdline.add(ipcPortList);
            cmdline.add("valgrind");
        }

        if (m_internalInterface != null && !m_internalInterface.isEmpty())
        {
            cmdline.add("internalinterface"); cmdline.add(m_internalInterface);
        }

        if (m_internalInterface != null && !m_externalInterface.isEmpty())
        {
            cmdline.add("externalinterface"); cmdline.add(m_externalInterface);
        }

        return cmdline;
    }
}
