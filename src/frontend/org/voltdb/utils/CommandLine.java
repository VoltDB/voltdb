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
        cl.m_useWatchdogs = m_useWatchdogs;
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

        // second, copy the derived class fields
        cl.rejoinHost = rejoinHost;
        cl.debugPort = debugPort;
        cl.ipcPortList = ipcPortList;
        cl.zkport = zkport;
        cl.buildDir = buildDir;
        cl.jzmq_dir = jzmq_dir;
        cl.log4j = log4j;
        cl.voltFilePrefix = voltFilePrefix;
        cl.maxHeap = maxHeap;
        cl.classPath = classPath;

        return cl;
    }

    // PLEASE NOTE The field naming convention: VoltDB.Configuration
    // fields start with "m_". CommandLine fields do not have a
    // prefix. This helps avoid collisions given the raw number
    // of fields at work. In some cases, the VoltDB.Configuration
    // setting is set (for the m_hasLocalServer case) and a CommandLine
    // field is set as well (for the process builder case).

    public CommandLine port(int port) {
        m_port = port;
        return this;
    }
    public int port() {
        return m_port;
    }

    public CommandLine adminPort(int adminPort) {
        m_adminPort = adminPort;
        return this;
    }

    public CommandLine startCommand(VoltDB.START_ACTION startCommand) {
        m_startAction = startCommand;
        return this;
    }

    public CommandLine rejoinTest(boolean rejoinTest) {
        m_isRejoinTest = rejoinTest;
        return this;
    }

    public CommandLine replicaMode(ReplicationRole replicaMode) {
        m_replicationRole = replicaMode;
        return this;
    }

    String rejoinHost = "";
    public CommandLine rejoinHost(String rejoinHost) {
        this.rejoinHost = rejoinHost;
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

    String buildDir = "";
    public CommandLine buildDir(String buildDir) {
        this.buildDir = buildDir;
        return this;
    }
    public String buildDir() {
        return buildDir;
    }

    String jzmq_dir = "";
    public CommandLine jzmqDir(String jzmqDir) {
        jzmq_dir = jzmqDir;
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

    String maxHeap = "-Xmx2g";
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


    // Return a command line list compatible with ProcessBuilder.command()
    public List<String> createCommandLine() {
        List<String> cmdline = new ArrayList<String>(50);
        cmdline.add("java");
        cmdline.add("-Djava.library.path=" + buildDir + "/nativelibs" + ":" + jzmq_dir);
        cmdline.add("-Dlog4j.configuration=" + log4j);
        cmdline.add("-DLOG_SEGMENT_SIZE=8");
        cmdline.add("-DVoltFilePrefix=" + voltFilePrefix);
        cmdline.add("-ea");
        cmdline.add("-XX:-ReduceInitialCardMarks");
        cmdline.add("-XX:MaxDirectMemorySize=2g");
        cmdline.add("-Xmx2g");
        cmdline.add("-XX:+HeapDumpOnOutOfMemoryError");
        cmdline.add("-classpath"); cmdline.add(classPath);

        if (debugPort > -1) {
            cmdline.add("-Xdebug");
            cmdline.add("-agentlib:jdwp=transport=dt_socket,address=" + debugPort + ",server=y,suspend=n");
        }

        //
        // VOLTDB main() parameters
        //
        cmdline.add("org.voltdb.VoltDB");

        // rejoin has no startAction
        if (m_startAction != null) {
            cmdline.add(m_startAction.toString().toLowerCase());
        }

        if (m_isEnterprise) {
            cmdline.add("license"); cmdline.add(m_pathToLicense);
        }

        cmdline.add("catalog"); cmdline.add(jarFileName());
        cmdline.add("deployment"); cmdline.add(pathToDeployment());

        if (rejoinHost.isEmpty()) {
            cmdline.add("leader"); cmdline.add("localhost");
        }
        else {
            cmdline.add("rejoinhost"); cmdline.add(rejoinHost);
        }

        if (m_replicationRole == ReplicationRole.REPLICA) {
            cmdline.add("replica");
        }

        cmdline.add("timestampsalt"); cmdline.add(Long.toString(m_timestampTestingSalt));
        cmdline.add("port"); cmdline.add(Integer.toString(m_port));
        cmdline.add("adminport"); cmdline.add(Integer.toString(m_adminPort));
        cmdline.add("zkport"); cmdline.add(Integer.toString(zkport));
        cmdline.add("replicationport"); cmdline.add(Integer.toString(m_drAgentPortStart));

        if (target().isIPC) {
            cmdline.add("ipcports"); cmdline.add(ipcPortList);
            cmdline.add("valgrind");
        }

        return cmdline;
    }
}
