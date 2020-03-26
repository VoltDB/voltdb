/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.servlet.ServletContextHandler;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;

/**
 * This listener handles requests for server status. It is similar
 * in a broad sense to HTTPAdminListener, but is separate since
 * it needs to be started early in server initialization. It is
 * also somewhat simpler.
 *
 * The listener is based on Jetty 9.4; the API documentation is at
 * https://www.eclipse.org/jetty/documentation/current/embedding-jetty.html
 */
public class StatusListener {

    private static final VoltLogger m_log = new VoltLogger("HOST");

    private static final int POOLSIZE = Integer.getInteger("STATUS_THREAD_POOL_SIZE", 10);
    private static final int QUEUELIM = POOLSIZE + 6;
    private static final int CONNTMO = Integer.getInteger("STATUS_CONNECTION_TIMEOUT_SECONDS", 30) * 1000;
    private static final int REQTMO = Integer.getInteger("STATUS_REQUEST_TIMEOUT_SECONDS", 15) * 1000;
    private static final int MAXQUERY = 256;
    private static final int MAXKEYS = 2;

    private final String m_resolvedIntf;
    private final String m_publicIntf;

    private Server m_server;
    private ServerConnector m_connector;
    private String m_hostHeader;
    private static StatusListener singleton;
    private static final Object lock = new Object();

    /**
     * Status listener:
     * @param intf  interface on which to listen for connections (null for any)
     * @param port  TCP port number (zero for automatic)
     * @param publicIntf  address to be returned in 'Host' header (if different)
     */
    public StatusListener(String intf, int port, String publicIntf) {
        m_resolvedIntf = resolveInterface(intf, "");
        m_publicIntf = resolveInterface(publicIntf, m_resolvedIntf);
        initServer(m_resolvedIntf, port);
   }

    private void initServer(String intf, int portReq) {
        QueuedThreadPool qtp = null;
        Server server = null;
        ServerConnector connector = null;

        try {
            qtp = new QueuedThreadPool(POOLSIZE, 0, REQTMO, new ArrayBlockingQueue<>(QUEUELIM));
            qtp.setName("status-thread-pool");
            server = new Server(qtp);
            connector = initConnector(server, intf, portReq);
            server.addConnector(connector);
            ServletContextHandler ctxHandler = new ServletContextHandler();
            ctxHandler.setContextPath("/");
            ctxHandler.setMaxFormContentSize(MAXQUERY);
            ctxHandler.setMaxFormKeys(MAXKEYS);
            ctxHandler.addServlet(StatusServlet.class, "/status").setAsyncSupported(true);
            server.setHandler(ctxHandler);
        }

        catch (Exception ex) {
            logError("StatusListener: initialization failure: %s", ex);
            try { connector.close(); } catch (Exception e2) { }
            try { server.destroy(); } catch (Exception e2) { }
            try { qtp.join(); } catch (Exception e2) { }
            throw new RuntimeException("Failed to initialize status listener", ex);
        }

        m_server = server;
        m_connector = connector;
    }

    private static ServerConnector initConnector(Server server, String intf, int portReq) {
        int portLo = portReq, portHi = portReq;
        if (portReq == 0) {
            portLo = VoltDB.DEFAULT_STATUS_PORT;
            portHi = portLo + 99; // a reasonable range to try
        }
        for (int port=portLo; port<=portHi; port++) {
            try {
                ServerConnector connector = new ServerConnector(server);
                connector.setHost(intf);
                connector.setPort(port);
                connector.setName("status-connector");
                connector.setIdleTimeout(CONNTMO);
                connector.open();
                return connector;
            }
            catch (IOException ex) {
                logDebug("Unable to open port %s: %s", port, ex);
            }
        }
        String fail;
        if (portLo == portHi) {
            fail = String.format("Unable to open port %s", portLo);
        }
        else {
            fail = String.format("Unable to open any port in range %s to %s", portLo, portHi);
        }
        throw new RuntimeException(fail);
    }

    public String getListenInterface() {
        return m_connector != null ? m_connector.getHost() : "";
    }

    public int getAssignedPort() {
        return m_connector != null ? m_connector.getLocalPort() : -1;
    }

    public static StatusListener instance() {
        return singleton;
    }

    public void start() {
        synchronized (lock) {
            if (m_server != null) {
                try {
                    singleton = this; // publish for use by servlets
                    logInfo("Starting status listener on %s:%s", getListenInterface(), getAssignedPort());
                    dumpThreadInfo();
                    m_server.start();
                    logInfo("Status listener started on %s:%s", getListenInterface(), getAssignedPort());
                }
                catch (Exception ex) {
                    logWarning("StatusListener: unexpected exception from start: %s", ex);
                    dumpThreadInfo();
                    safeStop();
                    singleton = null;
                    throw new RuntimeException("Failed to start status listener", ex);
                }
            }
        }
    }

    public static void shutdown() {
        synchronized (lock) {
            if (singleton != null) {
                logInfo("Shutting down status listener");
                singleton.safeStop();
                singleton = null;
            }
        }
    }

    private void safeStop() { // caller is holding lock
        if (m_server != null) {
            try {
                m_server.stop();
                m_server.join();
            }
            catch (Exception ex) {
                logWarning("StatusListener: unexpected exception from stop/join: %s", ex);
            }
            try {
                m_server.destroy();
            }
            catch (Exception ex) {
                logWarning("StatusListener: unexpected exception from destroy: %s", ex);
            }
            m_server = null;
            m_connector = null;
        }
    }

    private void dumpThreadInfo() {
        try {
            QueuedThreadPool qtp = (QueuedThreadPool) m_server.getThreadPool();
            logInfo("%s: min %d, max %d, reserved %d, idle %d, busy %d",
                    qtp.getName(), qtp.getMinThreads(), qtp.getMaxThreads(),
                    qtp.getReservedThreads(), qtp.getIdleThreads(), qtp.getBusyThreads());
        }
        catch (Exception ex) {
            logInfo("Can't dump thread pool info: %s", ex);
        }
    }

    protected String getHostHeader() {
        if (m_hostHeader == null) {
            String intf;
            if (!m_publicIntf.isEmpty()) {
                intf = m_publicIntf;
            } else {
                intf = getLocalAddress().getHostAddress();
                logInfo("Using %s for host header", intf);
            }
            m_hostHeader = intf + ':' + m_connector.getLocalPort();
        }
        return m_hostHeader;
    }

    private static InetAddress getLocalAddress() {
        InetAddress addr = null;
        try {
            String meta = VoltDB.instance().getLocalMetadata();
            if (meta != null && !meta.isEmpty()) {
                JSONObject jsObj = new JSONObject(meta);
                JSONArray ifList = jsObj.getJSONArray("interfaces");
                addr = InetAddress.getByName(ifList.getString(0)); // external interface
            }
        }
        catch (Exception ex) {
            logWarning("Failed to get HTTP interface information: %s", ex);
        }
        if (addr == null) {
            addr = CoreUtils.getLocalAddress();
        }
        return addr;
    }

    private static String resolveInterface(String intf, String defIntf) {
        String temp = (intf == null ? "" : intf.trim());
        return temp.isEmpty() ? defIntf : temp;
    }

    private static void doLog(Level level, String str, Object[] args) {
        if (args.length != 0) {
            str = String.format(str, args);
        }
        m_log.log(level, str, null);
    }

    private static void logError(String str, Object... args) {
        doLog(Level.ERROR, str, args);
    }

    private static void logWarning(String str, Object... args) {
        doLog(Level.WARN, str, args);
    }

    private static void logInfo(String str, Object... args) {
        doLog(Level.INFO, str, args);
    }

    private static void logDebug(String str, Object... args) {
        doLog(Level.DEBUG, str, args);
    }
}
