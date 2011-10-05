package org.voltdb.exportclient;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.voltdb.VoltDB;
import org.voltdb.logging.VoltLogger;


/**
 * Pull and ack export data from a VoltDB cluster.
 */
public class ExportClient {

    // logging target for all export client log4j output
    static final VoltLogger LOG = new VoltLogger("ExportClient");

    // unserviced advertisements (InetSocketAddress, String) pairs
    private final LinkedBlockingQueue<Object[]> m_advertisements =
        new LinkedBlockingQueue<Object[]>();

    // servers, configured and discovered
    private final List<InetSocketAddress> m_servers =
        new LinkedList<InetSocketAddress>();

    // pool of I/O workers
    private final ExecutorService m_workerPool =
        Executors.newFixedThreadPool(4);

    /** Loop forever reading advertisements and processing data channels */
    public void start() {
        try {
            // seed the advertisement pool
            for (InetSocketAddress s : m_servers) {
                m_workerPool.execute(new ExportClientListingConnection(s, m_advertisements));
            }

            while (true) {
                Object[] pair = m_advertisements.poll(30, TimeUnit.SECONDS);
                if (pair == null) {
                    for (InetSocketAddress s : m_servers) {
                        m_workerPool.execute(new ExportClientListingConnection(s, m_advertisements));
                    }
                }
                else {
                    InetSocketAddress socket = (InetSocketAddress) pair[0];
                    String advertisement =  (String) pair[1];
                    m_workerPool.execute(new ExportClientDataConnection(socket, advertisement));
                }
            }
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    /** Add server to server configuration list */
    void addServerInfo(String server, boolean useAdminPort) {
        InetSocketAddress addr = null;
        int defaultPort = useAdminPort ? VoltDB.DEFAULT_ADMIN_PORT : VoltDB.DEFAULT_PORT;
        String[] parts = server.trim().split(":");
        if (parts.length == 1) {
            addr = new InetSocketAddress(parts[0], defaultPort);
        }
        else {
            assert(parts.length == 2);
            int port = Integer.parseInt(parts[1]);
            addr = new InetSocketAddress(parts[0], port);
        }
        m_servers.add(addr);
    }

    /** Read command line configuration and fire up an export client */
    public static void main(String[] args) {
        LOG.info("Starting export client with arguments: " + args.toString());
        ExportClient that = new ExportClient();
        String clusterip = args[0];
        that.addServerInfo(clusterip, false);
        that.start();
    }
}

