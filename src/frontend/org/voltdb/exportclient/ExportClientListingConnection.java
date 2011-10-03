package org.voltdb.exportclient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltdb.client.ConnectionUtil;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.logging.VoltLogger;

/** Connect to a server and publish a list of available data channels */
class ExportClientListingConnection implements Runnable {
    static final VoltLogger LOG = new VoltLogger("ExportClient");
    final InetSocketAddress m_server;
    final LinkedBlockingQueue<Object[]> m_results;

    ExportClientListingConnection(InetSocketAddress server, LinkedBlockingQueue<Object[]> results) {
        m_server = server;
        m_results = results;
    }

    @Override
    public void run() {
        LOG.info("Retrieving advertisments from " + m_server);
        SocketChannel socket = null;
        LinkedList<String> advertisements = new LinkedList<String>();
        try {
            // connect
            Object[] cxndata = ConnectionUtil.getAuthenticatedExportListingConnection(
                m_server.getHostName(),
                "username",
                null,
                m_server.getPort());
            socket = (SocketChannel) cxndata[0];
            socket.configureBlocking(true);

            // poll for advertisements
            ExportProtoMessage m = new ExportProtoMessage(0,0,null);
            m.poll();
            socket.write(m.toBuffer());

            // read the advertisement count
            ByteBuffer stringLength = ByteBuffer.allocate(4);
            int read = socket.read(stringLength);
            if (read < 0) {
                LOG.error("Failed to read advertisement count from: " + m_server);
                return;
            }
            if (read != 4) {
                LOG.error("Invalid read reading advertisements from: " + m_server);
                return;
            }
            stringLength.flip();
            int count = stringLength.getInt();

            for (int i=0; i < count; i++) {
                // string length
                read = socket.read(stringLength);
                if (read < 0) {
                    LOG.info("Retrieved " + advertisements.size() + " advertisements");
                    break;
                }
                if (read != 4) {
                    LOG.error("Invalid read reading advertisements");
                    break;
                }
                // actual string
                stringLength.flip();
                int length = stringLength.getInt();
                ByteBuffer ad = ByteBuffer.allocate(length);
                read = socket.read(ad);
                if (read < 0) {
                    LOG.error("Invalid read reading advertisements");
                    break;
                }
                ad.flip();
                byte[] strbytes = new byte[length];
                ad.get(strbytes);
                advertisements.add(new String(strbytes, "UTF-8"));
            }
            LOG.info("Found " + advertisements.size() + " advertisements from " + m_server);
            for (String a : advertisements) {
                m_results.add(new Object[] {m_server, a});
            }
        } catch(IOException e) {
            LOG.error(e);
        } finally {
            try {
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                LOG.error(e);
            }
        }
    }
}