package org.voltdb.exportclient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.voltdb.client.ConnectionUtil;
import org.voltdb.logging.VoltLogger;


/** Connect to a server data feed */
class ExportClientDataConnection implements Runnable {
    static final VoltLogger LOG = new VoltLogger("ExportClient");
    private final String m_advertisement;
    private final InetSocketAddress m_server;

    public ExportClientDataConnection(InetSocketAddress server, String nextAdvertisement)
    {
        m_advertisement = nextAdvertisement;
        m_server = server;
    }

    @Override
    public void run()
    {
        SocketChannel socket = null;
        long totalBytes = 0;
        int bytesRead = 0;

        LOG.info("Retrieving data for advertisement: " + m_advertisement);

        try {
            Object[] cxndata = ConnectionUtil.getAuthenticatedExportDataConnection(
                m_advertisement,
                m_server.getHostName(),
                null,
                null,
                m_server.getPort());
            socket = (SocketChannel) cxndata[0];
            socket.configureBlocking(true);

            ByteBuffer buf = ByteBuffer.allocate(4096);
            do {
                bytesRead = socket.read(buf);
                buf.flip();
                totalBytes += bytesRead;
                LOG.info("Advertisement " + m_advertisement +
                    " read " + bytesRead + " bytes. Total read:"+ totalBytes/(1024*1024) + " MB");
            } while(bytesRead > 0);
        }
        catch (IOException e) {
            LOG.error(e);
        }
        finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            }
            catch (IOException e) {
                LOG.error(e);
            }
        }
    }
}
