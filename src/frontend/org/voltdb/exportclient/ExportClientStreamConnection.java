package org.voltdb.exportclient;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.voltdb.client.ConnectionUtil;
import org.voltdb.exportclient.ExportClient.CompletionEvent;
import org.voltdb.logging.VoltLogger;


/** Connect to a server data feed */
class ExportClientStreamConnection implements Runnable {
    static final VoltLogger LOG = new VoltLogger("ExportClient");
    private final String m_advertisement;
    private final InetSocketAddress m_server;
    private final CompletionEvent m_onCompletion;

    public ExportClientStreamConnection(InetSocketAddress server,
        String nextAdvertisement,
        CompletionEvent onCompletion)
    {
        m_advertisement = nextAdvertisement;
        m_server = server;
        m_onCompletion = onCompletion;
    }

    @Override
    public void run()
    {
        BufferedInputStream reader;
        SocketChannel socket = null;
        long totalBytes = 0;
        int bytesRead = 0;
        long lastLogged = 0;

        LOG.info("Retrieving data for advertisement: " + m_advertisement);

        try {
            Object[] cxndata = ConnectionUtil.getAuthenticatedExportStreamConnection(
                m_advertisement,
                m_server.getHostName(),
                null,
                null,
                m_server.getPort());
            socket = (SocketChannel) cxndata[0];
            socket.configureBlocking(true);
            reader = new BufferedInputStream(socket.socket().getInputStream());
            ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 2);
            do {
                bytesRead = reader.read(buf.array());
                buf.flip();
                totalBytes += bytesRead;
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Advertisement " + m_advertisement +
                        " read " + bytesRead +
                        " bytes. Total read:"+ totalBytes/(1024*1024) + " MB");
                }
                else if (totalBytes > lastLogged + (1024*1024*5)) {
                    lastLogged = totalBytes;
                    LOG.info("Advertisement " + m_advertisement +
                        " read " + totalBytes + ". Last read: " + bytesRead);
                }
            } while(bytesRead > 0);

            // trigger the ack for this advertisement
            m_onCompletion.run(totalBytes);
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
