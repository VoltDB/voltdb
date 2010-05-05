/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.elt;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.voltdb.elt.processors.RawProcessor;
import org.voltdb.messaging.FastDeserializer;


/**
 *   Listen for ELT output and run a provided tuple verifier
 *   against that stream of data. If there are multiple advertised
 *   data sources, each source will be polled to completion in turn.
 *   (Source 1 drained. Source 2 drained.. etc.)
 */

public class ELPoller implements Runnable {

    private final int m_processorPort;
    private final TupleVerifier m_verifier;

    public ELPoller(final TupleVerifier verifer) {
        m_verifier = verifer;
        m_processorPort = RawProcessor.LISTENER_PORT;
    }

    /**
     *  Runs the client's side of the poller protocol.
     */
    private class VerificationStream implements Runnable
    {
        private final SocketChannel m_socket;
        private int m_state = RawProcessor.CLOSED;
        private int m_currentPartitionId = -1;
        private int m_currentTableId = -1;

        private List<ELTProtoMessage.AdvertisedDataSource> m_sources = null;
        boolean m_done = false;

        VerificationStream(final SocketChannel client)
        throws IOException
        {
            m_socket = client;
        }

        void nextSource() throws IOException {
            if (m_sources.size() == 0) {
                m_done = true;
                return;
            }
            ELTProtoMessage.AdvertisedDataSource next = m_sources.remove(0);
            m_currentPartitionId = next.partitionId();;
            m_currentTableId = next.tableId();
            poll();
        }

        int currentPartitionId() {
            return m_currentPartitionId;
        }

        int currentTableId() {
            return m_currentTableId;
        }

        @Override
        public void run()
        {
            try {
                open();
                while (m_done == false) {
                    // blocks
                    final ELTProtoMessage m = nextMessage();
                    System.out.println("Poller: handling message: " + m);

                    // Assert against client-only messages
                    assert (!m.isOpen());
                    assert (!m.isPoll());
                    assert (!m.isAck());

                    if (m.isOpenResponse()) {
                        handleOpenResponse(m);
                    }
                    if (m.isPollResponse()) {
                        handlePollResponse(m);
                    }
                    if (m.isError()) {
                        m_done = true;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void open() throws IOException
        {
            // can send any table/partition pair to open
            // but I hesitate to add a no-argument constructor
            // while getting the interfaces to line up.
            System.out.println("Opening new verification stream connection.");
            ELTProtoMessage m = new ELTProtoMessage(-1, -1);
            m.open();
            ByteBuffer buf = m.toBuffer();
            while (buf.remaining() > 0) {
                m_socket.write(buf);
            }
        }

        private void poll() throws IOException
        {
            System.out.println("Polling for new data.");
            ELTProtoMessage m = new ELTProtoMessage(currentPartitionId(), currentTableId());
            m.poll();
            ByteBuffer buf = m.toBuffer();
            while (buf.remaining() > 0) {
                m_socket.write(buf);
            }
        }

        private void pollAndAck(ELTProtoMessage prev) throws IOException
        {
            System.out.println("Poller: pollAndAck " + prev.getAckOffset());
            ELTProtoMessage next = new ELTProtoMessage(currentPartitionId(), currentTableId());
            next.poll().ack(prev.getAckOffset());
            ByteBuffer buf = next.toBuffer();
            while (buf.remaining() > 0) {
                m_socket.write(buf);
            }
        }

        private ELTProtoMessage nextMessage() throws IOException
        {
            FastDeserializer fds;
            final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            while(lengthBuffer.hasRemaining()) {
                m_socket.read(lengthBuffer);
            }
            lengthBuffer.flip();
            int length = lengthBuffer.getInt();

            ByteBuffer messageBuf = ByteBuffer.allocate(length);
            while (messageBuf.remaining() > 0) {
                m_socket.read(messageBuf);
            }
            messageBuf.flip();
            fds = new FastDeserializer(messageBuf);
            ELTProtoMessage m = ELTProtoMessage.readExternal(fds);
            return m;
        }

        private void handleOpenResponse(ELTProtoMessage m) throws IOException
        {
            if (m_state == RawProcessor.CLOSED) {
                m_state = RawProcessor.CONNECTED;
                m_sources = m.getAdvertisedDataSources();
                System.out.printf("Poller: connected verification stream.");
                nextSource();
            }
            else {
                assert(false);
            }
        }

        private void handlePollResponse(ELTProtoMessage m) throws IOException
        {
            if (m_state == RawProcessor.CONNECTED) {
                // if a poll returns no data, this process is complete.
                if (m.getData().remaining() == 0) {
                    nextSource();
                    return;
                }

                // read the streamblock length prefix.
                int ttllength = m.getData().getInt();
                System.out.println("Poller: data payload bytes: " + ttllength);

                // a stream block prefix of 0 also means empty queue.
                if (ttllength == 0) {
                    nextSource();
                    return;
                }

                // run the verifier until m.getData() is consumed
                while (m.getData().hasRemaining()) {
                    int length = m.getData().getInt();
                    byte[] rowdata = new byte[length];
                    m.getData().get(rowdata, 0, length);
                    m_verifier.verifyRow(length, rowdata);
                    System.out.println("Poller verifier (all rows?) " + m_verifier.allRowsVerified());
                }

                // ack the old block and poll the next.
                pollAndAck(m);
            }
            else {
                assert(false);
            }
        }
    }

    /**
     * Connect to the cluster.
     */
    @Override
    public void run() {
        System.out.println("Starting ELT verification stream.");
        try {
            SocketAddress sockaddr = new InetSocketAddress(InetAddress.getLocalHost(), m_processorPort);
            final SocketChannel socket = SocketChannel.open(sockaddr);
            socket.configureBlocking(true);
            VerificationStream vs  = new VerificationStream(socket);
            vs.run();
        }
        catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
