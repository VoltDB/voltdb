/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltcore.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.netty.buffer.CompositeByteBuf;

public abstract class VoltProtocolHandler implements InputHandler {
    /** VoltProtocolPorts each have a unique id */
    private static AtomicLong m_globalConnectionCounter = new AtomicLong(0);

    /** The distinct exception class allows better logging of these unexpected errors. */
    class BadMessageLength extends IOException {
        private static final long serialVersionUID = 8547352379044459911L;

        private int badLength;
        private byte[] availBytes;
        private Supplier<byte[]> getBytes;
        public BadMessageLength(String string, int badLength, Supplier<byte[]> getBytes) {
            super(string);
            this.badLength = badLength;
            this.getBytes = getBytes;
        }
        int badLength() {
            return badLength;
        }
        byte[] badMessage() {
            if (availBytes == null) {
                availBytes = getBytes.get();
            }
            return availBytes;
        }
    }

    private static final int badMessageDumpLimit =
        Integer.parseInt(System.getProperty("VOLTDB_BAD_MESSAGE_DUMP_LIMIT", "512"));

    /** serial number of this VoltPort */
    private final long m_connectionId;
    private int m_nextLength;

    public VoltProtocolHandler() {
        m_connectionId = m_globalConnectionCounter.incrementAndGet();
    }

    public static long getNextConnectionId() {
        return m_globalConnectionCounter.incrementAndGet();
    }

    @Override
    public ByteBuffer retrieveNextMessage(final NIOReadStream inputStream) throws BadMessageLength {

        /*
         * Note that access to the read stream is not synchronized. In this application
         * the VoltPort will invoke this input handler to interact with the read stream guaranteeing
         * thread safety. That said the Connection interface does allow other parts of the application
         * access to the read stream.
         */
        ByteBuffer result = null;

        if (m_nextLength == 0 && inputStream.dataAvailable() > (Integer.SIZE/8)) {
            m_nextLength = inputStream.getInt();
            checkMessageLength(inputStream);
        }
        if (m_nextLength > 0 && inputStream.dataAvailable() >= m_nextLength) {
            result = ByteBuffer.allocate(m_nextLength);
            // Copy read buffers to result, move read buffers back to memory pool
            inputStream.getBytes(result.array());
            m_nextLength = 0;
        }
        return result;
    }

    @Override
    public ByteBuffer retrieveNextMessage(CompositeByteBuf inputBB) throws BadMessageLength {
        ByteBuffer result = null;
        if (m_nextLength == 0 && inputBB.readableBytes() > (Integer.SIZE/8)) {
            m_nextLength = inputBB.readInt();
            checkMessageLength(inputBB);
        }

        if (m_nextLength > 0 && inputBB.readableBytes() >= m_nextLength) {
            result = ByteBuffer.allocate(m_nextLength);
            // Copy read buffers to result, move read buffers back to memory pool
            inputBB.readBytes(result);
            m_nextLength = 0;
        }
        return result;
    }

    /*
     * Utilities for detecting and reporting bad message lengths.
     * The error might indicate an oversized message, or it might
     * indicate non-volt garbage sent to the socket.
     */

    private void checkMessageLength(NIOReadStream inputStream) throws BadMessageLength {
        if (m_nextLength <= 0 || m_nextLength > VoltPort.MAX_MESSAGE_LENGTH) {
            Supplier<byte[]> getBytes = () -> {
                int len = Math.min(inputStream.dataAvailable(), badMessageDumpLimit);
                byte[] buff = new byte[len];
                if (len > 0) {
                    inputStream.getBytes(buff);
                }
                return buff;
            };
            throw new BadMessageLength(badMessageReason(), m_nextLength, getBytes);
        }
    }

    private void checkMessageLength(CompositeByteBuf inputBB) throws BadMessageLength {
        if (m_nextLength <= 0 || m_nextLength > VoltPort.MAX_MESSAGE_LENGTH) {
            Supplier<byte[]> getBytes = () -> {
                int len = Math.min(inputBB.readableBytes(), badMessageDumpLimit);
                byte[] buff = new byte[len];
                if (len > 0) {
                    inputBB.readBytes(buff);
                }
                return buff;
            };
            throw new BadMessageLength(badMessageReason(), m_nextLength, getBytes);
        }
    }

    private String badMessageReason() {
        if (m_nextLength <= 0) {
            return "Next message length is " + m_nextLength + " which is less than the minimum length 1";
        }
        else {
            return "Next message length is " + m_nextLength +
                   " which is greater than the hardcoded max of " + VoltPort.MAX_MESSAGE_LENGTH +
                   ". Break up the work into smaller chunks (2 megabytes is reasonable) " +
                   "and send as multiple messages or stored procedure invocations";
        }
    }

    public static String formatBadLengthDump(String caption, BadMessageLength ex) {
        byte[] buf = ex.badMessage();
        String firstLine = String.format("%s, length %d %s\n", caption, buf.length,
                                         buf.length < badMessageDumpLimit ? "" : "(truncated)");
        int bytesPerLine = 32;
        int sbCapacity = firstLine.length() + (buf.length * 3) + (buf.length / bytesPerLine) + 1;
        StringBuffer sb = new StringBuffer(sbCapacity);
        sb.append(firstLine);
        for (int i = 0; i < buf.length; i += bytesPerLine) {
            for (int j = 0; j < bytesPerLine && i+j < buf.length; j++) {
                sb.append(String.format(" %02x", buf[i+j]));
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    @Override
    public void started(Connection c) {
    }

    @Override
    public void starting(Connection c) {
    }

    @Override
    public void stopped(Connection c) {
    }

    @Override
    public void stopping(Connection c) {
    }

    @Override
    public long connectionId() {
        return m_connectionId;
    }

    public int getNextMessageLength() {
        return m_nextLength;
    }

}
