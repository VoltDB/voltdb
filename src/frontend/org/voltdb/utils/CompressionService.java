/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.xerial.snappy.Snappy;

public final class CompressionService {

    private static class IOBuffers {
        private final ByteBuffer input;
        private final ByteBuffer output;

        private IOBuffers(ByteBuffer input, ByteBuffer output) {
            this.input = input;
            this.output = output;
        }
    }
    private static ThreadLocal<IOBuffers> m_buffers = new ThreadLocal<IOBuffers>() {
        @Override
        protected IOBuffers initialValue() {
            return new IOBuffers(ByteBuffer.allocateDirect(1024 * 32), ByteBuffer.allocateDirect(1024 * 32));
        }
    };

    public static void releaseThreadLocal() {
        m_buffers.remove();
    }

    /*
     * The executor service is only used if the VoltDB computation service is not available.
     */
    private static final ExecutorService m_executor =
            Executors.newFixedThreadPool(Math.max(2, CoreUtils.availableProcessors()),
                                         CoreUtils.getThreadFactory("Compression service thread"));

    private static IOBuffers getBuffersForCompression(int length, boolean inputNotUsed) {
        IOBuffers buffers = m_buffers.get();
        ByteBuffer input = buffers.input;
        ByteBuffer output = buffers.output;

        final int maxCompressedLength = Snappy.maxCompressedLength(length);

        /*
         * A direct byte buffer might be provided in which case no input buffer is needed
         */
        boolean changedBuffer = false;
        if (!inputNotUsed && input.capacity() < length) {
            input = ByteBuffer.allocateDirect(Math.max(input.capacity() * 2, length));
            changedBuffer = true;
        }

        if (output.capacity() < maxCompressedLength) {
            output = ByteBuffer.allocateDirect(Math.max(output.capacity() * 2, maxCompressedLength));
            changedBuffer = true;
        }

        if (changedBuffer) {
            buffers = new IOBuffers(input, output);
            m_buffers.set(buffers);
        }
        output.clear();
        input.clear();

        return buffers;
    }

    public static Future<byte[]> compressBufferAsync(final ByteBuffer buffer) {
        assert(buffer.isDirect());
        return submitCompressionTask(new Callable<byte[]>() {

            @Override
            public byte[] call() throws Exception {
                return compressBuffer(buffer);
            }

        });
    }

    public static Future<BBContainer> compressAndCRC32cBufferAsync(final ByteBuffer inBuffer, final BBContainer outBuffer) {
        assert(inBuffer.isDirect());
        assert(outBuffer.b.isDirect());
        return submitCompressionTask(new Callable<BBContainer>() {

            @Override
            public BBContainer call() throws Exception {
                //Reserve 4-bytes for the CRC
                final int crcPosition = outBuffer.b.position();
                outBuffer.b.position(outBuffer.b.position() + 4);
                final int crcCalcStart = outBuffer.b.position();
                compressBuffer(inBuffer, outBuffer.b);
                final int crc32c =
                        DBBPool.getCRC32C( outBuffer.address, crcCalcStart, outBuffer.b.limit() - crcCalcStart);
                outBuffer.b.putInt(crcPosition, crc32c);
                return outBuffer;
            }

        });
    }

    public static int compressBuffer(ByteBuffer buffer, ByteBuffer output) throws IOException {
        assert(buffer.isDirect());
        assert(output.isDirect());
        return Snappy.compress(buffer, output);
    }

    public static byte[] compressBuffer(ByteBuffer buffer) throws IOException {
        assert(buffer.isDirect());
        IOBuffers buffers = getBuffersForCompression(buffer.remaining(), true);
        ByteBuffer output = buffers.output;

        final int compressedSize = Snappy.compress(buffer, output);
        byte result[] = new byte[compressedSize];
        output.get(result);
        return result;
    }

    public static byte[] compressBytes(byte bytes[], int offset, int length) throws IOException {
        IOBuffers buffers = getBuffersForCompression(bytes.length, false);
        buffers.input.put(bytes, offset, length);
        buffers.input.flip();
        final int compressedSize = Snappy.compress(buffers.input, buffers.output);
        final byte compressed[] = new byte[compressedSize];
        buffers.output.get(compressed);
        return compressed;
    }

    public static byte[] compressBytes(byte bytes[]) throws IOException {
        return compressBytes(bytes, 0, bytes.length);
    }

    public static Future<byte[]> decompressBufferAsync(final ByteBuffer input) throws IOException {
        return submitCompressionTask(new Callable<byte[]>() {

            @Override
            public byte[] call() throws Exception {
                return decompressBuffer(input);
            }

        });
    }

    public static byte[] decompressBuffer(final ByteBuffer compressed) throws IOException {
        assert(compressed.isDirect());
        IOBuffers buffers = m_buffers.get();
        ByteBuffer output = buffers.output;

        final int uncompressedLength = Snappy.uncompressedLength(compressed);
        if (output.capacity() < uncompressedLength) {
            output = ByteBuffer.allocateDirect(Math.max(output.capacity() * 2, uncompressedLength));
            buffers = new IOBuffers(compressed, output);
            m_buffers.set(buffers);
        }
        output.clear();

        final int actualUncompressedLength = Snappy.uncompress(compressed, output);
        assert(uncompressedLength == actualUncompressedLength);

        byte result[] = new byte[actualUncompressedLength];
        output.get(result);
        return result;
    }

    public static int maxCompressedLength(int uncompressedSize) {
        return Snappy.maxCompressedLength(uncompressedSize);
    }

    public static int uncompressedLength(ByteBuffer compressed) throws IOException {
        assert(compressed.isDirect());
        return Snappy.uncompressedLength(compressed);
    }

    public static int decompressBuffer(final ByteBuffer compressed, final ByteBuffer uncompressed) throws IOException {
        assert(compressed.isDirect());
        assert(uncompressed.isDirect());

        return Snappy.uncompress(compressed, uncompressed);
    }

    public static byte[] decompressBytes(byte bytes[]) throws IOException {
        IOBuffers buffers = m_buffers.get();
        ByteBuffer input = buffers.input;
        ByteBuffer output = buffers.output;

        if (input.capacity() < bytes.length){
            input = ByteBuffer.allocateDirect(Math.max(input.capacity() * 2, bytes.length));
            buffers = new IOBuffers(input, output);
            m_buffers.set(buffers);
        }

        input.clear();
        input.put(bytes);
        input.flip();

        final int uncompressedLength = Snappy.uncompressedLength(input);
        if (output.capacity() < uncompressedLength) {
            output = ByteBuffer.allocateDirect(Math.max(output.capacity() * 2, uncompressedLength));
            buffers = new IOBuffers(input, output);
            m_buffers.set(buffers);
        }
        output.clear();

        final int actualUncompressedLength = Snappy.uncompress(input, output);
        assert(uncompressedLength == actualUncompressedLength);

        byte result[] = new byte[actualUncompressedLength];
        output.get(result);
        return result;
    }


    public static byte[][] compressBytes(byte bytes[][]) throws Exception {
        return compressBytes(bytes, false);
    }

    public static byte[][] compressBytes(byte bytes[][], final boolean base64Encode) throws Exception {
        if (bytes.length == 1) {
            if (base64Encode) {
                return new byte[][] {Base64.encodeToByte(compressBytes(bytes[0]), false)};
            } else {
                return new byte[][] {compressBytes(bytes[0])};
            }
        }
        ArrayList<Future<byte[]>> futures = new ArrayList<Future<byte[]>>(bytes.length);
        for (final byte bts[] : bytes) {
            futures.add(submitCompressionTask(new Callable<byte[]>() {

                @Override
                public byte[] call() throws Exception {
                    if (base64Encode) {
                        return Base64.encodeToByte(compressBytes(bts), false);
                    } else {
                        return compressBytes(bts);
                    }
                }

            }));
        }
        byte compressedBytes[][] = new byte[bytes.length][];

        for (int ii = 0; ii < bytes.length; ii++) {
            compressedBytes[ii] = futures.get(ii).get();
        }
        return compressedBytes;
    }

    public static byte[][] decompressBytes(byte bytes[][]) throws Exception {
        return decompressBytes(bytes, false);
    }

    public static byte[][] decompressBytes(byte bytes[][], final boolean base64Decode) throws Exception {
        if (bytes.length == 1) {
            if (base64Decode) {
                return new byte[][] { decompressBytes(Base64.decode(bytes[0]))};
            } else {
                return new byte[][] { decompressBytes(bytes[0])};
            }
        }
        ArrayList<Future<byte[]>> futures = new ArrayList<Future<byte[]>>(bytes.length);
        for (final byte bts[] : bytes) {
            futures.add(submitCompressionTask(new Callable<byte[]>() {

                @Override
                public byte[] call() throws Exception {
                    if (base64Decode) {
                        return decompressBytes(Base64.decode(bts));
                    } else {
                        return decompressBytes(bts);
                    }
                }

            }));
        }

        byte decompressedBytes[][] = new byte[bytes.length][];

        for (int ii = 0; ii < bytes.length; ii++) {
            decompressedBytes[ii] = futures.get(ii).get();
        }
        return decompressedBytes;
    }

    public static void main(String args[]) throws Exception {
        byte testBytes[] = new byte[1024];
        Arrays.fill(testBytes, (byte)2);
        System.out.println(CompressionService.compressBytes(new byte[][] {testBytes, testBytes, testBytes, testBytes, testBytes, testBytes}, true)[0].length);
        System.out.println(CompressionService.decompressBytes(CompressionService.compressBytes(new byte[][] {testBytes}, true), true)[0].length);
        CompressionService.decompressBytes(CompressionService.compressBytes(new byte[][] {testBytes}));
        CompressionService.decompressBytes(CompressionService.compressBytes(new byte[][] {testBytes}));
    }

    public static Future<byte[]> compressBytesAsync(final byte[] array, final int position,
            final int limit) {
        return submitCompressionTask(new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    return compressBytes(array, position, limit);
                }
        });
    }

    public static <T> Future<T> submitCompressionTask(Callable<T> task) {
        VoltDBInterface instance = VoltDB.instance();
        if (VoltDB.instance() != null) {
            ExecutorService es = instance.getComputationService();
            if (es != null) {
                return es.submit(task);
            }
        }
        return m_executor.submit(task);
    }

    public static byte[] gzipBytes(byte[] rawBytes) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(rawBytes.length);
        DeflaterOutputStream dos = new DeflaterOutputStream(bos);
        dos.write(rawBytes);
        dos.close();
        return bos.toByteArray();
    }

    public static byte[] gunzipBytes(byte[] compressedBytes) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream((int)(compressedBytes.length * 1.5));
        InflaterOutputStream dos = new InflaterOutputStream(bos);
        dos.write(compressedBytes);
        dos.close();
        return bos.toByteArray();
    }
}
