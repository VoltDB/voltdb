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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

public final class CompressionService {

    static {
        CoreUtils.m_threadLocalDeallocator = new Runnable() {
            @Override
            public void run() {
                releaseThreadLocal();
            }
        };
    }

    private static class IOBuffers {
        private final BBContainer input;
        private final BBContainer output;

        private IOBuffers(BBContainer input, BBContainer output) {
            this.input = input;
            this.output = output;
        }
    }
    private static ThreadLocal<IOBuffers> m_buffers = new ThreadLocal<IOBuffers>() {
        @Override
        protected IOBuffers initialValue() {
            return new IOBuffers(DBBPool.allocateDirect(1024 * 32), DBBPool.allocateDirect(1024 * 32));
        }
    };

    public static void releaseThreadLocal() {
        m_buffers.get().input.discard();
        m_buffers.get().output.discard();
        m_buffers.remove();
    }

    /*
     * The executor service is only used if the VoltDB computation service is not available.
     */
    private static final ListeningExecutorService m_executor = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(Math.max(2, CoreUtils.availableProcessors()),
                                         CoreUtils.getThreadFactory("Compression service thread"))
            );

    public static Future<byte[]> compressBufferAsync(final ByteBuffer buffer) {
        assert(buffer.isDirect());
        return submitCompressionTask(new Callable<byte[]>() {

            @Override
            public byte[] call() throws Exception {
                return compressBuffer(buffer);
            }

        });
    }

    public static Future<BBContainer> compressAndCRC32cBufferAsync(final ByteBuffer inBuffer, final BBContainer outBufferC) {
        assert(inBuffer.isDirect());
        assert(outBufferC.b().isDirect());
        return submitCompressionTask(new Callable<BBContainer>() {

            @Override
            public BBContainer call() throws Exception {
                final ByteBuffer outBuffer = outBufferC.b();
                //Reserve 4-bytes for the CRC
                final int crcPosition = outBuffer.position();
                outBuffer.position(outBuffer.position() + 4);
                final int crcCalcStart = outBuffer.position();
                compressBuffer(inBuffer, outBuffer);
                final int crc32c =
                        DBBPool.getCRC32C( outBufferC.address(), crcCalcStart, outBuffer.limit() - crcCalcStart);
                outBuffer.putInt(crcPosition, crc32c);
                return outBufferC;
            }

        });
    }

    public static int compressBuffer(ByteBuffer buffer, ByteBuffer output) throws IOException {
        assert(buffer.isDirect());
        assert(output.isDirect());
        int inputPos = buffer.position();
        int outputPos = output.position();
        int size = buffer.remaining();
        output.put(buffer);
        buffer.position(inputPos);
        output.position(outputPos);
        return size;
    }

    public static byte[] compressBuffer(ByteBuffer buffer) throws IOException {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    public static byte[] compressBytes(byte bytes[], int offset, int length) throws IOException {
        return Arrays.copyOfRange(bytes, offset, offset + length);
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
        byte[] result = new byte[compressed.remaining()];
        compressed.get(result);
        return result;
    }

    public static int maxCompressedLength(int uncompressedSize) {
        return uncompressedSize;
    }

    public static int uncompressedLength(ByteBuffer compressed) throws IOException {
        assert(compressed.isDirect());
        return compressed.remaining();
    }

    public static int decompressBuffer(final ByteBuffer compressed, final ByteBuffer uncompressed) throws IOException {
        assert(compressed.isDirect());
        assert(uncompressed.isDirect());
        int size = compressed.remaining();
        int inputPos = compressed.position();
        int outputPos = uncompressed.position();
        uncompressed.put(compressed);
        compressed.position(inputPos);
        uncompressed.position(outputPos);
        return size;
    }

    public static byte[] decompressBytes(byte bytes[]) throws IOException {
        return bytes;
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

    public static <T> ListenableFuture<T> submitCompressionTask(Callable<T> task) {
        VoltDBInterface instance = VoltDB.instance();
        if (VoltDB.instance() != null) {
            ListeningExecutorService es = instance.getComputationService();
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
