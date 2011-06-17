/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

import java.util.concurrent.*;
import java.util.*;
import java.io.*;
import java.util.zip.*;
import org.voltdb.utils.Base64;

public final class CompressionService {

    private final static class CompressionTools {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Deflater deflater = new Deflater(Deflater.BEST_SPEED);
        final DeflaterOutputStream defOutputStream = new DeflaterOutputStream(baos, deflater, 4096);
        final Inflater inflater = new Inflater();
        final InflaterOutputStream infOutputStream = new InflaterOutputStream(baos, inflater, 4096);
    }

    private static ThreadLocal<CompressionTools> m_tools = new ThreadLocal<CompressionTools>() {
        @Override
        protected CompressionTools initialValue() {
            return new CompressionTools();
        }
    };

    private static final ExecutorService m_executor =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
            private int threadIndex = 0;
            @Override
            public synchronized Thread  newThread(Runnable r) {
                Thread t = new Thread(r, "Compression service thread - " + threadIndex++);
                t.setDaemon(true);
                return t;
            }

        });

    public static byte[] compressBytes(byte bytes[]) throws IOException {
        final CompressionTools tools = m_tools.get();
        tools.baos.reset();
        tools.deflater.reset();
        tools.defOutputStream.write(bytes);
        tools.defOutputStream.finish();
        tools.defOutputStream.flush();
        return tools.baos.toByteArray();
    }

    public static byte[] decompressBytes(byte bytes[]) throws IOException {
        final CompressionTools tools = m_tools.get();
        tools.baos.reset();
        tools.inflater.reset();

        tools.infOutputStream.write(bytes);
        tools.infOutputStream.finish();
        tools.infOutputStream.flush();
        return tools.baos.toByteArray();
    }

    public static byte[][] compressBytes(byte bytes[][]) throws Exception {
        return compressBytes(bytes, false);
    }

    public static byte[][] compressBytes(byte bytes[][], final boolean base64Encode) throws Exception {
        if (bytes.length == 1) {
            if (base64Encode) {
                return new byte[][] {Base64.encodeBytesToBytes(compressBytes(bytes[0]))};
            } else {
                return new byte[][] {compressBytes(bytes[0])};
            }
        }
        ArrayList<Future<byte[]>> futures = new ArrayList<Future<byte[]>>(bytes.length);
        for (final byte bts[] : bytes) {
            futures.add(m_executor.submit(new Callable<byte[]>() {

                @Override
                public byte[] call() throws Exception {
                    if (base64Encode) {
                        return Base64.encodeBytesToBytes(compressBytes(bts));
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
            futures.add(m_executor.submit(new Callable<byte[]>() {

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
}
