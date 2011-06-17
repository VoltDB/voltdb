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

    private final static class DeflationTool {
        public DeflationTool(ByteArrayOutputStream baos, int compressionSetting) {
            this.baos = baos;
            this.deflater = new Deflater(compressionSetting);
            this.defOutputStream = new DeflaterOutputStream(baos, deflater, 4096);
        }
        final ByteArrayOutputStream baos;
        final Deflater deflater;
        final DeflaterOutputStream defOutputStream;
    }

    private final static class InflationTool {
        public InflationTool(ByteArrayOutputStream baos) {
            this.baos = baos;
            infOutputStream = new InflaterOutputStream(baos, inflater, 4096);
        }
        final ByteArrayOutputStream baos;
        final Inflater inflater = new Inflater();
        final InflaterOutputStream infOutputStream;
    }

    private static ThreadLocal<ByteArrayOutputStream> m_baos = new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream();
        }
    };

    private static ThreadLocal<Map<Integer, DeflationTool>> m_deflationTools = new ThreadLocal<Map<Integer, DeflationTool>>() {
        @Override
        protected Map<Integer, DeflationTool> initialValue() {
            HashMap<Integer, DeflationTool> tools = new HashMap<Integer, DeflationTool>();
            ByteArrayOutputStream baos = m_baos.get();
            tools.put(Deflater.BEST_SPEED, new DeflationTool(baos, Deflater.BEST_SPEED));
            tools.put(Deflater.BEST_COMPRESSION, new DeflationTool(baos, Deflater.BEST_COMPRESSION));
            tools.put(Deflater.DEFAULT_COMPRESSION, new DeflationTool(baos, Deflater.DEFAULT_COMPRESSION));
            return tools;
        }
    };

    private static ThreadLocal<InflationTool> m_inflationTools = new ThreadLocal<InflationTool>() {
        @Override
        protected InflationTool initialValue() {
            return new InflationTool(m_baos.get());
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
        return compressBytes(bytes, Deflater.BEST_SPEED);
    }

    public static byte[] compressBytes(byte bytes[], int setting) throws IOException {
        final DeflationTool tool = m_deflationTools.get().get(setting);
        tool.baos.reset();
        tool.deflater.reset();
        tool.defOutputStream.write(bytes);
        tool.defOutputStream.finish();
        tool.defOutputStream.flush();
        return tool.baos.toByteArray();
    }

    public static byte[] decompressBytes(byte bytes[]) throws IOException {
        final InflationTool tool = m_inflationTools.get();
        tool.baos.reset();
        tool.inflater.reset();

        tool.infOutputStream.write(bytes);
        tool.infOutputStream.finish();
        tool.infOutputStream.flush();
        return tool.baos.toByteArray();
    }

    public static byte[][] compressBytes(byte bytes[][]) throws Exception {
        return compressBytes(bytes, Deflater.BEST_SPEED, false);
    }

    public static byte[][] compressBytes(byte bytes[][], int setting) throws Exception {
        return compressBytes(bytes, setting, false);
    }

    public static byte[][] compressBytes(byte bytes[][], boolean base64Encode) throws Exception {
        return compressBytes(bytes, Deflater.BEST_SPEED, base64Encode);
    }

    public static byte[][] compressBytes(byte bytes[][],final int setting, final boolean base64Encode) throws Exception {
        if (bytes.length == 1) {
            if (base64Encode) {
                return new byte[][] {Base64.encodeBytesToBytes(compressBytes(bytes[0], setting))};
            } else {
                return new byte[][] {compressBytes(bytes[0], setting)};
            }
        }
        ArrayList<Future<byte[]>> futures = new ArrayList<Future<byte[]>>(bytes.length);
        for (final byte bts[] : bytes) {
            futures.add(m_executor.submit(new Callable<byte[]>() {

                @Override
                public byte[] call() throws Exception {
                    if (base64Encode) {
                        return Base64.encodeBytesToBytes(compressBytes(bts, setting));
                    } else {
                        return compressBytes(bts, setting);
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
