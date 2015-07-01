/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.ArrayList;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;

public class IOBench {

    public static final Object readyLock = new Object();

    public static final Object goLock = new Object();

    public static boolean goNow = false;

    public static int readyThreads = 0;

    public static ExecutorService m_es = Executors.newFixedThreadPool(16);

    public static final ByteBuffer m_buffer = ByteBuffer.allocateDirect(2097152);

    public static final boolean extra_output = false;

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            while (true) {
                runSerialTest();
                Thread.sleep(5000);
                runParallelTest();
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runParallelTest() throws IOException, InterruptedException {
        System.out.println("Starting parallel test");
        final ArrayList<Future<?>> futures = new ArrayList<Future<?>>();
        synchronized (readyLock) {

            for (int ii = 0; ii < 16; ii++) {
                futures.add(m_es.submit(new Writer()));
            }

            while (readyThreads < 16) {
                readyLock.wait();
            }
        }

        final long startTime = System.currentTimeMillis();
        synchronized (goLock) {
            goNow = true;
            goLock.notifyAll();
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        final long endTime = System.currentTimeMillis();
        final long delta = endTime - startTime;
        final double seconds = delta / 1000.0;
        final long written = 4096;//megabytes
        final double throughput = 4096 / seconds;
        System.out.printf("Parallel test took %.1f seconds to write 4 gigs at a rate of %.2f\n", seconds, throughput);
    }

    public static class Writer implements Callable<Object> {
        @Override
        public Object call() throws Exception {
            final File f = File.createTempFile("foo", "bar", new File("/var/voltdb"));
            f.deleteOnExit();
            try {
                final FileOutputStream fos = new FileOutputStream(f);
                final FileChannel fc = fos.getChannel();
                try {
                    synchronized (readyLock) {
                        readyThreads++;
                        readyLock.notify();
                    }

                    synchronized (goLock) {
                        while (!goNow) {
                            goLock.wait();
                        }
                    }
                    for (int ii = 0; ii < 128; ii++) {
                        final int percentDone = (int)((ii / 128.0) * 100.0);
                        if (extra_output && percentDone % 25 == 0) {
                            System.out.println(
                                    "Thread " + Thread.currentThread().getId() +
                                    "  is " + percentDone + "% complete with parallel test");
                        }
                        ByteBuffer b = m_buffer.duplicate();
                        while (b.hasRemaining()) {
                            fc.write(b);
                        }
                    }
                    fos.getFD().sync();

                    if (extra_output) {
                        System.out.println("Thread " + Thread.currentThread().getId() + " is finished");
                    }
                    synchronized (readyLock) {
                        readyThreads--;
                        readyLock.notifyAll();
                        while (readyThreads > 0) {
                            readyLock.wait();
                        }
                    }
                } finally {
                    fos.close();
                }
            } finally {
                f.delete();
            }
            return null;
        }
    }

    public static void runSerialTest() throws IOException {
        System.out.println("Starting serial test");
        final File f = File.createTempFile("foo", "bar", new File("/var/voltdb/"));
        f.deleteOnExit();
        try {
            final FileOutputStream fos = new FileOutputStream(f);
            final FileChannel fc = fos.getChannel();
            try {
                final long startTime = System.currentTimeMillis();
                for (int ii = 0; ii < 2048; ii++) {
                    final int percentDone = (int)((ii / 2048.0) * 100.0);
                    if (extra_output && percentDone % 10 == 0) {
                        System.out.println(percentDone + "% complete with serial test");
                    }
                    final ByteBuffer b = m_buffer.duplicate();
                    while (b.hasRemaining()) {
                        fc.write(b);
                    }
                }
                fos.getFD().sync();
                final long endTime = System.currentTimeMillis();
                final long delta = endTime - startTime;
                final double seconds = delta / 1000.0;
                final long written = 4096;//megabytes
                final double throughput = 4096 / seconds;
                System.out.printf("Serial test took %.1f seconds to write 4 gigs at a rate of %.2f\n", seconds, throughput);
            } finally {
                fos.close();
            }
        } finally {
            f.delete();
        }
    }

}
