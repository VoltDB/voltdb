/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

import org.mockito.Mockito;

/**
 * Error injection test utility that works by wrapping the {@link FileChannel} used by a snapshot targets with a spied
 * instance.
 * <p>
 * Two write methods are mocked. {@link FileChannel#write(ByteBuffer)} for {@link DefaultSnapshotDataTarget} and
 * {@link FileChannel#write(ByteBuffer[], int, int)} for {@link DirectIoSnapshotDataTarget}
 */
public class SnapshotErrorInjectionUtils {

    /**
     * Force the first write to any native snapshot data target to fail with IOException
     */
    public static void failFirstWrite() {
        NativeSnapshotDataTarget.SNAPSHOT_FILE_CHANEL_OPERATER = fc -> {
            FileChannel spied = Mockito.spy(fc);
            try {
                Mockito.doThrow(IOException.class).when(spied).write(Mockito.<ByteBuffer>any());

                Mockito.doThrow(IOException.class).when(spied).write(Mockito.<ByteBuffer[]>any(), Mockito.anyInt(),
                        Mockito.anyInt());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return spied;
        };
    }

    /**
     * Force the first write to any native snapshot data target to fail with IOException
     */
    public static void failSecondWrite() {
        NativeSnapshotDataTarget.SNAPSHOT_FILE_CHANEL_OPERATER = fc -> {
            FileChannel spied = Mockito.spy(fc);
            try {
                Mockito.doCallRealMethod().doThrow(IOException.class).when(spied).write(Mockito.<ByteBuffer>any());

                Mockito.doAnswer(i -> {
                    Object[] args = i.getArguments();
                    ByteBuffer[] buffers = (ByteBuffer[]) args[0];
                    if (buffers.length == 1) {
                        return i.callRealMethod();
                    } else {
                        args[2] = 1;
                        i.callRealMethod();
                        throw new IOException("Disk full");
                    }
                }).doThrow(IOException.class).when(spied).write(Mockito.<ByteBuffer[]>any(), Mockito.anyInt(),
                        Mockito.anyInt());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return spied;
        };
    }

    /**
     * Force writes to any native snapshot data target to block until {@link latch} is complete
     *
     * @param latch to wait on
     */
    public static void blockOn(CountDownLatch latch) {
        NativeSnapshotDataTarget.SNAPSHOT_FILE_CHANEL_OPERATER = fc -> {
            FileChannel spied = Mockito.spy(fc);
            try {
                Mockito.doCallRealMethod().doAnswer(i -> {
                    latch.await();
                    return i.callRealMethod();
                }).when(spied).write(Mockito.<ByteBuffer>any());

                Mockito.doAnswer(i -> {
                    latch.await();
                    return i.callRealMethod();
                }).when(spied).write(Mockito.<ByteBuffer[]>any(), Mockito.anyInt(), Mockito.anyInt());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return spied;
        };
    }

    private SnapshotErrorInjectionUtils() {}
}
