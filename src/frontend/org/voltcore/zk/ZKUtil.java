/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
package org.voltcore.zk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.utils.Pair;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class ZKUtil {

     /** Prevents class from being inherited or instantiated **/
     private ZKUtil() {}

    /**
     * Joins a path with a filename, if the path already ends with "/", it won't
     * add another "/" to the path.
     *
     * @param path
     * @param name
     * @return
     */
    public static String joinZKPath(String path, String name) {
        if (path.endsWith("/")) {
            return path + name;
        } else {
            return path + "/" + name;
        }
    }

    /**
     * Helper to produce a valid path from variadic strings.
     */
    public static String path(String... components)
    {
        String path = components[0];
        for (int i=1; i < components.length; i++) {
            path = ZKUtil.joinZKPath(path, components[i]);
        }
        return path;
    }

    public static File getUploadAsTempFile( ZooKeeper zk, String path, String prefix) throws Exception {
        byte data[] = retrieveChunksAsBytes(zk, path, prefix, false).getFirst();
        File tempFile = File.createTempFile("foo", "bar");
        tempFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tempFile);
        ByteBuffer b = ByteBuffer.wrap(data);
        try {
            while (b.hasRemaining()) {
                fos.getChannel().write(b);
            }
        } finally {
            fos.close();
        }
        return tempFile;
    }

//    /*
//     * Attempts to upload the files atomically and transactionally
//     */
//    public static void uploadFiles(
//            ZooKeeper zk,
//            String rootPath,
//            List<String> zkNames,
//            List<String> filePaths,
//            boolean ephemeral) throws Exception {
//        for (String filePath : filePaths) {
//            File file = new VoltFile(filePath);
//            if (!file.exists() || !file.canRead()) {
//                throw new Exception("File " + file + " does not exist or isn't readable");
//            }
//        }
//        for (int ii = 0; ii < filePaths.size(); ii++) {
//            File file = new VoltFile(filePaths.get(ii));
//            uploadFileAsChunks(zk, rootPath + "/" + zkNames.get(ii), file, ephemeral);
//        }
//    }

    public static boolean uploadFileAsChunks(ZooKeeper zk, String zkPath, File file, boolean ephemeral)
            throws Exception {
        if (file.exists() && file.canRead()) {
            FileInputStream fis = new FileInputStream(file);
            ByteBuffer fileBuffer = ByteBuffer.allocate((int)file.length());
            while (fileBuffer.hasRemaining()) {
                fis.getChannel().read(fileBuffer);
            }
            fileBuffer.flip();
            uploadBytesAsChunks(zk, zkPath, fileBuffer.array(), ephemeral);
        }
        return true;
    }

    public static void uploadBytesAsChunks(ZooKeeper zk, String node, byte payload[], boolean ephemeral) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(compressBytes(payload));
        while (buffer.hasRemaining()) {
            int nextChunkSize = Math.min(1024 * 1024, buffer.remaining());
            ByteBuffer nextChunk = ByteBuffer.allocate(nextChunkSize);
            buffer.limit(buffer.position() + nextChunkSize);
            nextChunk.put(buffer);
            buffer.limit(buffer.capacity());
            zk.create(node, nextChunk.array(), Ids.OPEN_ACL_UNSAFE, ephemeral ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT_SEQUENTIAL);
        }
        zk.create(node + "_complete", null, Ids.OPEN_ACL_UNSAFE, ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT);
    }

    public static byte[] retrieveChunksAsBytes(ZooKeeper zk, String path, String prefix) throws Exception {
        return retrieveChunksAsBytes(zk, path, prefix, false).getFirst();
    }

    public static Pair<byte[], Integer> retrieveChunksAsBytes(ZooKeeper zk, String path, String prefix, boolean getCRC) throws Exception {
        TreeSet<String> chunks = new TreeSet<String>();
        while (true) {
            boolean allUploadsComplete = true;
            if (!chunks.contains(path + "/" + prefix + "_complete")) {
                allUploadsComplete = false;
            }
            if (allUploadsComplete) {
                break;
            }

            chunks = new TreeSet<String>(zk.getChildren(path, false));
            for (String chunk : chunks) {
                for (int ii = 0; ii < chunks.size(); ii++) {
                    if (chunk.startsWith(path + "/" + prefix)) {
                        chunks.add(chunk);
                    }
                }
            }
        }

        byte resultBuffers[][] = new byte[chunks.size() - 1][];
        int ii = 0;
        PureJavaCrc32 crc = getCRC ? new PureJavaCrc32() : null;
        for (String chunk : chunks) {
            if (chunk.endsWith("_complete")) continue;
            resultBuffers[ii] = zk.getData(chunk, false, null);
            if (crc != null) {
                crc.update(resultBuffers[ii]);
            }
            ii++;
        }

        return Pair.of(decompressBytes(resultBuffers), crc != null ? (int)crc.getValue() : null);
    }

    public static byte[] compressBytes(byte bytes[]) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(baos);
        gos.write(bytes);
        gos.finish();
        return baos.toByteArray();
    }

    public static byte[] decompressBytes(byte bytess[][]) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte bytes[] : bytess) {
            baos.write(bytes);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        baos = new ByteArrayOutputStream();
        GZIPInputStream gis = new GZIPInputStream(bais);
        byte bytes[] = new byte[1024 * 8];
        while (true) {
            int read = gis.read(bytes);
            if (read == -1) {
                break;
            }
            baos.write(bytes, 0, read);
        }
        return baos.toByteArray();
    }

    public static final ZooKeeper getClient(String zkAddress, int timeout, Set<Long> verbotenThreads) throws Exception {
        final Semaphore zkConnect = new Semaphore(0);
        ZooKeeper zk = new ZooKeeper(zkAddress, 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    zkConnect.release();
                }
            }

        },
        verbotenThreads);
        if (!zkConnect.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
            return null;
        }
        return zk;
    }

    public static final void mkdirs(ZooKeeper zk, String dirDN) {
        ZKUtil.StringCallback callback = asyncMkdirs(zk, dirDN );
        try {
            callback.get();
        } catch (Throwable t) {
            Throwables.propagate(t);
        }
    }

    public static ZKUtil.StringCallback asyncMkdirs( ZooKeeper zk, String dirDN) {
        Preconditions.checkArgument(
                dirDN != null &&
                ! dirDN.trim().isEmpty() &&
                ! "/".equals(dirDN) &&
                dirDN.startsWith("/")
                );

        StringBuilder dsb = new StringBuilder(128);
        ZKUtil.StringCallback lastCallback = null;
        try {
            for (String dirPortion: dirDN.substring(1).split("/")) {
                lastCallback = new ZKUtil.StringCallback();
                dsb.append('/').append(dirPortion);
                zk.create(
                        dsb.toString(),
                        null,
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT,
                        lastCallback,
                        null);
            }
        }
        catch (Throwable t) {
            Throwables.propagate(t);
        }
        return lastCallback;
    }

    public static class StatCallback implements org.apache.zookeeper_voltpatches.AsyncCallback.StatCallback {
        private final CountDownLatch done = new CountDownLatch(1);
        private Object results[];


        public Object[] get() throws InterruptedException, KeeperException {
            done.await();
            return getResult();
        }

        public Stat getStat() throws InterruptedException, KeeperException {
            done.await();
            return (Stat)getResult()[3];
        }

        private Object[] getResult() throws KeeperException {
            KeeperException.Code code = KeeperException.Code.get((Integer)results[0]);
            if (code == KeeperException.Code.OK) {
                return results;
            } else {
                throw KeeperException.create(code);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            results = new Object[] { rc, path, ctx, stat };
            done.countDown();
        }
    }

    public static class VoidCallback implements org.apache.zookeeper_voltpatches.AsyncCallback.VoidCallback {
        private final CountDownLatch done = new CountDownLatch(1);
        private Object results[];


        public Object[] get() throws InterruptedException, KeeperException {
            done.await();
            return getResult();
        }

        public byte[] getData() throws InterruptedException, KeeperException {
            done.await();
            return (byte[])getResult()[3];
        }

        private Object[] getResult() throws KeeperException {
            KeeperException.Code code = KeeperException.Code.get((Integer)results[0]);
            if (code == KeeperException.Code.OK) {
                return results;
            } else {
                throw KeeperException.create(code);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            results = new Object[] { rc, path, ctx };
            done.countDown();
        }

    }
    public static class ByteArrayCallback implements org.apache.zookeeper_voltpatches.AsyncCallback.DataCallback {
        private final CountDownLatch done = new CountDownLatch(1);
        private Object results[];

        public Object[] get() throws InterruptedException, KeeperException {
            done.await();
            return getResult();
        }

        public String getPath() throws InterruptedException, KeeperException {
            done.await();
            return (String)getResult()[1];
        }

        public byte[] getData() throws InterruptedException, KeeperException {
            done.await();
            return (byte[])getResult()[3];
        }

        private Object[] getResult() throws KeeperException {
            KeeperException.Code code = KeeperException.Code.get((Integer)results[0]);
            if (code == KeeperException.Code.OK) {
                return results;
            } else {
                throw KeeperException.create(code);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data,
                Stat stat) {
            results = new Object[] { rc, path, ctx, data, stat };
            done.countDown();
        }
    }

    public static class FutureWatcher implements org.apache.zookeeper_voltpatches.Watcher {
        private final CountDownLatch done = new CountDownLatch(1);
        private WatchedEvent event;

        @Override
        public void process(WatchedEvent event) {
            this.event = event;
            done.countDown();
        }

        public WatchedEvent get() throws InterruptedException {
            done.await();
            return event;
        }

        /** @returns null if the timeout expired. */
        public WatchedEvent get(long timeout, TimeUnit unit) throws InterruptedException {
            done.await(timeout, unit);
            return event;
        }
    }

    public static class StringCallback implements
         org.apache.zookeeper_voltpatches.AsyncCallback.StringCallback {
        private final CountDownLatch done = new CountDownLatch(1);
        private Object results[];

        public Object[] get() throws InterruptedException, KeeperException {
            done.await();
            return getResult();
        }

        public Object[] get(long timeout, TimeUnit unit)
                throws InterruptedException, KeeperException,
                TimeoutException {
            if (done.await(timeout, unit)) {
                return getResult();
            } else {
                throw new TimeoutException();
            }
        }

        private Object[] getResult() throws KeeperException {
            KeeperException.Code code = KeeperException.Code.get((Integer)results[0]);
            if (code == KeeperException.Code.OK) {
                return results;
            } else {
                throw KeeperException.create(code);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            results = new Object[] { rc, path, ctx, name };
            done.countDown();
        }
    }

    public static class ChildrenCallback implements
    org.apache.zookeeper_voltpatches.AsyncCallback.ChildrenCallback {
        private final CountDownLatch done = new CountDownLatch(1);
        private Object results[];

        public Object[] get() throws InterruptedException, KeeperException {
            done.await();
            return getResult();
        }

        public Object[] get(long timeout, TimeUnit unit)
                throws InterruptedException, KeeperException,
                TimeoutException {
            if (done.await(timeout, unit)) {
                return getResult();
            } else {
                throw new TimeoutException();
            }
        }

        private Object[] getResult() throws KeeperException {
            KeeperException.Code code = KeeperException.Code.get((Integer)results[0]);
            if (code == KeeperException.Code.OK) {
                return results;
            } else {
                throw KeeperException.create(code);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            results = new Object[] {rc, path, ctx, children};
            done.countDown();
        }

        @SuppressWarnings("unchecked")
        public List<String> getChildren()  throws InterruptedException, KeeperException  {
            done.await();
            return (List<String>)getResult()[3];
        }
    }

}
