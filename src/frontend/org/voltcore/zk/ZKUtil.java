/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

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
        return asyncMkdirs( zk, dirDN, null);
    }

    public static ZKUtil.StringCallback asyncMkdirs( ZooKeeper zk, String dirDN, byte payload[]) {
        Preconditions.checkArgument(
                dirDN != null &&
                ! dirDN.trim().isEmpty() &&
                ! "/".equals(dirDN) &&
                dirDN.startsWith("/")
                );

        StringBuilder dsb = new StringBuilder(128);
        ZKUtil.StringCallback lastCallback = null;
        try {
            String dirPortions[] = dirDN.substring(1).split("/");
            for (int ii = 0; ii < dirPortions.length; ii++) {
                String dirPortion = dirPortions[ii];
                lastCallback = new ZKUtil.StringCallback();
                dsb.append('/').append(dirPortion);
                zk.create(
                        dsb.toString(),
                        ii == dirPortions.length - 1 ? payload : null,
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

    /*
     * A watcher that can be cancelled. Requires a single threaded executor service
     * to serialize the cancellation with the watch firing.
     */
    public static abstract class CancellableWatcher implements Watcher {
        volatile boolean canceled = false;
        final ExecutorService es;

        public CancellableWatcher(ExecutorService es) {
            this.es = es;
        }

        public void cancel() {
            canceled = true;
        }

        @Override
        public void process(final WatchedEvent event) {
           es.execute(new Runnable() {
               @Override
               public void run() {
                   if (canceled) return;
                   pProcess(event);
               }
           });
        }

        abstract protected void pProcess(final WatchedEvent event);
    }

    public static void deleteRecursively(ZooKeeper zk, String dir) throws KeeperException, InterruptedException
    {
        try {
            List<String> children = zk.getChildren(dir, false);
            for (String child : children) {
                deleteRecursively(zk, joinZKPath(dir, child));
            }
            zk.delete(dir, -1);
        } catch (KeeperException.NoNodeException ignore) {}
    }

    public static void asyncDeleteRecursively(ZooKeeper zk, String dirDN) throws KeeperException, InterruptedException {
        Preconditions.checkArgument(
                dirDN != null &&
                ! dirDN.trim().isEmpty() &&
                ! "/".equals(dirDN) &&
                dirDN.startsWith("/")
                );

        int beforeSize = 0;
        TreeSet<ListingNode> listing = new TreeSet<>();
        listing.add(new ListingNode(0, dirDN));

        Queue<Pair<ChildrenCallback,ListingNode>> callbacks = new ArrayDeque<>();
        while (beforeSize < listing.size()) {

            for (ListingNode node: listing) {
                if (node.childCount == ListingNode.UNPROBED) {
                    ChildrenCallback cb = new ChildrenCallback();
                    zk.getChildren(node.node, false, cb, null);
                    callbacks.offer(Pair.of(cb,node));
                }
            }

            beforeSize = listing.size();

            Iterator<Pair<ChildrenCallback,ListingNode>> itr = callbacks.iterator();
            while (itr.hasNext()) {
                Pair<ChildrenCallback,ListingNode> callbackPair = itr.next();
                try {
                    List<String> children = callbackPair.getFirst().getChildren();
                    callbackPair.getSecond().childCount = children.size();
                    for (String child: children) {
                        listing.add(new ListingNode(callbackPair.getSecond(), child));
                    }
                } catch (KeeperException.NoNodeException ignoreIt) {
                }
                itr.remove();
            }
        }
        // the last node is the root node (order implied in ListingNode.compareTo(other))
        VoidCallback lastCallback = null;
        Iterator<ListingNode> lnitr = listing.iterator();
        while (lnitr.hasNext()) {
            ListingNode node = lnitr.next();
            lastCallback = new VoidCallback();
            zk.delete(node.node, -1, lastCallback, null);
            lnitr.remove();
        }
        try {
            lastCallback.get();
        } catch (KeeperException.NoNodeException ignoreIt) {
        }
    }

    private final static class ListingNode implements Comparable<ListingNode> {
        static final int UNPROBED = -1;

        final int lvl;
        final String node;
        int childCount = UNPROBED;

        ListingNode(int lvl, String node) {
            this.lvl = lvl;
            this.node = node;
        }

        ListingNode(ListingNode parent, String child) {
            this.lvl = parent.lvl + 1;
            this.node = joinZKPath(parent.node, child);
        }

        @Override
        public int compareTo(ListingNode o) {
            int cmp = o.lvl - lvl;
            if (cmp == 0) {
                cmp = node.compareTo(o.node);
            }
            return cmp;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + lvl;
            result = prime * result + ((node == null) ? 0 : node.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ListingNode other = (ListingNode) obj;
            if (lvl != other.lvl)
                return false;
            if (node == null) {
                if (other.node != null)
                    return false;
            } else if (!node.equals(other.node))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "ListingNode [lvl=" + lvl + ", node=" + node
                    + ", childCount=" + childCount + "]";
        }
    }
}
