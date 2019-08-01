package org.voltdb.tools;

import com.google_voltpatches.common.collect.ConcurrentHashMultiset;
import com.google_voltpatches.common.collect.Multiset;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;

public class ZKDU {

    private ZooKeeper zoo;
    private String host;
    final CountDownLatch latch = new CountDownLatch(1);
    private List<String> nodeList;
    private Map<String,Integer> nodeSizeMap;
    private Multiset<String> stats = ConcurrentHashMultiset.create();


    public ZKDU(String host) {
        this.host = host;
    }

    public ZooKeeper connect() throws IOException, InterruptedException {

        zoo = new ZooKeeper(
            host,
            2000,
            new Watcher() {
                public void process(WatchedEvent we) {
                    if (we.getState() == KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            },
            new HashSet<Long>()
            );
        latch.await();
        System.err.println("Connected to " + host);
        return zoo;
    }

    public void close() throws InterruptedException {
        zoo.close();
        //System.err.println("Closed connection to " + host);
    }

    public void getTree(String path) throws KeeperException, InterruptedException {
        if(zoo.exists(path, false) == null){
            return;
        }
        nodeList = new ArrayList<String>();
        nodeSizeMap = new TreeMap<String,Integer>();
        nodeList.add(path);
        int index =0;
        while(index < nodeList.size()){
            String tempPath = nodeList.get(index);
            List<String> children = zoo.getChildren(tempPath, false);
            if(children.size() == 0) {
                Stat stat = zoo.exists(tempPath, false);
                int size = stat.getDataLength();
                nodeSizeMap.put(tempPath,size);
            }
            if(!tempPath.equals("/")) {
                tempPath = tempPath +"/";
            }
            Collections.sort(children);
            for(int i = children.size()-1; i>=0; i--){
                nodeList.add(index+1, tempPath + children.get(i));
            }
            index++;
        }
        return;
    }

    public void printTree() {
        //System.out.println("ZooKeeper Nodes:");
        for (String znode : nodeList) {
            System.out.println(znode);
        }
    }

    public void printSizeMap() {
        System.out.println();
        System.out.println("ZooKeeper Node Sizes (bytes):");
        int width = 40;
        for (Map.Entry<String,Integer> entry : nodeSizeMap.entrySet()) {
            int w = entry.getKey().length();
            if (w > width) {
                width = w;
            }
        }
        for (Map.Entry<String,Integer> entry : nodeSizeMap.entrySet()) {
            String node = entry.getKey();
            Integer size = entry.getValue();
            System.out.printf(" %-" + width + "s %12d\n",node,size);
        }
    }

    public void printSizeMapDepth(int depth) {
        System.out.println();
        System.out.println("ZooKeeper branch sizes (in bytes) to depth " + depth + ":");
        System.out.println();
        System.out.println(" ZNODE                                                                                   BYTES");
        System.out.println(" -------------------------------------------------------------------------------- ------------");

        Map<String,Integer> depthSizeMap = new TreeMap<String,Integer>();
        long total = 0;
        for (Map.Entry<String,Integer> entry : nodeSizeMap.entrySet()) {
            String node = entry.getKey();
            Integer size = entry.getValue();
            size = size + node.getBytes().length;
            total+=size;

            // trim leading "/"
            if (node.startsWith("/")) {
                node = node.substring(1);
            }
            String[] nodeElements = node.split("/");
            int depthLimit = nodeElements.length < depth ? nodeElements.length : depth;
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<depthLimit; i++) {
                sb.append(nodeElements[i]);
                sb.append("/");
            }
            String depthNode = sb.toString();

            Integer depthNodeSize = depthSizeMap.get(depthNode);
            if (depthNodeSize == null) {
                depthSizeMap.put(depthNode,size);
            } else {
                depthSizeMap.put(depthNode,size+depthNodeSize);
            }
        }

        for (Map.Entry<String,Integer> depthEntry : depthSizeMap.entrySet()) {
            String node = depthEntry.getKey();
            Integer size = depthEntry.getValue();
            System.out.printf(" %-80s %12d\n","/"+node,size);
        }
        System.out.println(" -------------------------------------------------------------------------------- ------------");
        System.out.printf(" %-80s %12d\n","Total",total);
    }

    public static void printHelp() {
        System.out.println("Usage:");
        System.out.println("   ./zkdu.sh [optional arguments]");
        System.out.println("Optional Arguments:");
        System.out.println("   -h                   print help");
        System.out.println("   -H connectstring     connect to the specified hostname:port");
        System.out.println("   -d depth             print summary to specified depth");
        System.out.println("   -v                   print the complete list of all znodes and their size");

        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        // defaults
        String connectString = "127.0.0.1:7181";
        boolean verbose = false;
        int depth = 2; // default depth

        // parse args
        for (int i=0; i<args.length; i++) {
            String arg = args[i];
            if (arg.equals("-h")) {
                printHelp();
            }
            if (arg.equals("-H")) {
                if (args.length > i+1) {
                    connectString = args[i+1];
                }
            }
            if (arg.equals("-d")) {
                if (args.length > i+1) {
                    depth = Integer.valueOf(args[i+1]);
                }
            }
            if (arg.equals("-v")) {
                verbose = true;
            }
        }

        ZKDU zkdu = new ZKDU(connectString);
        zkdu.connect();
        zkdu.getTree("/");

        // print requested depth, unless verbose was selected
        if (verbose) {
            zkdu.printSizeMap();
        } else {
            zkdu.printSizeMapDepth(depth);
        }
        zkdu.close();
    }
}
