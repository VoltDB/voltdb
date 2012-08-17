/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper_voltpatches.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;

import javax.management.InstanceAlreadyExistsException;

import org.apache.jute_voltpatches.BinaryInputArchive;
import org.apache.jute_voltpatches.Record;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.Code;
import org.apache.zookeeper_voltpatches.KeeperException.SessionMovedException;
import org.apache.zookeeper_voltpatches.ZooDefs;
import org.apache.zookeeper_voltpatches.ZooDefs.OpCode;
import org.apache.zookeeper_voltpatches.common.PathUtils;
import org.apache.zookeeper_voltpatches.data.ACL;
import org.apache.zookeeper_voltpatches.data.Id;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.apache.zookeeper_voltpatches.data.StatPersisted;
import org.apache.zookeeper_voltpatches.jmx.MBeanRegistry;
import org.apache.zookeeper_voltpatches.proto.CreateRequest;
import org.apache.zookeeper_voltpatches.proto.CreateResponse;
import org.apache.zookeeper_voltpatches.proto.DeleteRequest;
import org.apache.zookeeper_voltpatches.proto.ExistsRequest;
import org.apache.zookeeper_voltpatches.proto.ExistsResponse;
import org.apache.zookeeper_voltpatches.proto.GetACLRequest;
import org.apache.zookeeper_voltpatches.proto.GetACLResponse;
import org.apache.zookeeper_voltpatches.proto.GetChildren2Request;
import org.apache.zookeeper_voltpatches.proto.GetChildren2Response;
import org.apache.zookeeper_voltpatches.proto.GetChildrenRequest;
import org.apache.zookeeper_voltpatches.proto.GetChildrenResponse;
import org.apache.zookeeper_voltpatches.proto.GetDataRequest;
import org.apache.zookeeper_voltpatches.proto.GetDataResponse;
import org.apache.zookeeper_voltpatches.proto.ReplyHeader;
import org.apache.zookeeper_voltpatches.proto.RequestHeader;
import org.apache.zookeeper_voltpatches.proto.SetACLRequest;
import org.apache.zookeeper_voltpatches.proto.SetACLResponse;
import org.apache.zookeeper_voltpatches.proto.SetDataRequest;
import org.apache.zookeeper_voltpatches.proto.SetDataResponse;
import org.apache.zookeeper_voltpatches.proto.SetWatches;
import org.apache.zookeeper_voltpatches.proto.SyncRequest;
import org.apache.zookeeper_voltpatches.proto.SyncResponse;
import org.apache.zookeeper_voltpatches.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper_voltpatches.server.NIOServerCnxn.CnxnStats;
import org.apache.zookeeper_voltpatches.server.NIOServerCnxn.Factory;
import org.apache.zookeeper_voltpatches.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper_voltpatches.server.auth.AuthenticationProvider;
import org.apache.zookeeper_voltpatches.server.auth.ProviderRegistry;
import org.apache.zookeeper_voltpatches.txn.CreateSessionTxn;
import org.apache.zookeeper_voltpatches.txn.CreateTxn;
import org.apache.zookeeper_voltpatches.txn.DeleteTxn;
import org.apache.zookeeper_voltpatches.txn.ErrorTxn;
import org.apache.zookeeper_voltpatches.txn.SetACLTxn;
import org.apache.zookeeper_voltpatches.txn.SetDataTxn;
import org.apache.zookeeper_voltpatches.txn.TxnHeader;
import org.voltcore.logging.VoltLogger;

/**
 * This class implements a simple standalone ZooKeeperServer. It sets up the
 * following chain of RequestProcessors to process requests:
 * PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor
 */
public class ZooKeeperServer implements SessionExpirer, ServerStats.Provider,
        Runnable {
    protected static final VoltLogger LOG;

    static boolean skipACL;
    static {
        LOG = new VoltLogger("ZK-SERVER");
        //Environment.logEnv("Server environment:", LOG);
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    protected ZooKeeperServerBean jmxServerBean;
    protected DataTreeBean jmxDataTreeBean;

    /**
     * The server delegates loading of the tree to an instance of the interface
     */
    public interface DataTreeBuilder {
        public DataTree build();
    }

    static public class BasicDataTreeBuilder implements DataTreeBuilder {
        @Override
        public DataTree build() {
            return new DataTree();
        }
    }

    public interface Callout {
        public void run();

        public void request(Request r);

        public Semaphore createSession(final ServerCnxn cnxn,
                final byte[] passwd, final int timeout);
    }

    public static final int DEFAULT_TICK_TIME = 3000;
    protected int tickTime = DEFAULT_TICK_TIME;
    /** value of -1 indicates unset, use default */
    protected int minSessionTimeout = -1;
    /** value of -1 indicates unset, use default */
    protected int maxSessionTimeout = -1;
    protected SessionTracker sessionTracker;
    private ZKDatabase zkDb;
    public final static Exception ok = new Exception("No prob");
    protected volatile boolean running;
    private final Callout m_callout;

    /**
     * This is the secret that we use to generate passwords, for the moment it
     * is more of a sanity check.
     */
    public static final long superSecret = 0XB3415C00L;

    int requestsInProcess;
    final List<ChangeRecord> outstandingChanges = new ArrayList<ChangeRecord>();
    // this data structure must be accessed under the outstandingChanges lock
    final HashMap<String, ChangeRecord> outstandingChangesForPath = new HashMap<String, ChangeRecord>();

    private NIOServerCnxn.Factory serverCnxnFactory;

    private final ServerStats serverStats;

    void removeCnxn(ServerCnxn cnxn) {
        this.closeSession(cnxn.getSessionId());
    }

    /**
     * Creates a ZooKeeperServer instance. It sets everything up, but doesn't
     * actually start listening for clients until run() is invoked.
     *
     * @param dataDir
     *            the directory to put the data
     * @throws IOException
     */
    public ZooKeeperServer(Callout callout, int tickTime,
            int minSessionTimeout, int maxSessionTimeout) throws IOException {
        serverStats = new ServerStats(this);
        this.zkDb = new ZKDatabase();
        this.tickTime = tickTime;
        this.minSessionTimeout = minSessionTimeout;
        this.maxSessionTimeout = maxSessionTimeout;
        this.m_callout = callout;
        LOG.info("Created server with tickTime " + tickTime
                + " minSessionTimeout " + getMinSessionTimeout()
                + " maxSessionTimeout " + getMaxSessionTimeout());
    }

    public ServerStats serverStats() {
        return serverStats;
    }

    public void dumpConf(PrintWriter pwriter) {
        pwriter.print("clientPort=");
        pwriter.println(getClientPort());
        pwriter.print("tickTime=");
        pwriter.println(getTickTime());
        pwriter.print("maxClientCnxns=");
        pwriter.println(serverCnxnFactory.getMaxClientCnxns());
        pwriter.print("minSessionTimeout=");
        pwriter.println(getMinSessionTimeout());
        pwriter.print("maxSessionTimeout=");
        pwriter.println(getMaxSessionTimeout());
    }

    /**
     * Default constructor, relies on the config for its agrument values
     *
     * @throws IOException
     */
    public ZooKeeperServer(Callout callout) throws IOException {
        this(callout, DEFAULT_TICK_TIME, -1, -1);
        // this(callout, 999999, 999999, 999999);
    }

    /**
     * get the zookeeper database for this server
     *
     * @return the zookeeper database for this server
     */
    public ZKDatabase getZKDatabase() {
        return this.zkDb;
    }

    /**
     * set the zkdatabase for this zookeeper server
     *
     * @param zkDb
     */
    public void setZKDatabase(ZKDatabase zkDb) {
        this.zkDb = zkDb;
    }

    long getTime() {
        return System.currentTimeMillis();
    }

    private void close(long sessionId) {
        submitRequest(null, sessionId, OpCode.closeSession, 0, null, null);
    }

    public void closeSession(long sessionId) {
        LOG.debug("Closing session 0x" + Long.toHexString(sessionId));

        // we do not want to wait for a session close. send it as soon as we
        // detect it!
        close(sessionId);
    }

    protected void killSession(long sessionId, long zxid) {
        zkDb.killSession(sessionId, zxid);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                    LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "ZooKeeperServer --- killSession: 0x"
                            + Long.toHexString(sessionId));
        }
        if (sessionTracker != null) {
            sessionTracker.removeSession(sessionId);
        }
    }

    @Override
    public void expire(long sessionId) {
        LOG.info("Initiating close of session 0x" + Long.toHexString(sessionId));
        close(sessionId);
    }

    public static class MissingSessionException extends IOException {
        private static final long serialVersionUID = 7467414635467261007L;

        public MissingSessionException(String msg) {
            super(msg);
        }
    }

    protected void registerJMX() {
        // register with JMX
        try {
            jmxServerBean = new ZooKeeperServerBean(this);
            MBeanRegistry.getInstance().register(jmxServerBean, null);

            try {
                jmxDataTreeBean = new DataTreeBean(zkDb.getDataTree());
                MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
            }
            catch (Exception e) {
                LOG.error("Failed to register with JMX.", e);
                jmxDataTreeBean = null;
            }
        }
        catch (InstanceAlreadyExistsException e) {
            LOG.error("Failed to register ZooKeeper with JMX due to a conflict. " +
            		"You are likely running two VoltDB instances on one system. " +
            		"This will only affect JMX management.");
            jmxServerBean = null;
        }
        catch (Exception e) {
            LOG.error("Failed to register with JMX.", e);
            jmxServerBean = null;
        }
    }

    public Thread startup() {
        createSessionTracker();
        Thread thisThread = new Thread(this, "ZooKeeperServer");
        thisThread.start();

        registerJMX();

        synchronized (this) {
            running = true;
            notifyAll();
        }
        return thisThread;
    }

    public void createSessionTracker() {
        sessionTracker = new SessionTrackerImpl(this,
                zkDb.getSessionWithTimeOuts());
    }

    public void closeSessions(long owner) {
        sessionTracker.expireSessionsWithOwner(owner);
    }

    public boolean isRunning() {
        return running;
    }

    public void shutdown() {
        unregisterJMX();
    }

    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
        jmxDataTreeBean = null;
    }

    synchronized public void incInProcess() {
        requestsInProcess++;
    }

    synchronized public void decInProcess() {
        requestsInProcess--;
    }

    public int getInProcess() {
        return requestsInProcess;
    }

    /**
     * This structure is used to facilitate information sharing between PrepRP
     * and FinalRP.
     */
    static class ChangeRecord {
        ChangeRecord(long zxid, String path, StatPersisted stat,
                int childCount, List<ACL> acl) {
            this.zxid = zxid;
            this.path = path;
            this.stat = stat;
            this.childCount = childCount;
            this.acl = acl;
        }

        long zxid;

        String path;

        StatPersisted stat; /* Make sure to create a new object when changing */

        int childCount;

        List<ACL> acl; /* Make sure to create a new object when changing */

        @SuppressWarnings("unchecked")
        ChangeRecord duplicate(long zxid) {
            StatPersisted stat = new StatPersisted();
            if (this.stat != null) {
                DataTree.copyStatPersisted(this.stat, stat);
            }
            return new ChangeRecord(zxid, path, stat, childCount,
                    acl == null ? new ArrayList<ACL>() : new ArrayList(acl));
        }
    }

    byte[] generatePasswd(long id) {
        Random r = new Random(id ^ superSecret);
        byte p[] = new byte[16];
        r.nextBytes(p);
        return p;
    }

    protected boolean checkPasswd(long sessionId, byte[] passwd) {
        return sessionId != 0
                && Arrays.equals(passwd, generatePasswd(sessionId));
    }

    void createSession(ServerCnxn cnxn, byte passwd[], int timeout)
            throws InterruptedException {
        Semaphore sessionIdCreated = m_callout.createSession(cnxn, passwd,
                timeout);
        sessionIdCreated.acquire();
    }

    public void closeSession(ServerCnxn cnxn, RequestHeader requestHeader) {
        closeSession(cnxn.getSessionId());
    }

    /**
     * @param cnxn
     * @param sessionId
     * @param xid
     * @param bb
     */
    public void submitRequest(ServerCnxn cnxn, long sessionId, int type,
            int xid, ByteBuffer bb, List<Id> authInfo) {
        Request si = new Request(cnxn, sessionId, xid, type, bb, authInfo);
        submitRequest(si);
    }

    public void submitRequest(Request si) {
        boolean validpacket = Request.isValid(si.type);
        if (validpacket) {
            m_callout.request(si);
            if (si.cnxn != null) {
                incInProcess();
            }
        } else {
            LOG.warn("Dropping packet at server of type " + si.type);
            // if invalid packet drop the packet.
        }
    }

    static public void byteBuffer2Record(ByteBuffer bb, Record record)
            throws IOException {
        BinaryInputArchive ia;
        ia = BinaryInputArchive.getArchive(new ByteBufferInputStream(bb));
        record.deserialize(ia, "request");
    }

    public static int getSnapCount() {
        String sc = System.getProperty("zookeeper.snapCount");
        try {
            return Integer.parseInt(sc);
        } catch (Exception e) {
            return 100000;
        }
    }

    public int getGlobalOutstandingLimit() {
        String sc = System.getProperty("zookeeper.globalOutstandingLimit");
        int limit;
        try {
            limit = Integer.parseInt(sc);
        } catch (Exception e) {
            limit = 1000;
        }
        return limit;
    }

    public void setServerCnxnFactory(NIOServerCnxn.Factory factory) {
        serverCnxnFactory = factory;
    }

    public NIOServerCnxn.Factory getServerCnxnFactory() {
        return serverCnxnFactory;
    }

    /**
     * return the last proceesed id from the datatree
     */
    @Override
    public long getLastProcessedZxid() {
        return zkDb.getDataTreeLastProcessedZxid();
    }

    /**
     * return the outstanding requests in the queue, which havent been processed
     * yet
     */
    @Override
    public long getOutstandingRequests() {
        return getInProcess();
    }

    public int getTickTime() {
        return tickTime;
    }

    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    public int getMinSessionTimeout() {
        return minSessionTimeout == -1 ? tickTime * 2 : minSessionTimeout;
    }

    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }

    public int getMaxSessionTimeout() {
        return maxSessionTimeout == -1 ? tickTime * 20 : maxSessionTimeout;
    }

    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }

    public int getClientPort() {
        return serverCnxnFactory != null ? serverCnxnFactory.ss.socket()
                .getLocalPort() : -1;
    }

    @Override
    public String getState() {
        return "standalone";
    }

    public void dumpEphemerals(PrintWriter pwriter) {
        zkDb.dumpEphemerals(pwriter);
    }

    void addChangeRecord(ChangeRecord c) {
        synchronized (outstandingChanges) {
            outstandingChanges.add(c);
            outstandingChangesForPath.put(c.path, c);
        }
    }

    ChangeRecord getRecordForPath(String path)
            throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        synchronized (outstandingChanges) {
            lastChange = outstandingChangesForPath.get(path);
            /*
             * for (int i = 0; i < zks.outstandingChanges.size(); i++) {
             * ChangeRecord c = zks.outstandingChanges.get(i); if
             * (c.path.equals(path)) { lastChange = c; } }
             */
            if (lastChange == null) {
                DataNode n = getZKDatabase().getNode(path);
                if (n != null) {
                    Long acl;
                    Set<String> children;
                    synchronized (n) {
                        acl = n.acl;
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path, n.stat,
                            children != null ? children.size() : 0,
                            getZKDatabase().convertLong(acl));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        return lastChange;
    }

    static void checkACL(List<ACL> acl, int perm, List<Id> ids)
            throws KeeperException.NoAuthException {
        if (skipACL) {
            return;
        }
        if (acl == null || acl.size() == 0) {
            return;
        }
        for (Id authId : ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world")
                        && id.getId().equals("anyone")) {
                    return;
                }
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap != null) {
                    for (Id authId : ids) {
                        if (authId.getScheme().equals(id.getScheme())
                                && ap.matches(authId.getId(), id.getId())) {
                            return;
                        }
                    }
                }
            }
        }
        throw new KeeperException.NoAuthException();
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    public void prepRequest(Request request, long txnId) {
        // LOG.info("Prep>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = 0x" + Long.toHexString(request.sessionId));
        TxnHeader txnHeader = null;
        Record txn = null;
        try {
            switch (request.type) {
            case OpCode.create:
                txnHeader = new TxnHeader(request.sessionId, request.cxid,
                        txnId, getTime(), OpCode.create);
                CreateRequest createRequest = new CreateRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        createRequest);
                String path = createRequest.getPath();
                int lastSlash = path.lastIndexOf('/');
                if (lastSlash == -1 || path.indexOf('\0') != -1) {
                    LOG.info("Invalid path " + path + " with session 0x"
                            + Long.toHexString(request.sessionId));
                    throw new KeeperException.BadArgumentsException(path);
                }
                if (!fixupACL(request.authInfo, createRequest.getAcl())) {
                    throw new KeeperException.InvalidACLException(path);
                }
                String parentPath = path.substring(0, lastSlash);
                ChangeRecord parentRecord = getRecordForPath(parentPath);

                checkACL(parentRecord.acl, ZooDefs.Perms.CREATE,
                        request.authInfo);
                int parentCVersion = parentRecord.stat.getCversion();
                CreateMode createMode = CreateMode.fromFlag(createRequest
                        .getFlags());
                if (createMode.isSequential()) {
                    path = path + String.format("%010d", parentCVersion);
                }
                try {
                    PathUtils.validatePath(path);
                } catch (IllegalArgumentException ie) {
                    LOG.info("Invalid path " + path + " with session 0x"
                            + Long.toHexString(request.sessionId));
                    throw new KeeperException.BadArgumentsException(path);
                }
                try {
                    if (getRecordForPath(path) != null) {
                        throw new KeeperException.NodeExistsException(path);
                    }
                } catch (KeeperException.NoNodeException e) {
                    // ignore this one
                }
                boolean ephemeralParent = parentRecord.stat.getEphemeralOwner() != 0;
                if (ephemeralParent) {
                    throw new KeeperException.NoChildrenForEphemeralsException(
                            path);
                }
                txn = new CreateTxn(path, createRequest.getData(),
                        createRequest.getAcl(), createMode.isEphemeral());
                StatPersisted s = new StatPersisted();
                if (createMode.isEphemeral()) {
                    s.setEphemeralOwner(request.sessionId);
                }
                parentRecord = parentRecord.duplicate(txnHeader.getZxid());
                parentRecord.childCount++;
                parentRecord.stat
                        .setCversion(parentRecord.stat.getCversion() + 1);
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(txnHeader.getZxid(), path, s,
                        0, createRequest.getAcl()));

                break;
            case OpCode.delete:
                txnHeader = new TxnHeader(request.sessionId, request.cxid,
                        txnId, getTime(), OpCode.delete);
                DeleteRequest deleteRequest = new DeleteRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        deleteRequest);
                path = deleteRequest.getPath();
                lastSlash = path.lastIndexOf('/');
                if (lastSlash == -1 || path.indexOf('\0') != -1
                        || getZKDatabase().isSpecialPath(path)) {
                    throw new KeeperException.BadArgumentsException(path);
                }
                parentPath = path.substring(0, lastSlash);
                parentRecord = getRecordForPath(parentPath);
                ChangeRecord nodeRecord = getRecordForPath(path);
                checkACL(parentRecord.acl, ZooDefs.Perms.DELETE,
                        request.authInfo);
                int version = deleteRequest.getVersion();
                if (version != -1 && nodeRecord.stat.getVersion() != version) {
                    throw new KeeperException.BadVersionException(path);
                }
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                txn = new DeleteTxn(path);
                parentRecord = parentRecord.duplicate(txnHeader.getZxid());
                parentRecord.childCount--;
                parentRecord.stat
                        .setCversion(parentRecord.stat.getCversion() + 1);
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(txnHeader.getZxid(), path,
                        null, -1, null));
                break;
            case OpCode.setData:
                txnHeader = new TxnHeader(request.sessionId, request.cxid,
                        txnId, getTime(), OpCode.setData);
                SetDataRequest setDataRequest = new SetDataRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        setDataRequest);
                path = setDataRequest.getPath();
                nodeRecord = getRecordForPath(path);
                checkACL(nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo);
                version = setDataRequest.getVersion();
                int currentVersion = nodeRecord.stat.getVersion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                txn = new SetDataTxn(path, setDataRequest.getData(), version);
                nodeRecord = nodeRecord.duplicate(txnHeader.getZxid());
                nodeRecord.stat.setVersion(version);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.setACL:
                txnHeader = new TxnHeader(request.sessionId, request.cxid,
                        txnId, getTime(), OpCode.setACL);
                SetACLRequest setAclRequest = new SetACLRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        setAclRequest);
                path = setAclRequest.getPath();
                if (!fixupACL(request.authInfo, setAclRequest.getAcl())) {
                    throw new KeeperException.InvalidACLException(path);
                }
                nodeRecord = getRecordForPath(path);
                checkACL(nodeRecord.acl, ZooDefs.Perms.ADMIN, request.authInfo);
                version = setAclRequest.getVersion();
                currentVersion = nodeRecord.stat.getAversion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                txn = new SetACLTxn(path, setAclRequest.getAcl(), version);
                nodeRecord = nodeRecord.duplicate(txnHeader.getZxid());
                nodeRecord.stat.setAversion(version);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.createSession:
                txnHeader = new TxnHeader(request.sessionId, request.cxid,
                        txnId, getTime(), OpCode.createSession);
                request.request.rewind();
                int to = request.request.getInt();
                txn = new CreateSessionTxn((Long)request.getOwner());
                request.request.rewind();
                sessionTracker.addSession(request.sessionId,
                        (Long) request.getOwner());
                break;
            case OpCode.closeSession:
                txnHeader = new TxnHeader(request.sessionId, request.cxid,
                        txnId, getTime(), OpCode.closeSession);
                // We don't want to do this check since the session expiration
                // thread
                // queues up this operation without being the session owner.
                // this request is the last of the session so it should be ok
                // zks.sessionTracker.checkSession(request.sessionId,
                // request.getOwner());
                HashSet<String> es = getZKDatabase().getEphemerals(
                        request.sessionId);
                synchronized (outstandingChanges) {
                    for (ChangeRecord c : outstandingChanges) {
                        if (c.stat == null) {
                            // Doing a delete
                            es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == request.sessionId) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(new ChangeRecord(txnHeader.getZxid(),
                                path2Delete, null, 0, null));
                    }
                }
                sessionTracker.removeSession(request.sessionId);
                LOG.info("Processed session termination for sessionid: 0x"
                        + Long.toHexString(request.sessionId));
                break;
            case OpCode.sync:
            case OpCode.exists:
            case OpCode.getData:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getChildren2:
            case OpCode.ping:
            case OpCode.setWatches:
            default:
            }
        } catch (KeeperException e) {
            if (txnHeader != null) {
                txnHeader.setType(OpCode.error);
                txn = new ErrorTxn(e.code().intValue());
            }
            LOG.debug("Got user-level KeeperException when processing "
                    + request.toString() + " Error Path:" + e.getPath()
                    + " Error:" + e.getMessage());
            request.setException(e);
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if (bb != null) {
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (txnHeader != null) {
                txnHeader.setType(OpCode.error);
                txn = new ErrorTxn(Code.MARSHALLINGERROR.intValue());
            }
        }
        request.hdr = txnHeader;
        request.txn = txn;
        request.zxid = txnId;
        executeRequest(request);
    }

    private void executeRequest(Request request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        // request.addRQRec(">final");
        long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
        if (request.type == OpCode.ping) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logRequest(LOG, traceMask, 'E', request, "");
        }
        ProcessTxnResult rc = null;
        synchronized (outstandingChanges) {
            while (!outstandingChanges.isEmpty()
                    && outstandingChanges.get(0).zxid <= request.zxid) {
                ChangeRecord cr = outstandingChanges.remove(0);
                if (cr.zxid < request.zxid) {
                    LOG.warn("Zxid outstanding " + cr.zxid
                            + " is less than current " + request.zxid);
                }
                if (outstandingChangesForPath.get(cr.path) == cr) {
                    outstandingChangesForPath.remove(cr.path);
                }
            }
            if (request.hdr != null) {
                rc = getZKDatabase().processTxn(request.hdr, request.txn);
            }
        }

        if (request.hdr != null && request.hdr.getType() == OpCode.closeSession) {
            Factory scxn = getServerCnxnFactory();
            // this might be possible since
            // we might just be playing diffs from the leader
            if (scxn != null && request.cnxn == null) {
                // calling this if we have the cnxn results in the client's
                // close session response being lost - we've already closed
                // the session/socket here before we can send the closeSession
                // in the switch block below
                scxn.closeSession(request.sessionId);
                return;
            }
        }

        if (request.cnxn == null) {
            return;
        }
        ServerCnxn cnxn = request.cnxn;

        String lastOp = "NA";
        decInProcess();
        Code err = Code.OK;
        Record rsp = null;
        boolean closeSession = false;
        try {
            if (request.hdr != null && request.hdr.getType() == OpCode.error) {
                throw KeeperException.create(KeeperException.Code
                        .get(((ErrorTxn) request.txn).getErr()));
            }

            KeeperException ke = request.getException();
            if (ke != null) {
                throw ke;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(request);
            }
            switch (request.type) {
            case OpCode.ping: {
                serverStats().updateLatency(request.createTime);

                lastOp = "PING";
                ((CnxnStats) cnxn.getStats()).updateForResponse(request.cxid,
                        request.zxid, lastOp, request.createTime,
                        System.currentTimeMillis());

                cnxn.sendResponse(new ReplyHeader(-2, getZKDatabase()
                        .getDataTreeLastProcessedZxid(), 0), null, "response");
                return;
            }
            case OpCode.createSession: {
                serverStats().updateLatency(request.createTime);

                lastOp = "SESS";
                ((CnxnStats) cnxn.getStats()).updateForResponse(request.cxid,
                        request.zxid, lastOp, request.createTime,
                        System.currentTimeMillis());

                cnxn.finishSessionInit(true);
                return;
            }
            case OpCode.create: {
                lastOp = "CREA";
                rsp = new CreateResponse(rc.path);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.delete: {
                lastOp = "DELE";
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setData: {
                lastOp = "SETD";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setACL: {
                lastOp = "SETA";
                rsp = new SetACLResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.closeSession: {
                lastOp = "CLOS";
                closeSession = true;
                err = Code.get(rc.err);
                break;
            }
            case OpCode.sync: {
                lastOp = "SYNC";
                SyncRequest syncRequest = new SyncRequest();
                ZooKeeperServer.byteBuffer2Record(request.request, syncRequest);
                rsp = new SyncResponse(syncRequest.getPath());
                break;
            }
            case OpCode.exists: {
                lastOp = "EXIS";
                // TODO we need to figure out the security requirement for this!
                ExistsRequest existsRequest = new ExistsRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        existsRequest);
                String path = existsRequest.getPath();
                if (path.indexOf('\0') != -1) {
                    throw new KeeperException.BadArgumentsException();
                }
                Stat stat = getZKDatabase().statNode(path,
                        existsRequest.getWatch() ? cnxn : null);
                rsp = new ExistsResponse(stat);
                break;
            }
            case OpCode.getData: {
                lastOp = "GETD";
                GetDataRequest getDataRequest = new GetDataRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        getDataRequest);
                DataNode n = getZKDatabase().getNode(getDataRequest.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                Long aclL;
                synchronized (n) {
                    aclL = n.acl;
                }
                checkACL(getZKDatabase().convertLong(aclL), ZooDefs.Perms.READ,
                        request.authInfo);
                Stat stat = new Stat();
                byte b[] = getZKDatabase().getData(getDataRequest.getPath(),
                        stat, getDataRequest.getWatch() ? cnxn : null);
                rsp = new GetDataResponse(b, stat);
                break;
            }
            case OpCode.setWatches: {
                lastOp = "SETW";
                SetWatches setWatches = new SetWatches();
                // XXX We really should NOT need this!!!!
                request.request.rewind();
                ZooKeeperServer.byteBuffer2Record(request.request, setWatches);
                long relativeZxid = setWatches.getRelativeZxid();
                getZKDatabase().setWatches(relativeZxid,
                        setWatches.getDataWatches(),
                        setWatches.getExistWatches(),
                        setWatches.getChildWatches(), cnxn);
                break;
            }
            case OpCode.getACL: {
                lastOp = "GETA";
                GetACLRequest getACLRequest = new GetACLRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        getACLRequest);
                Stat stat = new Stat();
                List<ACL> acl = getZKDatabase().getACL(getACLRequest.getPath(),
                        stat);
                rsp = new GetACLResponse(acl, stat);
                break;
            }
            case OpCode.getChildren: {
                lastOp = "GETC";
                GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        getChildrenRequest);
                DataNode n = getZKDatabase().getNode(
                        getChildrenRequest.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                Long aclG;
                synchronized (n) {
                    aclG = n.acl;

                }
                checkACL(getZKDatabase().convertLong(aclG), ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = getZKDatabase().getChildren(
                        getChildrenRequest.getPath(), null,
                        getChildrenRequest.getWatch() ? cnxn : null);
                rsp = new GetChildrenResponse(children);
                break;
            }
            case OpCode.getChildren2: {
                lastOp = "GETC";
                GetChildren2Request getChildren2Request = new GetChildren2Request();
                ZooKeeperServer.byteBuffer2Record(request.request,
                        getChildren2Request);
                Stat stat = new Stat();
                DataNode n = getZKDatabase().getNode(
                        getChildren2Request.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                Long aclG;
                synchronized (n) {
                    aclG = n.acl;
                }
                checkACL(getZKDatabase().convertLong(aclG), ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = getZKDatabase().getChildren(
                        getChildren2Request.getPath(), stat,
                        getChildren2Request.getWatch() ? cnxn : null);
                rsp = new GetChildren2Response(children, stat);
                break;
            }
            }
        } catch (SessionMovedException e) {
            // session moved is a connection level error, we need to tear
            // down the connection otw ZOOKEEPER-710 might happen
            // ie client on slow follower starts to renew session, fails
            // before this completes, then tries the fast follower (leader)
            // and is successful, however the initial renew is then
            // successfully fwd/processed by the leader and as a result
            // the client and leader disagree on where the client is most
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }

        ReplyHeader hdr = new ReplyHeader(request.cxid, request.zxid,
                err.intValue());

        serverStats().updateLatency(request.createTime);
        ((CnxnStats) cnxn.getStats()).updateForResponse(request.cxid,
                request.zxid, lastOp, request.createTime,
                System.currentTimeMillis());

        try {
            cnxn.sendResponse(hdr, rsp, "response");
            if (closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG", e);
        }
    }

    /**
     * This method checks out the acl making sure it isn't null or empty, it has
     * valid schemes and ids, and expanding any relative ids that depend on the
     * requestor's authentication information.
     *
     * @param authInfo
     *            list of ACL IDs associated with the client connection
     * @param acl
     *            list of ACLs being assigned to the node (create or setACL
     *            operation)
     * @return
     */
    private boolean fixupACL(List<Id> authInfo, List<ACL> acl) {
        if (skipACL) {
            return true;
        }
        if (acl == null || acl.size() == 0) {
            return false;
        }
        Iterator<ACL> it = acl.iterator();
        LinkedList<ACL> toAdd = null;
        while (it.hasNext()) {
            ACL a = it.next();
            Id id = a.getId();
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                // wide open
            } else if (id.getScheme().equals("auth")) {
                // This is the "auth" id, so we have to expand it to the
                // authenticated ids of the requestor
                it.remove();
                if (toAdd == null) {
                    toAdd = new LinkedList<ACL>();
                }
                boolean authIdValid = false;
                for (Id cid : authInfo) {
                    AuthenticationProvider ap = ProviderRegistry
                            .getProvider(cid.getScheme());
                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for "
                                + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        toAdd.add(new ACL(a.getPerms(), cid));
                    }
                }
                if (!authIdValid) {
                    return false;
                }
            } else {
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap == null) {
                    return false;
                }
                if (!ap.isValid(id.getId())) {
                    return false;
                }
            }
        }
        if (toAdd != null) {
            for (ACL a : toAdd) {
                acl.add(a);
            }
        }
        return acl.size() > 0;
    }

    @Override
    public void run() {
        try {
            m_callout.run();
        } finally {
            running = false;
        }
    }

}
