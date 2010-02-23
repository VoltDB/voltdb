/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.elt;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Hashtable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorDestinationInfo;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.SysManageable;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 * Bridges the connection to an OLAP system and the buffers passed
 * between the OLAP connection and the execution engine.
 */
public class ELTManager implements SysManageable {

    /** tables names created in external DBs are prepended with this string */
    public final static String TablePrefix = "VOLTDB_";

    @SuppressWarnings("unused")
    private static final Logger log =
        Logger.getLogger(ELTManager.class.getName(), VoltLoggerFactory.instance());

    private static final Logger eltLog =
        Logger.getLogger("ELT", VoltLoggerFactory.instance());

    /**
     * Store address along with ByteBuffer in a container that can return the ByteBuffer to the ELT manager
     */
    private class ELBBContainer extends BBContainer {
        private ELBBContainer(final ByteBuffer b, final long address) {
            super(b, address);
        }

        @Override
        public void discard() {
            synchronized (m_bufferPool) {
                m_bufferPool.offer(this);
            }
        }
     }

    /** Thrown if the initial setup the loader fails */
    public static class SetupException extends Exception {
        private static final long serialVersionUID = 1L;
        private final String m_msg;
        SetupException(final String msg) {
            m_msg = msg;
        }
        @Override
        public String getMessage() {
            return m_msg;
        }
    }

    /** Connection to an OLAP loader */
    ELTDataProcessor m_loader;

    /** Size of ELT buffers, currently 2MB */
    private static final int kBufferSize = 2 * 1024 * 1024;

    /** Pool of native byte buffers: EE writes, Loader reads. */
    ArrayDeque<ELBBContainer> m_bufferPool = new ArrayDeque<ELBBContainer>();

    /** Map buffer pointers to their containers for lookup */
    Hashtable<Long, ELBBContainer> m_bufferMap = new Hashtable<Long, ELBBContainer>();

    /** Obtain the global ELTManager via its instance() method */
    private static ELTManager m_self;

    /** Construct ELTManager using catalog.
     * @param catalog
     * @throws ELTManager.SetupException
     */
    public static synchronized void initialize(final Catalog catalog)
      throws ELTManager.SetupException
    {
        m_self = new ELTManager(catalog);
    }

    /**
     * Get the global instance of the ELTManager.
     * @return The global single instance of the ELTManager.
     */
    public static ELTManager instance() {
        assert (m_self != null);
        return m_self;
    }

    /** Read the catalog to setup manager and loader(s) */
    private ELTManager(final Catalog catalog) throws ELTManager.SetupException {
        final Cluster cluster = catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");

        if (conn == null) {
            return;
        }

        if (conn.getEnabled() == false) {
            eltLog.info("Export is disabled by user configuration.");
            return;
        }

        ConnectorDestinationInfo dest = conn.getDestinfo().get("0");
        if (dest == null) {
            assert(false); // VoltCompiler should make this impossible
            eltLog.info("Export is disabled - no specified destination");
            return;
        }

        String urlStr = dest.getUrl();
        final String elloader = conn.getLoaderclass();
        String elusername = dest.getUsername();
        String elpassword = dest.getPassword();

        final CatalogMap<Table> tables = cluster.getDatabases().get("database").getTables();

        try {
            // create a new loader; pass it catalog tables and start run-loop
            eltLog.info("Attempting to load connector " + elloader +
                        " for user " + elusername + " password " + elpassword);
            final Class<?> loaderClass = Class.forName(elloader);
            m_loader = (ELTDataProcessor)loaderClass.newInstance();
            m_loader.addLogger(eltLog);

            eltLog.info("Adding destination: " + urlStr);
            m_loader.addHost(urlStr, "database", elusername, elpassword);
            for (final Table table : tables) {
                m_loader.addTable("database",
                                  table.getTypeName(),
                                  table.getRelativeIndex());
            }
            m_loader.readyForData();
        }
        catch (final ClassNotFoundException e) {
            eltLog.l7dlog( Level.ERROR, LogKeys.elt_ELTManager_NoLoaderExtensions.name(), e);
            throw new ELTManager.SetupException(e.getMessage());
        }
        catch (final Throwable e) {
            throw new ELTManager.SetupException(e.getMessage());
        }
    }

    /** Internal helper to allocate a buffer from the byte pool */
    private ELBBContainer claimELBBContainer(final long desiredSizeInBytes) {
        ELBBContainer b;
        synchronized (m_bufferPool) {
            b = m_bufferPool.poll();
        }

        if (b == null) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(kBufferSize);
            final long address = DBBPool.getBufferAddress(buffer);
            b = new ELBBContainer( buffer, address);
        }
        assert b.address != 0;
        m_bufferMap.put(b.address, b);
        return b;
    }

    /**
     * Originate an empty buffer for the (IPC) EE.
     * @param desiredSizeInBytes The size of the desired buffer.
     * @return the allocated buffer.
     */
    public ByteBuffer claimBufferForEE2(final long desiredSizeInBytes) {
        ELBBContainer b = claimELBBContainer(desiredSizeInBytes);
        return b.b;
    }

    /**
     * Originate an empty buffer for the EE
     * @param desiredSizeInBytes The size of the buffer desired.
     * @return The address of the assigned buffer.
     */
    public long claimBufferForEE(final long desiredSizeInBytes) {
        ELBBContainer b = claimELBBContainer(desiredSizeInBytes);
        return b.address;
    }

    /**
     * Receive a completed buffer from the (IPC) EE and pass to the loader
     */
    public void handoffToConnection2(ByteBuffer buff, final int limit, final int tableId) {
        final long address = DBBPool.getBufferAddress(buff);
        handoffToConnection(address, limit, tableId);
    }

    /**
     * Receive a completed buffer from the EE and pass to the loader
     * @param buff
     * @param limit
     * @param tableId
     */
    public void handoffToConnection(final long buff, final int limit, final int tableId) {
        // translate EE's hint to java buffer semantics and handoff to
        // loader. if the loader doesn't exist or doesn't accept the
        // data, return the container to the pool manager and continue.
        boolean transfered = false;
        BBContainer tin = m_bufferMap.remove(buff);
        if (tin == null) {
            assert(false);
            throw new IllegalStateException("ELTManager asked to handoff unknown buffer " + buff);
        }

        tin.b.position(0);
        tin.b.limit(limit);

        final ELTDataBlock msg = new ELTDataBlock(tin, tableId);
        transfered = m_loader.process(msg);

        if (!transfered) {
            eltLog.l7dlog(Level.ERROR, LogKeys.elt_ELTManager_DataDroppedLoaderDead.name(), null);
        }
    }

    /** Release a buffer back to the pool manager
     * @param buff
     */
    public void releaseManagedBuffer(final long buff) {
        BBContainer tin = m_bufferMap.remove(buff);
        if (tin == null) {
            throw new IllegalStateException("ELTManager asked to handoff unknown buffer");
        }
        tin.discard();
        tin = null;
    }

    /**
     * Only detect if loaders have idled. This is meaningless unless
     * the questioner has flushed all ELT buffers from the EE to the ELT
     * Manager already.
     */
    @Override
    public String operStatus() {
        if (m_loader == null) {
            return SysManageable.IDLE;
        }
        else if (m_loader.isIdle()) {
            return SysManageable.IDLE;
        }
        else {
            return SysManageable.RUNNING;
        }
    }

    /**
     * Send a stop message to the processor. The EE buffers have already been
     * flushed at this point, assuming this is invoked by "@Quiesce" sysproc.
     */
    @Override
    public void quiesce() {
        if (m_loader != null) {
            m_loader.process(
                             new ELTDataBlock() {
                                 @Override
                                 public boolean isStopMessage() {
                                     return true;
                                 }
                             });
        }
    }
}
