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

package org.voltdb;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.SpScheduler.DurableMpUniqueIdListener;
import org.voltdb.iv2.SpScheduler.DurableSpUniqueIdListener;
import org.voltdb.licensetool.LicenseApi;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Stub class that provides a gateway to the InvocationBufferServer when
 * DR is enabled. If no DR, then it acts as a noop stub.
 *
 */
public class PartitionDRGateway implements DurableSpUniqueIdListener, DurableMpUniqueIdListener {
    private static final VoltLogger log = new VoltLogger("DR");

    public enum DRRecordType {
        INSERT, DELETE, UPDATE, BEGIN_TXN, END_TXN, TRUNCATE_TABLE, DELETE_BY_INDEX;

        public static final ImmutableMap<Integer, DRRecordType> conversion;
        static {
            ImmutableMap.Builder<Integer, DRRecordType> b = ImmutableMap.builder();
            for (DRRecordType t : DRRecordType.values()) {
                b.put(t.ordinal(), t);
            }
            conversion = b.build();
        }

        public static DRRecordType valueOf(int ordinal) {
            return conversion.get(ordinal);
        }
    }

    public static final Map<Integer, PartitionDRGateway> m_partitionDRGateways = new NonBlockingHashMap<>();

    /**
     * Load the full subclass if it should, otherwise load the
     * noop stub.
     * @param partitionId partition id
     * @param overflowDir
     * @return Instance of PartitionDRGateway
     */
    public static PartitionDRGateway getInstance(int partitionId,
                                                 ProducerDRGateway producerGateway,
                                                 StartAction startAction)
    {
        final VoltDBInterface vdb = VoltDB.instance();
        LicenseApi api = vdb.getLicenseApi();
        final boolean licensedToDR = api.isDrReplicationAllowed();

        // if this is a primary cluster in a DR-enabled scenario
        // try to load the real version of this class
        PartitionDRGateway pdrg = null;
        if (licensedToDR && producerGateway != null) {
            pdrg = tryToLoadProVersion();
        }
        if (pdrg == null) {
            pdrg = new PartitionDRGateway();
        }

        // init the instance and return
        try {
            pdrg.init(partitionId, producerGateway, startAction);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
        }
        m_partitionDRGateways.put(partitionId,  pdrg);

        return pdrg;
    }

    private static PartitionDRGateway tryToLoadProVersion()
    {
        try {
            Class<?> pdrgiClass = null;
            pdrgiClass = Class.forName("org.voltdb.dr2.PartitionDRGatewayImpl");
            Constructor<?> constructor = pdrgiClass.getConstructor();
            Object obj = constructor.newInstance();
            return (PartitionDRGateway) obj;
        } catch (Exception e) {
        }
        return null;
    }

    // empty methods for community edition
    protected void init(int partitionId,
                        ProducerDRGateway producerGateway,
                        StartAction startAction) throws IOException {}
    public void onSuccessfulProcedureCall(long txnId, long uniqueId, int hash,
                                          StoredProcedureInvocation spi,
                                          ClientResponseImpl response) {}
    public void onSuccessfulMPCall(long spHandle, long txnId, long uniqueId, int hash,
                                   StoredProcedureInvocation spi,
                                   ClientResponseImpl response) {}
    public void onBinaryDR(int partitionId, long startSequenceNumber, long lastSequenceNumber, long lastUniqueId, ByteBuffer buf) {
        final BBContainer cont = DBBPool.wrapBB(buf);
        DBBPool.registerUnsafeMemory(cont.address());
        cont.discard();
    }

    @Override
    public void lastSpUniqueIdMadeDurable(long uniqueId) {}

    @Override
    public void lastMpUniqueIdMadeDurable(long uniqueId) {}

    private static final ThreadLocal<AtomicLong> haveOpenTransactionLocal = new ThreadLocal<AtomicLong>() {
        @Override
        protected AtomicLong initialValue() {
            return new AtomicLong(-1);
        }
    };

    private static final ThreadLocal<AtomicLong> lastCommittedSpHandleTL = new ThreadLocal<AtomicLong>() {
        @Override
        protected AtomicLong initialValue() {
            return new AtomicLong(0);
        }
    };

    public static synchronized void pushDRBuffer(
            int partitionId,
            long startSequenceNumber,
            long lastSequenceNumber,
            long lastUniqueId,
            ByteBuffer buf) {
        if (startSequenceNumber == lastUniqueId) {
            log.trace("Received DR buffer size " + buf.remaining());
            AtomicLong haveOpenTransaction = haveOpenTransactionLocal.get();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            //Magic header space for Java for implementing zero copy stuff
            buf.position(8 + 65 + (partitionId == MpInitiator.MP_INIT_PID ? 0 : 4));
            while (buf.hasRemaining()) {
                int startPosition = buf.position();
                byte version = buf.get();
                int type = buf.get();

                int checksum = 0;
                if (version != 0) log.trace("Remaining is " + buf.remaining());

                DRRecordType recordType = DRRecordType.valueOf(type);
                switch (recordType) {
                case INSERT:
                case DELETE:
                case DELETE_BY_INDEX: {
                    //Insert
                    if (haveOpenTransaction.get() == -1) {
                        log.error("Have insert/delete but no open transaction");
                        break;
                    }
                    final long tableHandle = buf.getLong();
                    final int lengthPrefix = buf.getInt();
                    final int indexCrc;
                    if (recordType == DRRecordType.DELETE_BY_INDEX) {
                        indexCrc = buf.getInt();
                    } else {
                        indexCrc = 0;
                    }
                    buf.position(buf.position() + lengthPrefix);
                    checksum = buf.getInt();
                    log.trace("Version " + version + " type " + recordType + "table handle " + tableHandle + " length " + lengthPrefix + " checksum " + checksum +
                              (recordType == DRRecordType.DELETE_BY_INDEX ? (" index checksum " + indexCrc) : ""));
                    break;
                }
                case BEGIN_TXN: {
                    //Begin txn
                    final long txnId = buf.getLong();
                    final long spHandle = buf.getLong();
                    if (haveOpenTransaction.get() != -1) {
                        log.error("Have open transaction txnid " + txnId + " spHandle " + spHandle + " but already open transaction");
                        break;
                    }
                    haveOpenTransaction.set(spHandle);
                    checksum = buf.getInt();
                    log.trace("Version " + version + " type BEGIN_TXN " + " txnid " + txnId + " spHandle " + spHandle + " checksum " + checksum);
                    break;
                }
                case END_TXN: {
                    //End txn
                    final long spHandle = buf.getLong();
                    if (haveOpenTransaction.get() == -1 ) {
                        log.error("Have end transaction spHandle " + spHandle + " but no open transaction and its less then last committed " + lastCommittedSpHandleTL.get().get());
                        break;
                    }
                    haveOpenTransaction.set(-1);
                    lastCommittedSpHandleTL.get().set(spHandle);
                    checksum = buf.getInt();
                    log.trace("Version " + version + " type END_TXN " + " spHandle " + spHandle + " checksum " + checksum);
                    break;
                }
                case TRUNCATE_TABLE: {
                    final long tableHandle = buf.getLong();
                    final byte tableNameBytes[] = new byte[buf.getInt()];
                    buf.get(tableNameBytes);
                    final String tableName = new String(tableNameBytes, Charsets.UTF_8);
                    checksum = buf.getInt();
                    log.trace("Version " + version + " type TRUNCATE_TABLE table handle " + tableHandle + " table name " + tableName);
                    break;
                }
                }
                int calculatedChecksum = DBBPool.getBufferCRC32C(buf, startPosition, buf.position() - startPosition - 4);
                if (calculatedChecksum != checksum) {
                    log.error("Checksum " + calculatedChecksum + " didn't match " + checksum);
                    break;
                }

            }
        }

        final PartitionDRGateway pdrg = m_partitionDRGateways.get(partitionId);
        if (pdrg == null) {
            VoltDB.crashLocalVoltDB("No PRDG when there should be", true, null);
        }
        pdrg.onBinaryDR(partitionId, startSequenceNumber, lastSequenceNumber, lastUniqueId, buf);
    }

    public void forceAllDRNodeBuffersToDisk(final boolean nofsync) {}
}
