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
import org.voltdb.licensetool.LicenseApi;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Stub class that provides a gateway to the InvocationBufferServer when
 * DR is enabled. If no DR, then it acts as a noop stub.
 *
 */
public class PartitionDRGateway {
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
    public long onBinaryDR(int partitionId, long startSequenceNumber, long lastSequenceNumber, long lastUniqueId, ByteBuffer buf) {
        final BBContainer cont = DBBPool.wrapBB(buf);
        DBBPool.registerUnsafeMemory(cont.address());
        cont.discard();
        return -1;
    }

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

    public static synchronized long pushDRBuffer(
            int partitionId,
            long startSequenceNumber,
            long lastSequenceNumber,
            long lastUniqueId,
            ByteBuffer buf) {
        return -1;
    }

    public void forceAllDRNodeBuffersToDisk(final boolean nofsync) {}
}
