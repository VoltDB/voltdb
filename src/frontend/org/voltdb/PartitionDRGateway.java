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
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.licensetool.LicenseApi;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Stub class that provides a gateway to the InvocationBufferServer when
 * DR is enabled. If no DR, then it acts as a noop stub.
 *
 */
public class PartitionDRGateway {

    public enum DRRecordType {
        INSERT, DELETE, UPDATE, BEGIN_TXN, END_TXN;

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

    /**
     * Load the full subclass if it should, otherwise load the
     * noop stub.
     * @param partitionId partition id
     * @param overflowDir
     * @return Instance of PartitionDRGateway
     */
    public static PartitionDRGateway getInstance(int partitionId,
                                                 NodeDRGateway nodeGateway,
                                                 boolean isRejoin)
    {
        final VoltDBInterface vdb = VoltDB.instance();
        LicenseApi api = vdb.getLicenseApi();
        final boolean licensedToDR = api.isDrReplicationAllowed();

        // if this is a primary cluster in a DR-enabled scenario
        // try to load the real version of this class
        PartitionDRGateway pdrg = null;
        if (licensedToDR && nodeGateway != null) {
            pdrg = tryToLoadProVersion();
        }
        if (pdrg == null) {
            pdrg = new PartitionDRGateway();
        }

        // init the instance and return
        try {
            pdrg.init(partitionId, nodeGateway, isRejoin);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
        }
        return pdrg;
    }

    private static PartitionDRGateway tryToLoadProVersion()
    {
        try {
            Class<?> pdrgiClass = null;
            if (Boolean.getBoolean("USE_DR_V2")) {
                pdrgiClass = Class.forName("org.voltdb.dr2.PartitionDRGatewayImpl");
            } else {
                pdrgiClass = Class.forName("org.voltdb.dr.PartitionDRGatewayImpl");
            }
            Constructor<?> constructor = pdrgiClass.getConstructor();
            Object obj = constructor.newInstance();
            return (PartitionDRGateway) obj;
        } catch (Exception e) {
        }
        return null;
    }

    // empty methods for community edition
    protected void init(int partitionId,
                        NodeDRGateway gateway,
                        boolean isRejoin) throws IOException {}
    public void onSuccessfulProcedureCall(long txnId, long uniqueId, int hash,
                                          StoredProcedureInvocation spi,
                                          ClientResponseImpl response) {}
    public void onSuccessfulMPCall(long spHandle, long txnId, long uniqueId, int hash,
                                   StoredProcedureInvocation spi,
                                   ClientResponseImpl response) {}
    public void tick(long txnId) {}

    private static final ThreadLocal<AtomicLong> haveOpenTransactionLocal = new ThreadLocal<AtomicLong>() {
        @Override
        protected AtomicLong initialValue() {
            return new AtomicLong(-1);
        }
    };

    private static final ThreadLocal<AtomicLong> lastCommittedSpHandle = new ThreadLocal<AtomicLong>() {
        @Override
        protected AtomicLong initialValue() {
            return new AtomicLong(0);
        }
    };

    private static final boolean logDebug = false;

    public static synchronized void pushDRBuffer(int partitionId, ByteBuffer buf) {
        if (logDebug) {
            System.out.println("Received DR buffer size " + buf.remaining());
            AtomicLong haveOpenTransaction = haveOpenTransactionLocal.get();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            //Magic header space for Java for implementing zero copy stuff
            buf.position(8);
            while (buf.hasRemaining()) {
                int startPosition = buf.position();
                byte version = buf.get();
                int type = buf.get();

                int checksum = 0;
                if (version != 0) System.out.println("Remaining is " + buf.remaining());

                switch (DRRecordType.valueOf(type)) {
                case INSERT: {
                    //Insert
                    if (haveOpenTransaction.get() == -1) {
                        System.out.println("Have insert but no open transaction");
                        System.exit(-1);
                    }
                    final long tableHandle = buf.getLong();
                    final int lengthPrefix = buf.getInt();
                    buf.position(buf.position() + lengthPrefix);
                    checksum = buf.getInt();
                    System.out.println("Version " + version + " type INSERT table handle " + tableHandle + " length " + lengthPrefix + " checksum " + checksum);
                    break;
                }
                case DELETE: {
                    //Delete
                    if (haveOpenTransaction.get() == -1) {
                        System.out.println("Have insert but no open transaction");
                        System.exit(-1);
                    }
                    final long tableHandle = buf.getLong();
                    final int lengthPrefix = buf.getInt();
                    buf.position(buf.position() + lengthPrefix);
                    checksum = buf.getInt();
                    System.out.println("Version " + version + " type DELETE table handle " + tableHandle + " length " + lengthPrefix + " checksum " + checksum);
                    break;
                }
                case UPDATE:
                    //Update
                    //System.out.println("Version " + version + " type UPDATE " + checksum " + checksum);
                    break;
                case BEGIN_TXN: {
                    //Begin txn
                    final long txnId = buf.getLong();
                    final long spHandle = buf.getLong();
                    if (haveOpenTransaction.get() != -1) {
                        System.out.println("Have open transaction txnid " + txnId + " spHandle " + spHandle + " but already open transaction");
                        System.exit(-1);
                    }
                    haveOpenTransaction.set(spHandle);
                    checksum = buf.getInt();
                    System.out.println("Version " + version + " type BEGIN_TXN " + " txnid " + txnId + " spHandle " + spHandle + " checksum " + checksum);
                    break;
                }
                case END_TXN: {
                    //End txn
                    final long spHandle = buf.getLong();
                    if (haveOpenTransaction.get() == -1 ) {
                        System.out.println("Have end transaction spHandle " + spHandle + " but no open transaction and its less then last committed " + lastCommittedSpHandle.get().get());
    //                    checksum = buf.getInt();
    //                    break;
                        System.exit(-1);
                    }
                    haveOpenTransaction.set(-1);
                    lastCommittedSpHandle.get().set(spHandle);
                    checksum = buf.getInt();
                    System.out.println("Version " + version + " type END_TXN " + " spHandle " + spHandle + " checksum " + checksum);
                    break;
                }
                }
                int calculatedChecksum = DBBPool.getBufferCRC32C(buf, startPosition, buf.position() - startPosition - 4);
                if (calculatedChecksum != checksum) {
                    System.out.println("Checksum " + calculatedChecksum + " didn't match " + checksum);
                    System.exit(-1);
                }
            }
        }
        final BBContainer cont = DBBPool.wrapBB(buf);
        DBBPool.registerUnsafeMemory(cont.address());
        cont.discard();
    }
}
