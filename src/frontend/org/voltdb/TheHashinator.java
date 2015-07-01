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
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google_voltpatches.common.base.*;
import com.google_voltpatches.common.collect.MapMaker;
import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.sysprocs.saverestore.HashinatorSnapshotData;

/**
 * Class that maps object values to partitions. It's rather simple
 * really. It'll get more complicated if you give it time.
 */
public abstract class TheHashinator {

    public static enum HashinatorType {
        LEGACY(0, LegacyHashinator.class)
        , ELASTIC(1, ElasticHashinator.class);

        public final int typeId;
        public final Class<? extends TheHashinator> hashinatorClass;
        private HashinatorType(int typeId, Class<? extends TheHashinator> hashinatorClass) {
            this.typeId = typeId;
            this.hashinatorClass = hashinatorClass;
        }
        public int typeId() {
            return typeId;
        }
    };

    public static class HashinatorConfig {
        public final HashinatorType type;
        public final byte configBytes[];
        public final long configPtr;
        public final int numTokens;
        public HashinatorConfig(HashinatorType type, byte configBytes[], long configPtr, int numTokens) {
            this.type = type;
            this.configBytes = configBytes;
            this.configPtr = configPtr;
            this.numTokens = numTokens;
        }
    }

    /**
     * Uncompressed configuration data accessor.
     * @return configuration data bytes
     */
    public abstract byte[] getConfigBytes();

    /**
     * A map to store hashinators, including older versions of hashinators.
     * During live rejoin sites will replay balance fragments in an order that results
     * in some sites skipping ahead to newer hashinators.
     *
     * References are weak so this has no impact on memory utilization
     */
    private static final ConcurrentMap<Long, TheHashinator> m_cachedHashinators =
            new MapMaker().weakValues().concurrencyLevel(1).initialCapacity(16).makeMap();

    /**
     * Return compressed (cooked) bytes for serialization.
     * Defaults to providing raw bytes, e.g. for legacy.
     * @return cooked config bytes
     */
    public byte[] getCookedBytes()
    {
        return getConfigBytes();
    }

    protected static final VoltLogger hostLogger = new VoltLogger("HOST");

     /*
     * Stamped instance, version associated with hash function, only update for newer versions
     */
    private static final AtomicReference<Pair<Long, ? extends TheHashinator>> instance =
            new AtomicReference<Pair<Long, ? extends TheHashinator>>();

    /**
     * Initialize TheHashinator with the specified implementation class and configuration.
     * The starting version number will be 0.
     */
    public static void initialize(Class<? extends TheHashinator> hashinatorImplementation, byte config[]) {
        TheHashinator hashinator = constructHashinator( hashinatorImplementation, config, false);
        m_cachedHashinators.put(0L, hashinator);
        instance.set(Pair.of(0L, hashinator));
    }

    /**
     * Get TheHashinator instanced based on knwon implementation and configuration.
     * Used by client after asking server what it is running.
     *
     * @param hashinatorImplementation
     * @param config
     * @return
     */
    public static TheHashinator getHashinator(Class<? extends TheHashinator> hashinatorImplementation,
            byte config[], boolean cooked) {
        return constructHashinator(hashinatorImplementation, config, cooked);
    }

    /**
     * Helper method to do the reflection boilerplate to call the constructor
     * of the selected hashinator and convert the exceptions to runtime exceptions.
     * @param hashinatorImplementation  hashinator class
     * @param configBytes  config data (raw or cooked)
     * @param cooked  true if configBytes is cooked, i.e. in wire serialization format
     * @return  the constructed hashinator
     */
    public static TheHashinator
        constructHashinator(
                Class<? extends TheHashinator> hashinatorImplementation,
                byte configBytes[], boolean cooked) {
        try {
            Constructor<? extends TheHashinator> constructor =
                    hashinatorImplementation.getConstructor(byte[].class, boolean.class);
            return constructor.newInstance(configBytes, cooked);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return null;
    }

    /**
     * Protected methods that implement hashination of specific data types.
     * Only string/varbinary and integer hashination is supported. String/varbinary
     * get the same handling once they the string is converted to UTF-8 binary
     * so there is only one protected method for bytes.
     *
     * Longs are converted to bytes in little endian order for elastic, modulus for legacy.
     */
    abstract public int pHashinateLong(long value);
    abstract public int pHashinateBytes(byte[] bytes);
    abstract public long pGetConfigurationSignature();
    abstract public HashinatorConfig pGetCurrentConfig();
    abstract public Map<Integer, Integer> pPredecessors(int partition);
    abstract public Pair<Integer, Integer> pPredecessor(int partition, int token);
    abstract public Map<Integer, Integer> pGetRanges(int partition);
    public abstract HashinatorType getConfigurationType();
    abstract public int pHashToPartition(VoltType type, Object obj);
    abstract protected Set<Integer> pGetPartitions();

    /**
     * Returns the configuration signature
     * @return the configuration signature
     */
    static public long getConfigurationSignature() {
        return instance.get().getSecond().pGetConfigurationSignature();
    }

    /**
     * It computes a signature from the given configuration bytes
     * @param config configuration byte array
     * @return signature from the given configuration bytes
     */
    static public long computeConfigurationSignature(byte [] config) {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(config);
        return crc.getValue();
    }

    /**
     * Given an byte[] bytes, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    int hashinateBytes(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return pHashinateBytes(bytes);
        }
    }

    /**
     * Given an object, map it to a partition. DON'T EVER MAKE ME PUBLIC
     */
    private static int hashToPartition(TheHashinator hashinator, VoltType type, Object obj) {
        return hashinator.pHashToPartition(type, obj);

    }

    /**
     * Converts the object into bytes for hashing.
     * @param obj
     * @return null if the obj is null or is a Volt null type.
     */
    public static byte[] valueToBytes(Object obj) {
        long value = 0;
        byte[] retval = null;

        if (VoltType.isNullVoltType(obj)) {
            return null;
        } else if (obj instanceof Long) {
            value = ((Long) obj).longValue();
        } else if (obj instanceof String ) {
            retval = ((String) obj).getBytes(Charsets.UTF_8);
        } else if (obj instanceof Integer) {
            value = ((Integer)obj).intValue();
        } else if (obj instanceof Short) {
            value = ((Short)obj).shortValue();
        } else if (obj instanceof Byte) {
            value = ((Byte)obj).byteValue();
        } else if (obj instanceof byte[]) {
            retval = (byte[]) obj;
        }

        if (retval == null) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(value);
            retval = buf.array();
        }

        return retval;
    }

    /**
     * Converts a byte array with type back to the original partition value.
     * This is the inverse of {@see TheHashinator#valueToBytes(Object)}.
     * @param type VoltType of partition parameter.
     * @param value Byte array representation of partition parameter.
     * @return Java object of the correct type.
     */
    protected static Object bytesToValue(VoltType type, byte[] value) {
        if ((type == VoltType.NULL) || (value == null)) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.wrap(value);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        switch (type) {
        case BIGINT:
            return buf.getLong();
        case STRING:
            return new String(value, Charsets.UTF_8);
        case INTEGER:
            return buf.getInt();
        case SMALLINT:
            return buf.getShort();
        case TINYINT:
            return buf.get();
        case VARBINARY:
            return value;
        default:
            throw new RuntimeException(
                    "TheHashinator#bytesToValue failed to convert a non-partitionable type.");
        }
    }

    /**
     * Given the type of the targeting partition parameter and an object,
     * coerce the object to the correct type and hash it.
     * NOTE NOTE NOTE NOTE! THIS SHOULD BE THE ONLY WAY THAT
     * YOU FIGURE OUT THE PARTITIONING FOR A PARAMETER! ON SERVER
     *
     * @return The partition best set up to execute the procedure.
     * @throws VoltTypeException
     */
    public static int getPartitionForParameter(VoltType partitionType, Object invocationParameter) {
        return instance.get().getSecond().getHashedPartitionForParameter(partitionType, invocationParameter);
    }

    /**
     * Given the type of the targeting partition parameter and an object,
     * coerce the object to the correct type and hash it.
     * NOTE NOTE NOTE NOTE! THIS SHOULD BE THE ONLY WAY THAT
     * YOU FIGURE OUT THE PARTITIONING FOR A PARAMETER! ON SERVER
     *
     * @return The partition best set up to execute the procedure.
     * @throws VoltTypeException
     */
    public static int getPartitionForParameter(int partitionType, Object invocationParameter)
            throws VoltTypeException
    {
        return instance.get().getSecond().getHashedPartitionForParameter(partitionType, invocationParameter);
    }

    /**
     * Given the type of the targeting partition parameter and an object,
     * coerce the object to the correct type and hash it.
     * NOTE NOTE NOTE NOTE! THIS SHOULD BE THE ONLY WAY THAT YOU FIGURE OUT
     * THE PARTITIONING FOR A PARAMETER! THIS IS SHARED BY SERVER AND CLIENT
     * CLIENT USES direct instance method as it initializes its own per connection
     * Hashinator.
     *
     * @return The partition best set up to execute the procedure.
     * @throws VoltTypeException
     */
    public int getHashedPartitionForParameter(int partitionValueType, Object partitionValue) {
        final VoltType partitionParamType = VoltType.get((byte) partitionValueType);
        return getHashedPartitionForParameter(partitionParamType, partitionValue);
    }

    /**
     * Given the type of the targeting partition parameter and an object,
     * coerce the object to the correct type and hash it.
     * NOTE NOTE NOTE NOTE! THIS SHOULD BE THE ONLY WAY THAT YOU FIGURE OUT
     * THE PARTITIONING FOR A PARAMETER! THIS IS SHARED BY SERVER AND CLIENT
     * CLIENT USES direct instance method as it initializes its own per connection
     * Hashinator.
     *
     * @return The partition best set up to execute the procedure.
     * @throws VoltTypeException
     */
    public int getHashedPartitionForParameter(VoltType partitionParamType, Object partitionValue)
            throws VoltTypeException {
        // Special cases:
        // 1) if the user supplied a string for a number column,
        // try to do the conversion. This makes it substantially easier to
        // load CSV data or other untyped inputs that match DDL without
        // requiring the loader to know precise the schema.
        // 2) For legacy hashinators, if we have a numeric column but the param is in a byte
        // array, convert the byte array back to the numeric value
        if (partitionValue != null && partitionParamType.isPartitionableNumber()) {
            if (partitionValue.getClass() == String.class) {
                {
                    Object tempParam = ParameterConverter.stringToLong(
                            partitionValue,
                            partitionParamType.classFromType());
                    // Just in case someone managed to feed us a non integer
                    if (tempParam != null) {
                        partitionValue = tempParam;
                    }
                }
            }
            else if (partitionValue.getClass() == byte[].class) {
                partitionValue = bytesToValue(partitionParamType, (byte[]) partitionValue);
            }
        }

        return hashToPartition(this, partitionParamType, partitionValue);
    }

    /**
     * Update the hashinator in a thread safe manner with a newer version of the hash function.
     * A version number must be provided and the new config will only be used if it is greater than
     * the current version of the hash function.
     *
     * Returns an action for undoing the hashinator update
     * @param hashinatorImplementation  hashinator class
     * @param version  hashinator version/txn id
     * @param configBytes  config data (format determined by cooked flag)
     * @param cooked  compressible wire serialization format if true
     */
    public static Pair<? extends UndoAction, TheHashinator> updateHashinator(
            Class<? extends TheHashinator> hashinatorImplementation,
            long version,
            byte configBytes[],
            boolean cooked) {
        //Use a cached/canonical hashinator if possible
        TheHashinator existingHashinator = m_cachedHashinators.get(version);
        if (existingHashinator == null) {
            existingHashinator = constructHashinator(hashinatorImplementation, configBytes, cooked);
            TheHashinator tempVal = m_cachedHashinators.putIfAbsent( version, existingHashinator);
            if (tempVal != null) existingHashinator = tempVal;
        }

        //Do a CAS loop to maintain a global instance
        while (true) {
            final Pair<Long, ? extends TheHashinator> snapshot = instance.get();
            if (version > snapshot.getFirst()) {
                final Pair<Long, ? extends TheHashinator> update =
                        Pair.of(version, existingHashinator);
                if (instance.compareAndSet(snapshot, update)) {
                    return Pair.of(new UndoAction() {
                        @Override
                        public void release() {}

                        @Override
                        public void undo() {
                            boolean rolledBack = instance.compareAndSet(update, snapshot);
                            if (!rolledBack) {
                                hostLogger.info(
                                        "Didn't roll back hashinator because it wasn't set to expected hashinator");
                            }
                        }
                    }, existingHashinator);
                }
            } else {
                return Pair.of(new UndoAction() {

                    @Override
                    public void release() {}

                    @Override
                    public void undo() {}
                }, existingHashinator);
            }
        }
    }

    /**
     * By default returns LegacyHashinator.class, but for development another hashinator
     * can be specified using the environment variable HASHINATOR
     */
    public static Class<? extends TheHashinator> getConfiguredHashinatorClass() {
        HashinatorType type = getConfiguredHashinatorType();
        switch (type) {
        case LEGACY:
            return LegacyHashinator.class;
        case ELASTIC:
            return ElasticHashinator.class;
        }
        throw new RuntimeException("Should not reach here");
    }

    private static volatile HashinatorType configuredHashinatorType = null;

    /**
     * By default returns HashinatorType.LEGACY, but for development another hashinator
     * can be specified using the environment variable or the Java property HASHINATOR
     */
    public static HashinatorType getConfiguredHashinatorType() {
        if (configuredHashinatorType != null) {
            return configuredHashinatorType;
        }
        String hashinatorType = System.getenv("HASHINATOR");
        if (hashinatorType == null) {
            hashinatorType = System.getProperty("HASHINATOR", HashinatorType.ELASTIC.name());
        }
        if (hostLogger.isDebugEnabled()) {
            hostLogger.debug("Overriding hashinator to use " + hashinatorType);
        }
        configuredHashinatorType = HashinatorType.valueOf(hashinatorType.trim().toUpperCase());
        return configuredHashinatorType;
    }

    /**
     * This is only called by server client should never call this.
     *
     * @param type
     */
    public static void setConfiguredHashinatorType(HashinatorType type) {
        if (System.getenv("HASHINATOR") == null && System.getProperty("HASHINATOR") == null) {
            configuredHashinatorType = type;
        } else {
            hostLogger.info("Ignoring manually specified hashinator type " + type +
                            " in favor of environment/property type " + getConfiguredHashinatorType());
        }
    }

    /**
     * Get a basic configuration for the currently selected hashinator type based
     * on the current partition count. If Elastic is in play
     */
    public static byte[] getConfigureBytes(int partitionCount) {
        HashinatorType type = getConfiguredHashinatorType();
        switch (type) {
        case LEGACY:
            return LegacyHashinator.getConfigureBytes(partitionCount);
        case ELASTIC:
            return ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
        }
        throw new RuntimeException("Should not reach here");
    }

    public static HashinatorConfig getCurrentConfig() {
        return instance.get().getSecond().pGetCurrentConfig();
    }

    public static TheHashinator getCurrentHashinator() {
        return instance.get().getSecond();
    }

    public static Map<Integer, Integer> predecessors(int partition) {
        return instance.get().getSecond().pPredecessors(partition);
    }

    public static Pair<Integer, Integer> predecessor(int partition, int token) {
        return instance.get().getSecond().pPredecessor(partition, token);
    }

    /**
     * Get the ranges the given partition is assigned to.
     * @param partition
     * @return A map of ranges, the key is the start of a range, the value is
     * the corresponding end. Ranges returned in the map are [start, end).
     * The ranges may or may not be contiguous.
     */
    public static Map<Integer, Integer> getRanges(int partition) {
        return instance.get().getSecond().pGetRanges(partition);
    }

    /**
     * Get optimized configuration data for wire serialization.
     * @return optimized configuration data
     * @throws IOException
     */
    public static HashinatorSnapshotData serializeConfiguredHashinator()
            throws IOException
    {
        HashinatorSnapshotData hashData = null;
        Pair<Long, ? extends TheHashinator> currentInstance = instance.get();
        switch (getConfiguredHashinatorType()) {
          case LEGACY:
            break;
          case ELASTIC: {
            byte[] cookedData = currentInstance.getSecond().getCookedBytes();
            hashData = new HashinatorSnapshotData(cookedData, currentInstance.getFirst());
            break;
          }
        }
        return hashData;
    }

    /**
     * Update the current configured hashinator class. Used by snapshot restore.
     * @param version
     * @param config
     * @return Pair<UndoAction, TheHashinator> Undo action to revert hashinator update and the hashinator that was
     *         requested which may actually be an older one (although the one requested) during live rejoin
     */
    public static Pair< ? extends UndoAction, TheHashinator> updateConfiguredHashinator(long version, byte config[]) {
        return updateHashinator(getConfiguredHashinatorClass(), version, config, true);
    }

    public static Pair<Long, byte[]> getCurrentVersionedConfig()
    {
        Pair<Long, ? extends TheHashinator> currentHashinator = instance.get();
        return Pair.of(currentHashinator.getFirst(), currentHashinator.getSecond().pGetCurrentConfig().configBytes);
    }

    /**
     * Get the current version/config in compressed (wire) format.
     * @return version/config pair
     */
    public static Pair<Long, byte[]> getCurrentVersionedConfigCooked()
    {
        Pair<Long, ? extends TheHashinator> currentHashinator = instance.get();
        Long version = currentHashinator.getFirst();
        byte[] bytes = currentHashinator.getSecond().getCookedBytes();
        return Pair.of(version, bytes);
    }

    public static final String CNAME_PARTITION_KEY = "PARTITION_KEY";

    private Supplier<VoltTable> getSupplierForType(final VoltType type) {
        return new Supplier() {
            @Override
            public Object get() {
                VoltTable vt = new VoltTable(new VoltTable.ColumnInfo[] {
                        new VoltTable.ColumnInfo(
                                VoltSystemProcedure.CNAME_PARTITION_ID,
                                VoltSystemProcedure.CTYPE_ID),
                        new VoltTable.ColumnInfo(CNAME_PARTITION_KEY, type)});
                Set<Integer> partitions = TheHashinator.this.pGetPartitions();

                for (int ii = 0; ii < 500000; ii++) {
                    if (partitions.isEmpty()) break;
                    Object value = null;
                    if (type == VoltType.INTEGER) value = ii;
                    if (type == VoltType.STRING) value = String.valueOf(ii);
                    if (type == VoltType.VARBINARY) {
                        ByteBuffer buf = ByteBuffer.allocate(4);
                        buf.putInt(ii);
                        value = buf.array();
                    }
                    int partition = TheHashinator.this.getHashedPartitionForParameter(type.getValue(), value);
                    if (partitions.remove(partition)) {
                        vt.addRow(partition, value);
                    }
                }
                return vt;
            }
        };
    }
    private final Supplier<VoltTable> m_integerPartitionKeys = Suppliers.memoize(getSupplierForType(VoltType.INTEGER));
    private final Supplier<VoltTable> m_stringPartitionKeys = Suppliers.memoize(getSupplierForType(VoltType.STRING));
    private final Supplier<VoltTable> m_varbinaryPartitionKeys = Suppliers.memoize(getSupplierForType(VoltType.VARBINARY));

    /**
     * Get a VoltTable containing the partition keys for each partition that can be found.
     * May be missing some partitions during elastic rebalance when the partitions don't own
     * enough of the ring to be probed
     *
     * If the type is not supported returns null
     * @param type
     * @return
     */
    public static VoltTable getPartitionKeys(VoltType type) {
        TheHashinator hashinator = instance.get().getSecond();
        switch (type) {
            case INTEGER:
                return hashinator.m_integerPartitionKeys.get();
            case STRING:
                return hashinator.m_stringPartitionKeys.get();
            case VARBINARY:
                return hashinator.m_varbinaryPartitionKeys.get();
            default:
                return null;
        }
    }
}
