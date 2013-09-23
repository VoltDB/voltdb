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

package org.voltdb;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.sysprocs.saverestore.HashinatorSnapshotData;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.voltdb.dtxn.UndoAction;

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

    /**
     * Uncompressed configuration data accessor.
     * @return configuration data bytes
     */
    public abstract byte[] getConfigBytes();

    /**
     * Implementer should return compressed (cooked) bytes for serialization.
     * @return config bytes
     * @throws IOException
     */
    public abstract byte[] getCookedBytes() throws IOException;

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
        instance.set(Pair.of(0L, constructHashinator( hashinatorImplementation, config, false)));
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
    abstract protected Pair<HashinatorType, byte[]> pGetCurrentConfig();
    abstract public Map<Long, Integer> pPredecessors(int partition);
    abstract public Pair<Long, Integer> pPredecessor(int partition, long token);
    abstract public Map<Long, Long> pGetRanges(int partition);

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
     * Given a long value, pick a partition to store the data. It's only called for legacy
     * hashinator, elastic hashinator hashes all types the same way through hashinateBytes().
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int hashinateLong(long value) {
        return instance.get().getSecond().pHashinateLong(value);
    }

    /**
     * Given an byte[] bytes, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int hashinateBytes(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return instance.get().getSecond().pHashinateBytes(bytes);
        }
    }

    /**
     * Given an object, map it to a partition.
     * DON'T EVER MAKE ME PUBLIC
     */
    private static int hashToPartition(Object obj) {
        HashinatorType type = getConfiguredHashinatorType();
        if (type == HashinatorType.LEGACY) {
            // Annoying, legacy hashes numbers and bytes differently, need to preserve that.
            if (obj == null || VoltType.isNullVoltType(obj)) {
                return 0;
            } else if (obj instanceof Long) {
                long value = ((Long) obj).longValue();
                return hashinateLong(value);
            } else if (obj instanceof Integer) {
                long value = ((Integer)obj).intValue();
                return hashinateLong(value);
            } else if (obj instanceof Short) {
                long value = ((Short)obj).shortValue();
                return hashinateLong(value);
            } else if (obj instanceof Byte) {
                long value = ((Byte)obj).byteValue();
                return hashinateLong(value);
            }
        }
        return hashinateBytes(valueToBytes(obj));
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
    private static Object bytesToValue(VoltType type, byte[] value) {
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
     * NOTE NOTE NOTE NOTE!  THIS SHOULD BE THE ONLY WAY THAT YOU FIGURE OUT THE PARTITIONING
     * FOR A PARAMETER!
     * @return The partition best set up to execute the procedure.
     * @throws VoltTypeException
     */
    public static int getPartitionForParameter(int partitionType, Object invocationParameter)
        throws VoltTypeException
    {
        final VoltType partitionParamType = VoltType.get((byte)partitionType);

        // Special cases:
        // 1) if the user supplied a string for a number column,
        // try to do the conversion. This makes it substantially easier to
        // load CSV data or other untyped inputs that match DDL without
        // requiring the loader to know precise the schema.
        // 2) For legacy hashinators, if we have a numeric column but the param is in a byte
        // array, convert the byte array back to the numeric value
        if (invocationParameter != null && partitionParamType.isPartitionableNumber()) {
            if (invocationParameter.getClass() == String.class) {
                {
                    Object tempParam = ParameterConverter.stringToLong(
                            invocationParameter,
                            partitionParamType.classFromType());
                    // Just in case someone managed to feed us a non integer
                    if (tempParam != null) {
                        invocationParameter = tempParam;
                    }
                }
            }
            else if (getConfiguredHashinatorType() == HashinatorType.LEGACY &&
                     invocationParameter.getClass() == byte[].class) {
                invocationParameter = bytesToValue(partitionParamType, (byte[])invocationParameter);
            }
        }

        return hashToPartition(invocationParameter);
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
    public static UndoAction updateHashinator(
            Class<? extends TheHashinator> hashinatorImplementation,
            long version,
            byte configBytes[],
            boolean cooked) {
        while (true) {
            final Pair<Long, ? extends TheHashinator> snapshot = instance.get();
            if (version > snapshot.getFirst()) {
                Pair<Long, ? extends TheHashinator> update =
                        Pair.of(version, constructHashinator(hashinatorImplementation, configBytes, cooked));
                if (instance.compareAndSet(snapshot, update)) {
                    return new UndoAction() {
                        @Override
                        public void release() {}

                        @Override
                        public void undo() {
                            instance.set(snapshot);
                        }
                    };
                }
            } else {
                return new UndoAction() {

                    @Override
                    public void release() {}

                    @Override
                    public void undo() {}
                };
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
            hashinatorType = System.getProperty("HASHINATOR", HashinatorType.LEGACY.name());
        }
        if (hostLogger.isDebugEnabled()) {
            hostLogger.debug("Overriding hashinator to use " + hashinatorType);
        }
        configuredHashinatorType = HashinatorType.valueOf(hashinatorType.trim().toUpperCase());
        return configuredHashinatorType;
    }

    public static void setConfiguredHashinatorType(HashinatorType type) {
        configuredHashinatorType = type;
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
            return ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        }
        throw new RuntimeException("Should not reach here");
    }

    public static Pair<HashinatorType, byte[]> getCurrentConfig() {
        return instance.get().getSecond().pGetCurrentConfig();
    }

    public static Map<Long, Integer> predecessors(int partition) {
        return instance.get().getSecond().pPredecessors(partition);
    }

    public static Pair<Long, Integer> predecessor(int partition, long token) {
        return instance.get().getSecond().pPredecessor(partition, token);
    }

    /**
     * Get the ranges the given partition is assigned to.
     * @param partition
     * @return A map of ranges, the key is the start of a range, the value is
     * the corresponding end. Ranges returned in the map are [start, end).
     * The ranges may or may not be contiguous.
     */
    public static Map<Long, Long> getRanges(int partition) {
        return instance.get().getSecond().pGetRanges(partition);
    }

    /**
     * Get optimized configuration data for wire serialization.
     * @return optimized configuration data
     * @throws IOException
     */
    public static synchronized HashinatorSnapshotData serializeConfiguredHashinator()
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
     * @return UndoAction Undo action to revert hashinator update
     */
    public static UndoAction updateConfiguredHashinator(long version, byte config[]) {
        return updateHashinator(getConfiguredHashinatorClass(), version, config, true);
    }

    public static Pair<Long, byte[]> getCurrentVersionedConfig()
    {
        Pair<Long, ? extends TheHashinator> currentHashinator = instance.get();
        return Pair.of(currentHashinator.getFirst(), currentHashinator.getSecond().pGetCurrentConfig().getSecond());
    }
}
