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

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;

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

    private static final VoltLogger hostLogger = new VoltLogger("HOST");

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
        instance.set(Pair.of(0L, constructHashinator( hashinatorImplementation, config)));
    }

    /**
     * Helper method to do the reflection boilerplate to call the constructor
     * of the selected hashinator and convert the exceptions to runtime excetions.
     */
    private static TheHashinator
        constructHashinator(
                Class<? extends TheHashinator> hashinatorImplementation,
                byte config[]) {
        try {
            Constructor<? extends TheHashinator> constructor = hashinatorImplementation.getConstructor(byte[].class);
            return constructor.newInstance(config);
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
     * The same trick can't be done for longs because modulus is used by the legacy
     * hashinator
     */
    abstract protected int pHashinateLong(long value);
    abstract protected int pHashinateBytes(byte[] bytes);
    abstract protected Pair<HashinatorType, byte[]> pGetCurrentConfig();

    /**
     * Given a long value, pick a partition to store the data.
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
        return instance.get().getSecond().pHashinateBytes(bytes);
    }

    /**
     * Given an String value, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int hashinateString(String value) {
        return instance.get().getSecond().pHashinateBytes(value.getBytes(Charsets.UTF_8));
    }

    /**
     * Given an object, map it to a partition.
     * @param obj The object to be mapped to a partition.
     * @return The id of the partition desired.
     */
    public static int hashToPartition(Object obj) {
        int index = 0;
        if (obj == null || VoltType.isNullVoltType(obj))
        {
            index = 0;
        }
        else if (obj instanceof Long) {
            long value = ((Long) obj).longValue();
            index = hashinateLong(value);
        } else if (obj instanceof String ) {
            index = hashinateString((String) obj);
        } else if (obj instanceof Integer) {
            long value = ((Integer)obj).intValue();
            index = hashinateLong(value);
        } else if (obj instanceof Short) {
            long value = ((Short)obj).shortValue();
            index = hashinateLong(value);
        } else if (obj instanceof Byte) {
            long value = ((Byte)obj).byteValue();
            index = hashinateLong(value);
        } else if (obj instanceof byte[]) {
            index = hashinateBytes((byte[]) obj );
        }
        return index;
    }

    /**
     * Update the hashinator in a thread safe manner with a newer version of the hash function.
     * A version number must be provided and the new config will only be used if it is greater than
     * the current version of the hash function.
     */
    public static void updateHashinator(
            Class<? extends TheHashinator> hashinatorImplementation, long version, byte config[]) {
        while (true) {
            final Pair<Long, ? extends TheHashinator> snapshot = instance.get();
            if (version > snapshot.getFirst()) {
                Pair<Long, ? extends TheHashinator> update =
                        Pair.of(version, constructHashinator(hashinatorImplementation, config));
                if (instance.compareAndSet(snapshot, update)) return;
            } else {
                return;
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

    /**
     * By default returns HashinatorType.LEGACY, but for development another hashinator
     * can be specified using the environment variable HASHINATOR
     */
    public static HashinatorType getConfiguredHashinatorType() {
        String hashinatorType = System.getenv("HASHINATOR");
        if (hashinatorType == null) {
            return HashinatorType.LEGACY;
        } else {
            hostLogger.info("Overriding hashinator to use " + hashinatorType);
            return HashinatorType.valueOf(hashinatorType.trim().toUpperCase());
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
            return ElasticHashinator.getConfigureBytes(partitionCount, 8);
        }
        throw new RuntimeException("Should not reach here");
    }

    public static Pair<HashinatorType, byte[]> getCurrentConfig() {
        return instance.get().getSecond().pGetCurrentConfig();
    }
}
