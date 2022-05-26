/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.planner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.compiler.AdHocPlannedStatement;

/**
 * CorePlan is an immutable representation of a SQL execution plan.
 * It refers to a parameterized statement, and so it could apply to
 * multiple SQL literal strings before constants have been pulled
 * out. It is usually embedded in an {@link AdHocPlannedStatement}
 * together with extracted parameter values, a SQL literal and more.
 */
public class CorePlan {

    /** The plan itself. Collector can be null. */
    public final byte[] aggregatorFragment;
    public final byte[] collectorFragment;

    /** hashes */
    public final byte[] aggregatorHash;
    public final byte[] collectorHash;

    /**
     * If true, divide the number of tuples changed
     * by the number of partitions, as the number will
     * be the sum of tuples changed on all replicas.
     */
    public final boolean isReplicatedTableDML;

    /** Does the statement write? */
    public final boolean readOnly;

    /** What SHA-1 hash of the catalog is this plan good for? */
    private final byte[] catalogHash;

    /** What are the types of the parameters this plan accepts? */
    public final VoltType[] parameterTypes;

    /**
     * If single partition, which of the parameters can be used
     * to determine the correct partition to route the transaction?
     * (Note, not serialized because it's not needed at the ExecutionSite.)
     */
    private int partitioningParamIndex = -1;
    private Object partitioningParamValue = null;
    private final CompiledPlan m_compiledPlan;

    /**
     * Constructor from QueryPlanner output.
     *
     * @param plan The output from the QueryPlanner.
     * @param catalogHash  The sha-1 hash of the catalog this plan was generated against.
     */
    public CorePlan(CompiledPlan plan, byte[] catalogHash) {
        m_compiledPlan = plan;
        aggregatorFragment = CompiledPlan.bytesForPlan(plan.rootPlanGraph, plan.getIsLargeQuery());
        collectorFragment = CompiledPlan.bytesForPlan(plan.subPlanGraph, plan.getIsLargeQuery());

        // compute the hashes
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(-1); // JVM is broken
        }
        md.update(aggregatorFragment);
        aggregatorHash = md.digest();
        if (collectorFragment != null) {
            md.reset();
            md.update(collectorFragment);
            collectorHash = md.digest();
        }
        else {
            collectorHash = null;
        }

        isReplicatedTableDML = plan.replicatedTableDML;
        this.catalogHash = catalogHash;
        parameterTypes = plan.parameterTypes();
        readOnly = plan.isReadOnly();
    }

    public void validate() {
        assert(aggregatorFragment != null);

        // dml => !readonly
        assert(! isReplicatedTableDML || ! readOnly);

        // repdml => 2partplan
        assert(! isReplicatedTableDML || collectorFragment != null);

        // zero param types => null extracted params
        // nonzero param types => param types and extracted params have same size
        assert parameterTypes != null;
        assert m_compiledPlan == null || m_compiledPlan.validate();
    }

    /***
     * Constructor, mainly for deserialization (but also testing)
     *
     * @param aggregatorFragment        planned aggregator fragment
     * @param collectorFragment         planned collector fragment
     * @param isReplicatedTableDML      replication flag
     * @param isReadOnly                does it write
     * @param paramTypes                parameter type array
     * @param catalogHash               SHA-1 hash of catalog
     */
    public CorePlan(byte[] aggregatorFragment,
                    byte[] collectorFragment,
                    byte[] aggregatorHash,
                    byte[] collectorHash,
                    boolean isReplicatedTableDML,
                    boolean isReadOnly,
                    VoltType[] paramTypes,
                    byte[] catalogHash) {
        m_compiledPlan = null;      // not reconstructing the CompiledPlan
        this.aggregatorFragment = aggregatorFragment;
        this.collectorFragment = collectorFragment;
        this.aggregatorHash = aggregatorHash;
        this.collectorHash = collectorHash;
        this.isReplicatedTableDML = isReplicatedTableDML;
        this.readOnly = isReadOnly;
        this.parameterTypes = paramTypes;
        this.catalogHash = catalogHash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("COMPILED PLAN {\n");
        sb.append("  ONE: ").append(aggregatorFragment == null ?
                "null" : new String(aggregatorFragment, Constants.UTF8ENCODING)).append("\n");
        sb.append("  ALL: ").append(collectorFragment == null ?
                "null" : new String(collectorFragment, Constants.UTF8ENCODING)).append("\n");
        sb.append("  RTD: ").append(isReplicatedTableDML ? "true" : "false").append("\n");
        sb.append("}");
        return sb.toString();
    }

    public int getSerializedSize() {
        // plan fragments first
        int size = 4 + aggregatorFragment.length + 20; // hash is 20b
        if (collectorFragment != null) {
            size += 4 + collectorFragment.length + 20; // hash is 20b
        }
        else {
            size += 4;
        }
        size += 2; // booleans
        size += 20;  // catalog hash SHA-1 is 20b

        size += 2; // params count
        size += parameterTypes.length;

        return size;
    }

    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        // plan fragments first
        buf.putInt(aggregatorFragment.length);
        buf.put(aggregatorFragment);
        buf.put(aggregatorHash);
        if (collectorFragment == null) {
            buf.putInt(-1);
        }
        else {
            buf.putInt(collectorFragment.length);
            buf.put(collectorFragment);
            buf.put(collectorHash);
        }

        // booleans
        buf.put((byte) (isReplicatedTableDML ? 1 : 0));
        buf.put((byte) (readOnly ? 1 : 0));

        // catalog hash
        buf.put(catalogHash);

        // param types
        buf.putShort((short) parameterTypes.length);
        for (VoltType type : parameterTypes) {
            buf.put(type.getValue());
        }
    }

    public static CorePlan fromBuffer(ByteBuffer buf) throws IOException {
        // plan fragments first
        byte[] aggregatorFragment = new byte[buf.getInt()];
        buf.get(aggregatorFragment);
        byte[] aggregatorHash = new byte[20]; // sha-1 hash is 20b
        buf.get(aggregatorHash);
        byte[] collectorFragment = null;
        byte[] collectorHash = null;
        int cflen = buf.getInt();
        if (cflen >= 0) {
            collectorFragment = new byte[cflen];
            buf.get(collectorFragment);
            collectorHash = new byte[20]; // sha-1 hash is 20b
            buf.get(collectorHash);
        }

        // booleans
        boolean isReplicatedTableDML = buf.get() == 1;
        boolean isReadOnly = buf.get() == 1;

        // catalog hash
        byte[] catalogHash = new byte[20];  // Catalog sha-1 hash is 20b
        buf.get(catalogHash);

        // param types
        short paramCount = buf.getShort();
        VoltType[] paramTypes = new VoltType[paramCount];
        for (int i = 0; i < paramCount; ++i) {
            paramTypes[i] = VoltType.get(buf.get());
        }

        return new CorePlan(
                aggregatorFragment,
                collectorFragment,
                aggregatorHash,
                collectorHash,
                isReplicatedTableDML,
                isReadOnly,
                paramTypes,
                catalogHash);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     *
     * Used mainly for debugging and for assertions.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CorePlan)) {
            return false;
        }
        CorePlan other = (CorePlan) obj;

        if (!Arrays.equals(aggregatorHash, other.aggregatorHash)) {
            return false;
        }
        if (!Arrays.equals(collectorHash, other.collectorHash)) {
            return false;
        }
        if (!Arrays.equals(parameterTypes, other.parameterTypes)) {
            return false;
        }
        if (isReplicatedTableDML != other.isReplicatedTableDML) {
            return false;
        }
        if (readOnly != other.readOnly) {
            return false;
        }
        if (!Arrays.equals(catalogHash, other.catalogHash)) {
            return false;
        }
        if (partitioningParamIndex != other.partitioningParamIndex) {
            return false;
        }

        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }

    public void setPartitioningParamIndex(int partitioningParamIndex) {
        this.partitioningParamIndex = partitioningParamIndex;
    }
    public int getPartitioningParamIndex() {
        return partitioningParamIndex;
    }
    public void setPartitioningParamValue(Object partitioningParamValue) {
        this.partitioningParamValue = partitioningParamValue;
    }
    public Object getPartitioningParamValue() {
        return partitioningParamValue;
    }

    public VoltType getPartitioningParamType() {
        if (partitioningParamIndex < 0 || partitioningParamIndex >= parameterTypes.length) {
            return VoltType.NULL;
        }
        return parameterTypes[partitioningParamIndex];
    }

    public boolean wasPlannedAgainstHash(byte[] catalogHash) {
        return Arrays.equals(catalogHash, this.catalogHash);
    }
}
