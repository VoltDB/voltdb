/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.planner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.voltdb.VoltDB;
import org.voltdb.VoltType;
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

    /**
     * If true, divide the number of tuples changed
     * by the number of partitions, as the number will
     * be the sum of tuples changed on all replicas.
     */
    public final boolean isReplicatedTableDML;

    /** Should results be exactly the same across partitions? */
    public final boolean isNonDeterministic;

    /** Does the statement write? */
    public final boolean readOnly;

    /** Which version of the catalog is this plan good for? */
    public final int catalogVersion;

    /** What are the types of the paramters this plan accepts? */
    public final VoltType[] parameterTypes;

    /**
     * If single partition, which of the parameters can be used
     * to determine the correct partition to route the transaction?
     * (Note, not serialized because it's not needed at the ExecutionSite.)
     */
    public final int partitioningParamIndex;

    /**
     * Constructor from QueryPlanner output.
     *
     * @param plan The output from the QueryPlanner.
     * @param catalogVersion The version of the catalog this plan was generated against.
     */
    public CorePlan(CompiledPlan plan, int catalogVersion) {
        aggregatorFragment = CompiledPlan.bytesForPlan(plan.rootPlanGraph);
        collectorFragment = CompiledPlan.bytesForPlan(plan.subPlanGraph);
        isReplicatedTableDML = plan.replicatedTableDML;
        isNonDeterministic = (!plan.isContentDeterministic()) || (!plan.isOrderDeterministic());
        this.catalogVersion = catalogVersion;
        parameterTypes = plan.parameters;
        readOnly = plan.readOnly;
        partitioningParamIndex = plan.partitioningKeyIndex;
    }

    /***
     * Constructor, mainly for deserialization (but also testing)
     *
     * @param aggregatorFragment        planned aggregator fragment
     * @param collectorFragment         planned collector fragment
     * @param isReplicatedTableDML      replication flag
     * @param isNonDeterministic        non-deterministic SQL flag
     * @param isReadOnly                does it write
     * @param paramTypes                parameter type array
     * @param catalogVersion            catalog version
     */
    public CorePlan(byte[] aggregatorFragment,
                    byte[] collectorFragment,
                    boolean isReplicatedTableDML,
                    boolean isNonDeterministic,
                    boolean isReadOnly,
                    VoltType[] paramTypes,
                    int catalogVersion) {
        this.aggregatorFragment = aggregatorFragment;
        this.collectorFragment = collectorFragment;
        this.isReplicatedTableDML = isReplicatedTableDML;
        this.isNonDeterministic = isNonDeterministic;
        this.readOnly = isReadOnly;
        this.parameterTypes = paramTypes;
        this.catalogVersion = catalogVersion;
        partitioningParamIndex = -1; // invalid after de-serialization
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("COMPILED PLAN {\n");
        sb.append("  ONE: ").append(aggregatorFragment == null ?
                "null" : new String(aggregatorFragment, VoltDB.UTF8ENCODING)).append("\n");
        sb.append("  ALL: ").append(collectorFragment == null ?
                "null" : new String(collectorFragment, VoltDB.UTF8ENCODING)).append("\n");
        sb.append("  RTD: ").append(isReplicatedTableDML ? "true" : "false").append("\n");
        sb.append("}");
        return sb.toString();
    }

    public int getSerializedSize() {
        // plan fragments first
        int size = 4 + aggregatorFragment.length;
        if (collectorFragment != null) {
            size += 4 + collectorFragment.length;
        }
        else {
            size += 4;
        }
        size += 3; // booleans
        size += 4; // catalog version

        size += 2; // params count
        size += parameterTypes.length;

        return size;
    }

    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        // plan fragments first
        buf.putInt(aggregatorFragment.length);
        buf.put(aggregatorFragment);
        if (collectorFragment == null) {
            buf.putInt(-1);
        }
        else {
            buf.putInt(collectorFragment.length);
            buf.put(collectorFragment);
        }

        // booleans
        buf.put((byte) (isReplicatedTableDML ? 1 : 0));
        buf.put((byte) (isNonDeterministic ? 1 : 0));
        buf.put((byte) (readOnly ? 1 : 0));

        // catalog version
        buf.putInt(catalogVersion);

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
        byte[] collectorFragment = null;
        int cflen = buf.getInt();
        if (cflen >= 0) {
            collectorFragment = new byte[cflen];
            buf.get(collectorFragment);
        }

        // booleans
        boolean isReplicatedTableDML = buf.get() == 1;
        boolean isNonDeterministic = buf.get() == 1;
        boolean isReadOnly = buf.get() == 1;

        // catalog version
        int catalogVersion = buf.getInt();

        // param types
        short paramCount = buf.getShort();
        VoltType[] paramTypes = new VoltType[paramCount];
        for (int i = 0; i < paramCount; ++i) {
            paramTypes[i] = VoltType.get(buf.get());
        }

        return new CorePlan(
                aggregatorFragment,
                collectorFragment,
                isReplicatedTableDML,
                isNonDeterministic,
                isReadOnly,
                paramTypes,
                catalogVersion);
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

        if (!Arrays.equals(aggregatorFragment, other.aggregatorFragment)) {
            return false;
        }
        if (!Arrays.equals(collectorFragment, other.collectorFragment)) {
            return false;
        }
        if (!Arrays.equals(parameterTypes, other.parameterTypes)) {
            return false;
        }
        if (isNonDeterministic != other.isNonDeterministic) {
            return false;
        }
        if (isReplicatedTableDML != other.isReplicatedTableDML) {
            return false;
        }
        if (readOnly != other.readOnly) {
            return false;
        }
        if (catalogVersion != other.catalogVersion) {
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
}
