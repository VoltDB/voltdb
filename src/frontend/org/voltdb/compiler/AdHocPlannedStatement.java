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

package org.voltdb.compiler;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.planner.CompiledPlan;

/**
 * Holds the plan and original SQL source for a single statement.
 *
 * Will typically be contained by AdHocPlannedStmtBatch. Both this class and the batch extend
 * AsyncCompilerResult to allow working at either the batch or the individual statement level.
 */
public class AdHocPlannedStatement implements Cloneable {
    public byte[] sql;
    public byte[] aggregatorFragment = null;
    public byte[] collectorFragment = null;
    public boolean isReplicatedTableDML;
    public boolean isNonDeterministic;
    public boolean readOnly;
    public int catalogVersion;
    public VoltType[] parameterTypes;
    public ParameterSet extractedParamValues;
    public Object partitionParam; // not serialized

    AdHocPlannedStatement(CompiledPlan plan) {
        sql = plan.sql.getBytes(VoltDB.UTF8ENCODING);
        aggregatorFragment = CompiledPlan.bytesForPlan(plan.rootPlanGraph);
        collectorFragment = CompiledPlan.bytesForPlan(plan.subPlanGraph);
        isReplicatedTableDML = plan.replicatedTableDML;
        isNonDeterministic = (!plan.isContentDeterministic()) || (!plan.isOrderDeterministic());
        catalogVersion = -1;
        parameterTypes = plan.parameters;
        extractedParamValues = plan.extractedParamValues;
        readOnly = plan.readOnly;
        partitionParam = plan.getPartitioningKey();

        validate();
    }

    /***
     * Constructor
     *
     * @param sql                       SQL statement source
     * @param aggregatorFragment        planned aggregator fragment
     * @param collectorFragment         planned collector fragment
     * @param isReplicatedTableDML      replication flag
     * @param isNonDeterministic        non-deterministic SQL flag
     * @param isReadOnly                does it write
     * @param paramTypes                parameter type array
     * @param extractedParamValues      params extracted from constant values
     * @param catalogVersion            catalog version
     */
    public AdHocPlannedStatement(byte[] sql,
                                 byte[] aggregatorFragment,
                                 byte[] collectorFragment,
                                 boolean isReplicatedTableDML,
                                 boolean isNonDeterministic,
                                 boolean isReadOnly,
                                 VoltType[] paramTypes,
                                 ParameterSet extractedParamValues,
                                 int catalogVersion) {
        this.sql = sql;
        this.aggregatorFragment = aggregatorFragment;
        this.collectorFragment = collectorFragment;
        this.isReplicatedTableDML = isReplicatedTableDML;
        this.isNonDeterministic = isNonDeterministic;
        this.readOnly = isReadOnly;
        this.parameterTypes = paramTypes;
        this.extractedParamValues = extractedParamValues;
        this.catalogVersion = catalogVersion;

        // as this constructor is used for deserializaton on the proc-running side,
        // no partitioning param object is needed

        validate();
    }

    private void validate() {
        assert(aggregatorFragment != null);

        // nondet => readonly
        assert((isNonDeterministic == false) || (readOnly == true));

        // dml => !readonly
        assert((isReplicatedTableDML == false) || (readOnly == false));

        // repdml => 2partplan
        assert((isReplicatedTableDML == false) || (collectorFragment != null));

        // zero param types => null extracted params
        // nonzero param types => param types and extracted params have same size
        assert(parameterTypes != null);
        assert(extractedParamValues != null);
        // any extracted params => extracted param size == param type array size
        assert((extractedParamValues.size() == 0) || (extractedParamValues.size() == parameterTypes.length));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("COMPILED PLAN {\n");
        sb.append("  SQL: ").append((sql != null) ? new String(sql, VoltDB.UTF8ENCODING) : "null").append("\n");
        sb.append("  ONE: ").append(aggregatorFragment == null ? "null" : new String(aggregatorFragment, VoltDB.UTF8ENCODING)).append("\n");
        sb.append("  ALL: ").append(collectorFragment == null ? "null" : new String(collectorFragment, VoltDB.UTF8ENCODING)).append("\n");
        sb.append("  RTD: ").append(isReplicatedTableDML ? "true" : "false").append("\n");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public int getSerializedSize() {
        int size = 2 + sql.length;
        size += 4 + aggregatorFragment.length;
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

        size += extractedParamValues.getSerializedSize();

        return size;
    }

    void flattenToBuffer(ByteBuffer buf) throws IOException {
        validate(); // assertions for extra safety

        buf.putShort((short) sql.length);
        buf.put(sql);

        buf.putInt(aggregatorFragment.length);
        buf.put(aggregatorFragment);

        if (collectorFragment == null) {
            buf.putInt(-1);
        }
        else {
            buf.putInt(collectorFragment.length);
            buf.put(collectorFragment);
        }

        buf.put((byte) (isReplicatedTableDML ? 1 : 0));
        buf.put((byte) (isNonDeterministic ? 1 : 0));
        buf.put((byte) (readOnly ? 1 : 0));

        buf.putInt(catalogVersion);

        buf.putShort((short) parameterTypes.length);
        for (VoltType type : parameterTypes) {
            buf.put(type.getValue());
        }

        extractedParamValues.flattenToBuffer(buf);
    }

    public static AdHocPlannedStatement fromBuffer(ByteBuffer buf) throws IOException {
        byte[] sql = new byte[buf.getShort()];
        buf.get(sql);

        byte[] aggregatorFragment = new byte[buf.getInt()];
        buf.get(aggregatorFragment);

        byte[] collectorFragment = null;
        int cflen = buf.getInt();
        if (cflen >= 0) {
            collectorFragment = new byte[cflen];
            buf.get(collectorFragment);
        }

        boolean isReplicatedTableDML = buf.get() == 1;
        boolean isNonDeterministic = buf.get() == 1;
        boolean isReadOnly = buf.get() == 1;

        int catalogVersion = buf.getInt();

        short paramCount = buf.getShort();
        VoltType[] paramTypes = new VoltType[paramCount];
        for (int i = 0; i < paramCount; ++i) {
            paramTypes[i] = VoltType.get(buf.get());
        }

        ParameterSet parameterSet = new ParameterSet();
        FastDeserializer fds = new FastDeserializer(buf);
        parameterSet.readExternal(fds);

        return new AdHocPlannedStatement(
                sql,
                aggregatorFragment,
                collectorFragment,
                isReplicatedTableDML,
                isNonDeterministic,
                isReadOnly,
                paramTypes,
                parameterSet,
                catalogVersion);
    }
}
