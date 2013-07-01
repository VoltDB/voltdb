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

package org.voltdb.compiler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.voltdb.ParameterSet;
import org.voltdb.common.Constants;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CorePlan;

/**
 * Holds the plan and original SQL source for a single statement.
 *
 * Will typically be contained by AdHocPlannedStmtBatch. Both this class and the batch extend
 * AsyncCompilerResult to allow working at either the batch or the individual statement level.
 */
public class AdHocPlannedStatement {
    public final CorePlan core;
    public final byte[] sql;
    public final ParameterSet extractedParamValues;
    private final int[] boundParamIndexes;
    public final String[] extractedParamStrings;
    private String[] boundParamStrings;
    public final Object partitionParam; // not serialized

    AdHocPlannedStatement(CompiledPlan plan, int catalogVersion, String[] extractedLiterals) {
        sql = plan.sql.getBytes(Constants.UTF8ENCODING);
        core = new CorePlan(plan, catalogVersion);
        extractedParamValues = plan.extractedParamValues;
        boundParamIndexes = plan.boundParamIndexes();
        extractedParamStrings = extractedLiterals;
        partitionParam = plan.getPartitioningKey();

        validate();
    }

    /***
     * Constructor
     *
     * @param sql                       bytes of sql string (utf-8)
     * @param core                      core immutable plan
     * @param extractedParamValues      params extracted from constant values
     * @param partitionParam            value used for partitioning
     */
    public AdHocPlannedStatement(byte[] sql,
                                 CorePlan core,
                                 ParameterSet extractedParamValues,
                                 String[] extractedParamStrings,
                                 String[] constants,
                                 Object partitionParam) {

        this.sql = sql;
        this.core = core;
        this.extractedParamValues = extractedParamValues;
        this.boundParamIndexes = null;
        this.extractedParamStrings = extractedParamStrings;
        this.boundParamStrings = constants;
        this.partitionParam = partitionParam;

        // When this constructor is used for deserializaton on the proc-running side,
        // the bound param and extracted param string constants and the partitioning param object are not required.

        validate();
    }

    private void validate() {
        assert(core != null);
        assert(core.aggregatorFragment != null);

        // nondet => readonly
        assert((core.isNonDeterministic == false) || (core.readOnly == true));

        // dml => !readonly
        assert((core.isReplicatedTableDML == false) || (core.readOnly == false));

        // repdml => 2partplan
        assert((core.isReplicatedTableDML == false) || (core.collectorFragment != null));

        // zero param types => null extracted params
        // nonzero param types => param types and extracted params have same size
        assert(core.parameterTypes != null);
        assert(extractedParamValues != null);
        // any extracted params => extracted param size == param type array size
        assert((extractedParamValues.size() == 0) ||
                (extractedParamValues.size() == core.parameterTypes.length));
    }

    @Override
    public String toString() {
        return core.toString();
    }

    public int getSerializedSize() {
        // plan
        int size = core.getSerializedSize();

        // sql bytes
        size += 2;
        size += sql.length;

        // params
        size += extractedParamValues.getSerializedSize();

        return size;
    }

    void flattenToBuffer(ByteBuffer buf) throws IOException {
        validate(); // assertions for extra safety

        // plan
        core.flattenToBuffer(buf);

        // sql bytes
        buf.putShort((short) sql.length);
        buf.put(sql);

        // params
        extractedParamValues.flattenToBuffer(buf);
    }

    public static AdHocPlannedStatement fromBuffer(ByteBuffer buf) throws IOException {
        // plan
        CorePlan core = CorePlan.fromBuffer(buf);

        // sql bytes
        short sqlLength = buf.getShort();
        byte[] sql = new byte[sqlLength];
        buf.get(sql);

        // params
        FastDeserializer fds = new FastDeserializer(buf);
        ParameterSet parameterSet = ParameterSet.fromFastDeserializer(fds);

        return new AdHocPlannedStatement(sql, core, parameterSet, null, null, null);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     * Mostly for debugging and testing.
     * Not zippy for the fast path.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AdHocPlannedStatement)) {
            return false;
        }
        AdHocPlannedStatement other = (AdHocPlannedStatement) obj;

        if (partitionParam != null) {
            if (!partitionParam.equals(other.partitionParam)) {
                return false;
            }
        }
        else {
            if (other.partitionParam != null) {
                return false;
            }
        }
        if (!Arrays.equals(sql, other.sql)) {
            return false;
        }
        if (!core.equals(other.core)) {
            return false;
        }
        if (!extractedParamValues.equals(other.extractedParamValues)) {
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

    public String[] parameterBindings() {
        if (boundParamStrings != null) {
            return boundParamStrings;
        }
        if (extractedParamStrings == null) {
            return null;
        }
        if (boundParamIndexes == null || boundParamIndexes.length == 0) {
            return null;
        }
        boundParamStrings = new String[extractedParamValues.size()];
        for (int paramIndex : boundParamIndexes) {
            boundParamStrings[paramIndex] = extractedParamStrings[paramIndex];
        }
        return boundParamStrings;
    }
}
