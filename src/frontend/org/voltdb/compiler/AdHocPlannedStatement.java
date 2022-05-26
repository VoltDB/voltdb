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

package org.voltdb.compiler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.voltdb.ParameterSet;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
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
    private final ParameterSet extractedParamValues;
    private final int[] boundParamIndexes;
    private String[] boundParamStrings;

    /***
     * Constructor
     *
     * @param sql                       bytes of sql string (utf-8)
     * @param core                      core immutable plan
     * @param extractedParamValues      params extracted from constant values
     * @param partitionParam            value used for partitioning
     */
    AdHocPlannedStatement(byte[] sql,
                          CorePlan core,
                          ParameterSet extractedParamValues,
                          int[] boundParamIndexes) {
        this.sql = sql;
        this.core = core;
        this.extractedParamValues = extractedParamValues;
        this.boundParamIndexes = boundParamIndexes;

        validate();
    }

    AdHocPlannedStatement(CompiledPlan plan, CorePlan coreIn) {
        this(plan.sql.getBytes(Constants.UTF8ENCODING), coreIn,
             plan.extractedParamValues(), plan.boundParamIndexes());
    }

    AdHocPlannedStatement(AdHocPlannedStatement original, CorePlan coreIn) {
        this(original.sql, coreIn, original.extractedParamValues, null);
    }

    private void validate() {
        assert(core != null);
        assert(extractedParamValues != null);
        // any extracted params => extracted param size == param type array size
        assert(extractedParamValues.size() == 0 || extractedParamValues.size() == core.parameterTypes.length);
        core.validate();
    }

    @Override
    public String toString() {
        return core.toString();
    }

    public int getSerializedSize() {
        // plan
        int size = core.getSerializedSize();

        // sql bytes
        size += 4;
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
        buf.putInt(sql.length);
        buf.put(sql);

        // params
        extractedParamValues.flattenToBuffer(buf);
    }

    public static AdHocPlannedStatement fromBuffer(ByteBuffer buf) throws IOException {
        // plan
        CorePlan core = CorePlan.fromBuffer(buf);

        // sql bytes
        int sqlLength = buf.getInt();
        //AS PER ENG-10059, there is a 1MB limit for in List Expression
        if ((sqlLength < 0) || (sqlLength >= 1024*1024)) {
            throw new RuntimeException("AdHoc SQL text exceeds the length limitation 1 MB");
        }

        byte[] sql = new byte[sqlLength];
        buf.get(sql);

        // params
        ParameterSet parameterSet = ParameterSet.fromByteBuffer(buf);

        return new AdHocPlannedStatement(sql, core, parameterSet, null);
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

    public String[] parameterBindings(String[] extractedParamStrings) {
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

    void setBoundConstants(String[] boundConstants) {
        boundParamStrings = boundConstants;
    }

    public Object[] extractedParamArray() {
        return extractedParamValues.toArray();
    }

    public boolean hasExtractedParams() {
        return extractedParamValues.size() > 0;
    }

    public int getPartitioningParameterIndex() { return core.getPartitioningParamIndex(); }

    public Object getPartitioningParameterValue() {
        int paramIndex = core.getPartitioningParamIndex();
        if (paramIndex != -1 && extractedParamValues != null && extractedParamValues.size() > paramIndex ) {
            return extractedParamValues.toArray()[paramIndex];
        } else {
            return core.getPartitioningParamValue();
        }
    }

    public VoltType getPartitioningParameterType() {
        return core.getPartitioningParamType();
    }
}
