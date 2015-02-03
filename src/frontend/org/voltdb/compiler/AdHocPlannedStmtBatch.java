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

package org.voltdb.compiler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterConverter;
import org.voltdb.ParameterSet;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Database;
import org.voltdb.common.Constants;
import org.voltdb.compiler.AsyncCompilerWork.AsyncCompilerWorkCompletionHandler;
import org.voltdb.planner.CorePlan;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.plannodes.SendPlanNode;

/**
 * Holds a batch of planned SQL statements.
 *
 * Both AdHocPlannedStmtBatch and AdHocPlannedStatement are derived from
 * AsyncCompilerResult. So there's some data redundancy, e.g. clientData.
 */
public class AdHocPlannedStmtBatch extends AsyncCompilerResult implements Cloneable {
    private static final long serialVersionUID = -8627490621430290801L;

    // One or the other of these may be assigned if the planner infers single partition work.
    // Not persisted across serializations.
    public final int partitionParamIndex;
    public final VoltType partitionParamType;
    public final Object partitionParamValue;

    // The planned statements.
    public final List<AdHocPlannedStatement> plannedStatements;

    // Assume the batch is read-only until we see the first non-select statement.
    public final boolean readOnly;

    // The original work request to the planner
    public final AdHocPlannerWork work;

    /**
     * Statement batch constructor.
     *
     * IMPORTANT: sqlBatchText is not maintained or updated by this class when
     * statements are added. The caller is responsible for splitting the batch
     * text and assuring that the individual SQL statements correspond to the
     * original.
     *
     * @param work                  the original work request
     * @param stmts                 the planned statements
     * @param partitionParamIndex   the (possibly inferred) partition parameter index
     * @param partitionParamValue   the (possibly inferred) partition parameter value
     * @param errors                sad news from the planner or null
     */
    public AdHocPlannedStmtBatch(
            AdHocPlannerWork work,
            List<AdHocPlannedStatement> stmts,
            int partitionParamIndex,
            VoltType partitionParamType,
            Object partitionParamValue,
            String errors) {
        this.work = work;

        this.clientHandle = work.clientHandle;
        this.connectionId = work.connectionId;
        this.hostname =
            (work.clientData == null) ? "" : ((Connection)work.clientData).getHostnameAndIPAndPort();
        this.adminConnection = work.adminConnection;
        this.clientData = work.clientData;

        this.plannedStatements = stmts;
        boolean allReadOnly = true;
        for (AdHocPlannedStatement plannedStmt : stmts) {
            // The first non-select statement makes it not read-only.
            if (!plannedStmt.core.readOnly) {
                allReadOnly = false;
                break;
            }
        }
        this.readOnly = allReadOnly;
        this.partitionParamIndex = partitionParamIndex;
        this.partitionParamType = partitionParamType;
        this.partitionParamValue = partitionParamValue;
        this.errorMsg = errors;
    }

    public static AdHocPlannedStmtBatch mockStatementBatch(long replySiteId, String sql,
            Object[] extractedValues, VoltType[] paramTypes,
            Object[] userParams, int partitionParamIndex, byte[] catalogHash)
    {
        // Mock up a dummy completion handler to satisfy the dummy work request.
        AsyncCompilerWorkCompletionHandler dummyHandler = new AsyncCompilerWorkCompletionHandler() {

            @Override
            public void onCompletion(AsyncCompilerResult result) {
                System.out.println("Hmm. Never expected to call this dummy handler.");
            }
        };
        // Mock up a dummy work request.
        AdHocPlannerWork work = AdHocPlannerWork.makeStoredProcAdHocPlannerWork(replySiteId,
                                                                                sql,
                                                                                userParams,
                                                                                false, // mock inferred partitioning
                                                                                null, dummyHandler);
        // Mock up dummy results from the work request.
        CorePlan core = new CorePlan(new byte[0],
                partitionParamIndex == -1 ? new byte[20] : null,
                new byte[20],
                partitionParamIndex == -1 ? new byte[20] : null,
                false,
                true,
                paramTypes,
                catalogHash);
        AdHocPlannedStatement s = new AdHocPlannedStatement(sql.getBytes(Constants.UTF8ENCODING),
                core,
                extractedValues == null ? ParameterSet.emptyParameterSet() :
                                          ParameterSet.fromArrayNoCopy(extractedValues),
                null);
        List<AdHocPlannedStatement> stmts = new ArrayList<AdHocPlannedStatement>();
        stmts.add(s);
        VoltType partitionParamType = null;
        Object partitionParamValue = null;
        if (work.userPartitionKey != null) {
            partitionParamValue = work.userPartitionKey[0];
        }
        else if (partitionParamIndex > -1) {
            partitionParamValue = userParams[partitionParamIndex];
        }
        if (partitionParamValue != null) {
            partitionParamType = VoltType.typeFromObject(partitionParamValue);
        }
        // Finally, mock up the planned batch.
        AdHocPlannedStmtBatch plannedStmtBatch = new AdHocPlannedStmtBatch(work,
                                                                           stmts,
                                                                           partitionParamIndex,
                                                                           partitionParamType,
                                                                           partitionParamValue,
                                                                           null);
        return plannedStmtBatch;
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "\n  partition param: " + ((partitionParamValue != null) ? partitionParamValue.toString() : "null");
        retval += "\n  partition param index: " + partitionParamIndex;
        retval += "\n  sql: " + work.sqlBatchText;
        return retval;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve all the SQL statement text as a list of strings.
     *
     * @return list of SQL statement strings
     */
    public List<String> getSQLStatements() {
        List<String> sqlStatements = new ArrayList<String>(plannedStatements.size());
        for (AdHocPlannedStatement plannedStatement : plannedStatements) {
            sqlStatements.add(new String(plannedStatement.sql, Constants.UTF8ENCODING));
        }
        return sqlStatements;
    }

    /**
     * Detect if batch is compatible with single partition optimizations
     * @return true if nothing is replicated and nothing has a collector.
     */
    public boolean isSinglePartitionCompatible() {
        for (AdHocPlannedStatement plannedStmt : plannedStatements) {
            if (plannedStmt.core.collectorFragment != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the number of planned statements.
     *
     * @return planned statement count
     */
    public int getPlannedStatementCount() {
        return plannedStatements.size();
    }

    /**
     * Get a particular planned statement by index.
     * The index is not validated here.
     *
     * @param index
     * @return planned statement
     */
    public AdHocPlannedStatement getPlannedStatement(int index) {
        return plannedStatements.get(index);
    }

    /**
     * Read-only flag accessor
     *
     * @return true if read-only
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * For convenience, serialization is accomplished with this single method,
     * but deserialization is piecemeal via the static methods userParamsFromBuffer
     * and planArrayFromBuffer with no dummy "AdHocPlannedStmtBatch receiver" instance required.
     */
    public ByteBuffer flattenPlanArrayToBuffer() throws IOException {
        int size = 0; // sizeof batch

        ParameterSet userParamCache = null;
        if (work.userParamSet == null) {
            userParamCache = ParameterSet.emptyParameterSet();
        } else {
            Object[] typedUserParams = new Object[work.userParamSet.length];
            int ii = 0;
            for (AdHocPlannedStatement cs : plannedStatements) {
                for (VoltType paramType : cs.core.parameterTypes) {
                    if (ii >= typedUserParams.length) {
                        String errorMsg =
                            "Too few actual arguments were passed for the parameters in the sql statement(s): (" +
                            typedUserParams.length + " vs. " + ii + ")";
                        // Volt-TYPE-Exception is slightly cheating, here, should there be a more general VoltArgumentException?
                        throw new VoltTypeException(errorMsg);
                    }
                    typedUserParams[ii] =
                            ParameterConverter.tryToMakeCompatible(paramType.classFromType(),
                                                                   work.userParamSet[ii]);
                    // System.out.println("DEBUG typed parameter: " + work.userParamSet[ii] +
                    //         "using type: " + paramType + "as: " + typedUserParams[ii]);
                    ii++;
                }
            }
            // Each parameter referenced in each statements should be represented
            // exactly once in userParams.
            if (ii < typedUserParams.length) {
                // Volt-TYPE-Exception is slightly cheating, here, should there be a more general VoltArgumentException?
                String errorMsg =
                        "Too many actual arguments were passed for the parameters in the sql statement(s): (" +
                        typedUserParams.length + " vs. " + ii + ")";
                        throw new VoltTypeException(errorMsg);
            }
            userParamCache = ParameterSet.fromArrayNoCopy(typedUserParams);
        }
        size += userParamCache.getSerializedSize();

        size += 2; // sizeof batch
        for (AdHocPlannedStatement cs : plannedStatements) {
            size += cs.getSerializedSize();
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        userParamCache.flattenToBuffer(buf);
        buf.putShort((short) plannedStatements.size());
        for (AdHocPlannedStatement cs : plannedStatements) {
            cs.flattenToBuffer(buf);
        }
        return buf;
    }

    /**
     * Deserialize the first part of the statement batch -- the combined parameters.
     */
    public static Object[] userParamsFromBuffer(ByteBuffer buf) throws IOException {
        return ParameterSet.fromByteBuffer(buf).toArray();
    }

    /**
     * Deserialize the second part of the statement batch -- the statements.
     */
    public static AdHocPlannedStatement[] planArrayFromBuffer(ByteBuffer buf) throws IOException {
        short csCount = buf.getShort();
        AdHocPlannedStatement[] statements = new AdHocPlannedStatement[csCount];
        for (int i = 0; i < csCount; ++i) {
            AdHocPlannedStatement cs = AdHocPlannedStatement.fromBuffer(buf);
            statements[i] = cs;
        }
        return statements;
    }

    public ExplainMode getExplainMode() {
        return work.explainMode;
    }

    /*
     * Return the partitioning value (if any) for an SP statement batch.
     * It may have come from a number of sources:
     * - an explicit override -- such as for AdHocSpForTest or an ad hoc statement queued
     * from an SP stored proc (which currently dummies up a null value)
     * - a user-provided parameter to an ad hoc that the planner determined to be the
     * partitioning parameter.
     * - a planner-extracted parameter from an ad hoc that the planner determined to be the
     * partitioning parameter.
     * - a constant from an ad hoc that the planner determined to be the
     * partitioning parameter (after opting out of or failing parameterization).
     */
    public Object partitionParam() {
        if (work.userPartitionKey != null) {
            return work.userPartitionKey[0];
        }
        if (partitionParamIndex > -1 && work.userParamSet != null &&
                work.userParamSet.length > partitionParamIndex) {
            Object userParamValue = work.userParamSet[partitionParamIndex];
            if (partitionParamType == null) {
                return userParamValue;
            } else {
                return ParameterConverter.tryToMakeCompatible(partitionParamType.classFromType(), userParamValue);
            }
        }
        return partitionParamValue;
    }

    /**
     * Return the "EXPLAIN" string of the batched statement at the index
     * @param i the index
     * @param db the database context (for adding catalog details).
     */
    public String explainStatement(int i, Database db) {
        String str = "";
        AdHocPlannedStatement plannedStatement = plannedStatements.get(i);
        String aggplan = new String(plannedStatement.core.aggregatorFragment, Constants.UTF8ENCODING);
        PlanNodeTree pnt = new PlanNodeTree();
        try {
            JSONObject jobj = new JSONObject( aggplan );
            JSONArray jarray =  jobj.getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
            pnt.loadFromJSONArray(jarray, db);

            if( plannedStatement.core.collectorFragment != null ) {
                //multi-partition query plan
                String collplan = new String(plannedStatement.core.collectorFragment, Constants.UTF8ENCODING);
                PlanNodeTree collpnt = new PlanNodeTree();
                //reattach plan fragments
                jobj = new JSONObject( collplan );
                jarray =  jobj.getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
                collpnt.loadFromJSONArray(jarray, db);
                assert( collpnt.getRootPlanNode() instanceof SendPlanNode);
                pnt.getRootPlanNode().reattachFragment( (SendPlanNode) collpnt.getRootPlanNode() );
            }
            str = pnt.getRootPlanNode().toExplainPlanString();
        } catch (JSONException e) {
            System.out.println(e.getMessage());
        }
        return str;
    }

}
