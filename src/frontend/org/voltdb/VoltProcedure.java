/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltLoggerFactory;

/**
 * Wraps the stored procedure object created by the user
 * with metadata available at runtime. This is used to call
 * the procedure.
 *
 * VoltProcedure is extended by all running stored procedures.
 * Consider this when specifying access privileges.
 *
 */
public abstract class VoltProcedure {
    private static final Logger log = Logger.getLogger(VoltProcedure.class.getName(), VoltLoggerFactory.instance());

    // Used to get around the "abstract" for StmtProcedures.
    // Path of least resistance?
    static class StmtProcedure extends VoltProcedure {}

    final static Double DOUBLE_NULL = new Double(-1.7976931348623157E+308);
    public static final String ANON_STMT_NAME = "sql";

    protected HsqlBackend hsql;

    final private ProcedureProfiler profiler = new ProcedureProfiler();

    //For runtime statistics collection
    private ProcedureStatsCollector statsCollector;

    // package scoped members used by VoltSystemProcedure
    Cluster m_cluster;
    SiteProcedureConnection m_site;
    TransactionState m_currentTxnState;  // assigned in call()

    /**
     * Allow VoltProcedures access to their transaction id.
     * @return transaction id
     */
    public long getTransactionId() {
        return m_currentTxnState.txnId;
    }

    private boolean m_initialized;

    // private members reserved exclusively to VoltProcedure
    private Method procMethod;
    private Class<?>[] paramTypes;
    private boolean paramTypeIsPrimitive[];
    private boolean paramTypeIsArray[];
    private Class<?> paramTypeComponentType[];
    private int paramTypesLength;
    private Procedure catProc;
    private boolean isNative = true;
    private int numberOfPartitions;

    // cached fake SQLStmt array for single statement non-java procs
    SQLStmt[] m_cachedSingleStmt = { null };

    // Workload Trace Handles
    private Object m_workloadXactHandle = null;
    private Integer m_workloadBatchId = null;
    private Set<Object> m_workloadQueryHandles;

    // data copied from EE proc wrapper
    private final SQLStmt batchQueryStmts[] = new SQLStmt[1000];
    private int batchQueryStmtIndex = 0;
    private Object[] batchQueryArgs[];
    private int batchQueryArgsIndex = 0;
    private final long fragmentIds[] = new long[1000];
    private final int expectedDeps[] = new int[1000];
    private ParameterSet parameterSets[];

    // data from hsql wrapper
    private final ArrayList<VoltTable> queryResults = new ArrayList<VoltTable>();

    /**
     * End users should not instantiate VoltProcedure instances.
     * Constructor does nothing. All actual initialization is done in the
     * {@link VoltProcedure init} method.
     */
    public VoltProcedure() {}

    /**
     * End users should not call this method.
     * Used by the VoltDB runtime to initialize stored procedures for execution.
     */
    public void init(
            int numberOfPartitions,
            SiteProcedureConnection site,
            Procedure catProc,
            BackendTarget eeType,
            HsqlBackend hsql,
            Cluster cluster)
    {
        if (m_initialized) {
            throw new IllegalStateException("VoltProcedure has already been initialized");
        } else {
            m_initialized = true;
        }

        this.catProc = catProc;
        this.m_site = site;
        this.isNative = (eeType != BackendTarget.HSQLDB_BACKEND);
        this.hsql = hsql;
        this.m_cluster = cluster;
        this.numberOfPartitions = numberOfPartitions;
        statsCollector = new ProcedureStatsCollector();
        VoltDB.instance().getStatsAgent().registerStatsSource(
                SysProcSelector.PROCEDURE,
                Integer.parseInt(site.getCorrespondingCatalogSite().getTypeName()),
                statsCollector);

        // this is a stupid hack to make the EE happy
        for (int i = 0; i < expectedDeps.length; i++)
            expectedDeps[i] = 1;

        if (catProc.getHasjava()) {
            int tempParamTypesLength = 0;
            Method tempProcMethod = null;
            Method[] methods = getClass().getMethods();
            Class<?> tempParamTypes[] = null;
            boolean tempParamTypeIsPrimitive[] = null;
            boolean tempParamTypeIsArray[] = null;
            Class<?> tempParamTypeComponentType[] = null;
            for (final Method m : methods) {
                String name = m.getName();
                if (name.equals("run")) {
                    //inspect(m);
                    tempProcMethod = m;
                    tempParamTypes = tempProcMethod.getParameterTypes();
                    tempParamTypesLength = tempParamTypes.length;
                    tempParamTypeIsPrimitive = new boolean[tempParamTypesLength];
                    tempParamTypeIsArray = new boolean[tempParamTypesLength];
                    tempParamTypeComponentType = new Class<?>[tempParamTypesLength];
                    for (int ii = 0; ii < tempParamTypesLength; ii++) {
                        tempParamTypeIsPrimitive[ii] = tempParamTypes[ii].isPrimitive();
                        tempParamTypeIsArray[ii] = tempParamTypes[ii].isArray();
                        tempParamTypeComponentType[ii] = tempParamTypes[ii].getComponentType();
                    }
                }
            }
            paramTypesLength = tempParamTypesLength;
            procMethod = tempProcMethod;
            paramTypes = tempParamTypes;
            paramTypeIsPrimitive = tempParamTypeIsPrimitive;
            paramTypeIsArray = tempParamTypeIsArray;
            paramTypeComponentType = tempParamTypeComponentType;

            if (procMethod == null) {
                log.debug("No good method found in: " + getClass().getName());
            }

            Field[] fields = getClass().getFields();
            for (final Field f : fields) {
                if (f.getType() == SQLStmt.class) {
                    String name = f.getName();
                    Statement s = catProc.getStatements().get(name);
                    if (s != null) {
                        try {
                            /*
                             * Cache all the information we need about the statements in this stored
                             * procedure locally instead of pulling them from the catalog on
                             * a regular basis.
                             */
                            SQLStmt stmt = (SQLStmt) f.get(this);

                            stmt.catStmt = s;

                            initSQLStmt(stmt);

                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                        //LOG.fine("Found statement " + name);
                    }
                }
            }
        }
        // has no java
        else {
            Statement catStmt = catProc.getStatements().get(ANON_STMT_NAME);
            SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
            stmt.catStmt = catStmt;
            initSQLStmt(stmt);
            m_cachedSingleStmt[0] = stmt;

            procMethod = null;

            paramTypesLength = catProc.getParameters().size();

            paramTypes = new Class<?>[paramTypesLength];
            paramTypeIsPrimitive = new boolean[paramTypesLength];
            paramTypeIsArray = new boolean[paramTypesLength];
            paramTypeComponentType = new Class<?>[paramTypesLength];
            for (ProcParameter param : catProc.getParameters()) {
                VoltType type = VoltType.get((byte) param.getType());
                if (type == VoltType.INTEGER) type = VoltType.BIGINT;
                if (type == VoltType.SMALLINT) type = VoltType.BIGINT;
                if (type == VoltType.TINYINT) type = VoltType.BIGINT;
                paramTypes[param.getIndex()] = type.classFromType();
                paramTypeIsPrimitive[param.getIndex()] = true;
                paramTypeIsArray[param.getIndex()] = param.getIsarray();
                assert(paramTypeIsArray[param.getIndex()] == false);
                paramTypeComponentType[param.getIndex()] = null;
            }
        }
    }

    final void initSQLStmt(SQLStmt stmt) {
        stmt.numFragGUIDs = stmt.catStmt.getFragments().size();
        PlanFragment fragments[] = new PlanFragment[stmt.numFragGUIDs];
        stmt.fragGUIDs = new long[stmt.numFragGUIDs];
        int i = 0;
        for (PlanFragment frag : stmt.catStmt.getFragments()) {
            fragments[i] = frag;
            stmt.fragGUIDs[i] = CatalogUtil.getUniqueIdForFragment(frag);
            i++;
        }

        stmt.numStatementParamJavaTypes = stmt.catStmt.getParameters().size();
        //StmtParameter parameters[] = new StmtParameter[stmt.numStatementParamJavaTypes];
        stmt.statementParamJavaTypes = new byte[stmt.numStatementParamJavaTypes];
        for (StmtParameter param : stmt.catStmt.getParameters()) {
            //parameters[i] = param;
            stmt.statementParamJavaTypes[param.getIndex()] = (byte)param.getJavatype();
            i++;
        }
    }

    /* Package private but not final, to enable mock volt procedure objects */
    ClientResponseImpl call(TransactionState txnState, Object... paramList) {
        m_currentTxnState = txnState;

        if (ProcedureProfiler.profilingLevel != ProcedureProfiler.Level.DISABLED) {
            profiler.startCounter(catProc);
        }
        statsCollector.beginProcedure();

        // in case sql was queued but executed
        batchQueryStmtIndex = 0;
        batchQueryArgsIndex = 0;

        VoltTable[] results = new VoltTable[0];
        byte status = ClientResponseImpl.SUCCESS;

        if (paramList.length != paramTypesLength) {
            statsCollector.endProcedure( false, true);
            String msg = "PROCEDURE " + catProc.getTypeName() + " EXPECTS " + String.valueOf(paramTypesLength) +
                " PARAMS, BUT RECEIVED " + String.valueOf(paramList.length);
            status = ClientResponseImpl.GRACEFUL_FAILURE;
            return getErrorResponse(status, msg, null);
        }

        for (int i = 0; i < paramTypesLength; i++) {
            try {
                paramList[i] = tryToMakeCompatible( i, paramList[i]);
            } catch (Exception e) {
                statsCollector.endProcedure( false, true);
                String msg = "PROCEDURE " + catProc.getTypeName() + " TYPE ERROR FOR PARAMETER " + i +
                        ": " + e.getMessage();
                status = ClientResponseImpl.GRACEFUL_FAILURE;
                return getErrorResponse(status, msg, null);
            }
        }

        // Workload Trace
        // Create a new transaction record in the trace manager. This will give us back
        // a handle that we need to pass to the trace manager when we want to register a new query
        if ((ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) &&
                (ProcedureProfiler.workloadTrace != null)) {
            m_workloadQueryHandles = new HashSet<Object>();
            m_workloadXactHandle = ProcedureProfiler.workloadTrace.startTransaction(this, catProc, paramList);
        }

        ClientResponseImpl retval = null;
        boolean error = false;
        boolean abort = false;
        // run a regular java class
        if (catProc.getHasjava()) {
            try {
                if (log.isEnabledFor(Level.TRACE)) {
                    log.trace("invoking... procMethod=" + procMethod.getName() + ", class=" + getClass().getName());
                }
                try {
                    batchQueryArgs = new Object[1000][];
                    parameterSets = new ParameterSet[1000];
                    Object rawResult = procMethod.invoke(this, paramList);
                    results = getResultsFromRawResults(rawResult);
                    if (results == null)
                        results = new VoltTable[0];
                } catch (IllegalAccessException e) {
                    // If reflection fails, invoke the same error handling that other exceptions do
                    throw new InvocationTargetException(e);
                } finally {
                    batchQueryArgs = null;
                    parameterSets = null;
                }
                log.trace("invoked");
            }
            catch (InvocationTargetException itex) {
                //itex.printStackTrace();
                Throwable ex = itex.getCause();
                if (ex instanceof VoltAbortException &&
                        !(ex instanceof EEException)) {
                    abort = true;
                } else {
                    error = true;
                }
                if (ex instanceof Error) {
                    statsCollector.endProcedure( false, true);
                    throw (Error)ex;
                }

                retval = getErrorResponse(ex);
            }
        }
        // single statement only work
        // (this could be made faster, but with less code re-use)
        else {
            assert(catProc.getStatements().size() == 1);
            try {
                batchQueryArgs = new Object[1000][];
                parameterSets = new ParameterSet[1000];
                if (!isNative) {
                    // HSQL handling
                    VoltTable table = hsql.runSQLWithSubstitutions(m_cachedSingleStmt[0], paramList);
                    results = new VoltTable[] { table };
                }
                else {
                    results = executeQueriesInABatch(1, m_cachedSingleStmt, new Object[][] { paramList } , true);
                }
            }
            catch (SerializableException ex) {
                retval = getErrorResponse(ex);
            } finally {
                batchQueryArgs = null;
                parameterSets = null;
            }
        }

        if (ProcedureProfiler.profilingLevel != ProcedureProfiler.Level.DISABLED)
            profiler.stopCounter();
        statsCollector.endProcedure( abort, error);

        // Workload Trace - Stop the transaction trace record.
        if ((ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) &&
                (ProcedureProfiler.workloadTrace != null && m_workloadXactHandle != null)) {
            ProcedureProfiler.workloadTrace.stopTransaction(m_workloadXactHandle);
        }

        if (retval == null)
            retval = new ClientResponseImpl( status, results, null);

        return retval;
    }

    /**
     * Given the results of a procedure, convert it into a sensible array of VoltTables.
     */
    final private VoltTable[] getResultsFromRawResults(Object result) {
        if (result == null)
            return new VoltTable[0];
        if (result instanceof VoltTable[])
            return (VoltTable[]) result;
        if (result instanceof VoltTable)
            return new VoltTable[] { (VoltTable) result };
        if (result instanceof Long) {
            VoltTable t = new VoltTable(new VoltTable.ColumnInfo("", VoltType.BIGINT));
            t.addRow(result);
            return new VoltTable[] { t };
        }
        throw new RuntimeException("Procedure didn't return acceptable type.");
    }

    /** @throws Exception with a message describing why the types are incompatible. */
    final private Object tryToMakeCompatible(int paramTypeIndex, Object param) throws Exception {
        if (param == null || param == VoltType.NULL_STRING ||
            param == VoltType.NULL_DECIMAL)
        {
            // Passing a null where we expect a primitive is a Java compile time error.
            if (paramTypeIsPrimitive[paramTypeIndex]) {
                throw new Exception("Primitive type " + paramTypes[paramTypeIndex] + " cannot be null");
            }

            // Pass null reference to the procedure run() method. These null values will be
            // converted to a serialize-able NULL representation for the EE in getCleanParams()
            // when the parameters are serialized for the plan fragment.
            return null;
        }

        if (param instanceof ExecutionSite.SystemProcedureExecutionContext) {
            return param;
        }

        Class<?> pclass = param.getClass();
        boolean slotIsArray = paramTypeIsArray[paramTypeIndex];
        if (slotIsArray != pclass.isArray())
            throw new Exception("Array / Scalar parameter mismatch");

        if (slotIsArray) {
            Class<?> pSubCls = pclass.getComponentType();
            Class<?> sSubCls = paramTypeComponentType[paramTypeIndex];
            if (pSubCls == sSubCls) {
                return param;
            } else {
                /*
                 * Arrays can be quite large so it doesn't make sense to silently do the conversion
                 * and incur the performance hit. The client should serialize the correct invocation
                 * parameters
                 */
                new Exception(
                        "tryScalarMakeCompatible: Unable to match parameter array:"
                        + sSubCls.getName() + " to provided " + pSubCls.getName());
            }
        }

        /*
         * inline tryScalarMakeCompatible so we can save on reflection
         */
        final Class<?> slot = paramTypes[paramTypeIndex];
        if ((slot == long.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == int.class) && (pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == short.class) && (pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == byte.class) && (pclass == Byte.class)) return param;
        if ((slot == double.class) && (pclass == Double.class)) return param;
        if ((slot == String.class) && (pclass == String.class)) return param;
        if (slot == TimestampType.class) {
            if (pclass == Long.class) return new TimestampType((Long)param);
            if (pclass == TimestampType.class) return param;
        }
        if (slot == BigDecimal.class) {
            if (pclass == Long.class) {
                BigInteger bi = new BigInteger(param.toString());
                BigDecimal bd = new BigDecimal(bi);
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == BigDecimal.class) {
                BigDecimal bd = (BigDecimal) param;
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
        }
        if (slot == VoltTable.class && pclass == VoltTable.class) {
            return param;
        }
        throw new Exception(
                "tryToMakeCompatible: Unable to match parameters:"
                + slot.getName() + " to provided " + pclass.getName());
    }

    /**
     * Thrown from a stored procedure to indicate to VoltDB
     * that the procedure should be aborted and rolled back.
     */
    public static class VoltAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;
        private String message = "No message specified.";

        /**
         * Constructs a new <code>AbortException</code>
         */
        public VoltAbortException() {}

        /**
         * Constructs a new <code>AbortException</code> with the specified detail message.
         */
        public VoltAbortException(String msg) {
            message = msg;
        }
        /**
         * Returns the detail message string of this <tt>AbortException</tt>
         *
         * @return The detail message.
         */
        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * Currently unsupported in VoltDB.
     * Batch load method for populating a table with a large number of records.
     *
     * Faster then calling {@link #voltQueueSQL(SQLStmt, Object...)} and {@link #voltExecuteSQL()} to
     * insert one row at a time.
     * @param clusterName Name of the cluster containing the database, containing the table
     *                    that the records will be loaded in.
     * @param databaseName Name of the database containing the table to be loaded.
     * @param tableName Name of the table records should be loaded in.
     * @param data {@link org.voltdb.VoltTable VoltTable} containing the records to be loaded.
     *             {@link org.voltdb.VoltTable.ColumnInfo VoltTable.ColumnInfo} schema must match the schema of the table being
     *             loaded.
     * @throws VoltAbortException
     */
    public void voltLoadTable(String clusterName, String databaseName,
                              String tableName, VoltTable data, int allowELT)
    throws VoltAbortException
    {
        if (data == null || data.getRowCount() == 0) {
            return;
        }
        try {
            m_site.loadTable(m_currentTxnState.txnId,
                             clusterName, databaseName,
                             tableName, data, allowELT);
        }
        catch (EEException e) {
            throw new VoltAbortException("Failed to load table: " + tableName);
        }
    }



    /**
     * Queue the SQL {@link org.voltdb.SQLStmt statement} for execution with the specified argument list.
     *
     * @param stmt {@link org.voltdb.SQLStmt Statement} to queue for execution.
     * @param args List of arguments to be bound as parameters for the {@link org.voltdb.SQLStmt statement}
     * @see <a href="#allowable_params">List of allowable parameter types</a>
     */
    public void voltQueueSQL(final SQLStmt stmt, Object... args) {
        if (!isNative) {
            //HSQLProcedureWrapper does nothing smart. it just implements this interface with runStatement()
            VoltTable table = hsql.runSQLWithSubstitutions(stmt, args);
            queryResults.add(table);
            return;
        }

        if (batchQueryStmtIndex == batchQueryStmts.length) {
            throw new RuntimeException("Procedure attempted to queue more than " + batchQueryStmts.length +
                    " statements in a batch.");
        } else {
            batchQueryStmts[batchQueryStmtIndex++] = stmt;
            batchQueryArgs[batchQueryArgsIndex++] = args;
        }
    }

    /**
     * Execute the currently queued SQL {@link org.voltdb.SQLStmt statements} and return
     * the result tables.
     *
     * @return Result {@link org.voltdb.VoltTable tables} generated by executing the queued
     * query {@link org.voltdb.SQLStmt statements}
     */
    public VoltTable[] voltExecuteSQL() {
        return voltExecuteSQL(false);
    }

    /**
     * Execute the currently queued SQL {@link org.voltdb.SQLStmt statements} and return
     * the result tables. Boolean option allows caller to indicate if this is the final
     * batch for a procedure. If it's final, then additional optimizatons can be enabled.
     *
     * @param isFinalSQL Is this the final batch for a procedure?
     * @return Result {@link org.voltdb.VoltTable tables} generated by executing the queued
     * query {@link org.voltdb.SQLStmt statements}
     */
    public VoltTable[] voltExecuteSQL(boolean isFinalSQL) {
        if (!isNative) {
            VoltTable[] batch_results = queryResults.toArray(new VoltTable[queryResults.size()]);
            queryResults.clear();
            return batch_results;
        }

        assert (batchQueryStmtIndex == batchQueryArgsIndex);

        // if profiling is turned on, record the sql statements being run
        if (ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) {
            // Workload Trace - Start Query
            if (ProcedureProfiler.workloadTrace != null && m_workloadXactHandle != null) {
                m_workloadBatchId = ProcedureProfiler.workloadTrace.getNextBatchId(m_workloadXactHandle);
                for (int i = 0; i < batchQueryStmtIndex; i++) {
                    Object queryHandle = ProcedureProfiler.workloadTrace.startQuery(
                            m_workloadXactHandle, batchQueryStmts[i].catStmt, batchQueryArgs[i], m_workloadBatchId);
                    m_workloadQueryHandles.add(queryHandle);
                }
            }
        }

        VoltTable[] retval = null;

        if (ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) {
            retval = executeQueriesInIndividualBatches(batchQueryStmtIndex, batchQueryStmts, batchQueryArgs, isFinalSQL);
        }
        else if (catProc.getSinglepartition() == false) {
            // check for duplicate sql statements
            // if so, do them individually for now
            boolean duplicate = false;

            for (int i = 0; i < batchQueryStmtIndex; i++) {
                for (int j = i + 1; i < batchQueryStmtIndex; i++) {
                    if (batchQueryStmts[i] == batchQueryStmts[j])
                        duplicate = true;
                }
            }
            if (duplicate) {
                retval = executeQueriesInIndividualBatches(
                    batchQueryStmtIndex, batchQueryStmts, batchQueryArgs, isFinalSQL);
            }
            else {
                retval = executeQueriesInABatch(
                    batchQueryStmtIndex, batchQueryStmts, batchQueryArgs, isFinalSQL);
            }
        }
        else {
            retval = executeQueriesInABatch(
                batchQueryStmtIndex, batchQueryStmts, batchQueryArgs, isFinalSQL);
        }

        // Workload Trace - Stop Query
        if (ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) {
            if (ProcedureProfiler.workloadTrace != null) {
                for (Object handle : m_workloadQueryHandles) {
                    if (handle != null) ProcedureProfiler.workloadTrace.stopQuery(handle);
                }
                // Make sure that we clear out our query handles so that the next
                // time they queue a query they will get a new batch id
                m_workloadQueryHandles.clear();
            }
        }

        batchQueryStmtIndex = 0;
        batchQueryArgsIndex = 0;
        return retval;
    }

    private VoltTable[] executeQueriesInIndividualBatches(int stmtCount, SQLStmt[] batchStmts, Object[][] batchArgs, boolean finalTask) {
        assert(stmtCount > 0);
        assert(batchStmts != null);
        assert(batchArgs != null);

        VoltTable[] retval = new VoltTable[stmtCount];

        for (int i = 0; i < stmtCount; i++) {
            assert(batchStmts[i] != null);
            assert(batchArgs[i] != null);

            SQLStmt[] subBatchStmts = new SQLStmt[1];
            Object[][] subBatchArgs = new Object[1][];

            subBatchStmts[0] = batchStmts[i];
            subBatchArgs[0] = batchArgs[i];

            boolean isThisLoopFinalTask = finalTask && (i == (stmtCount - 1));
            VoltTable[] results = executeQueriesInABatch(1, subBatchStmts, subBatchArgs, isThisLoopFinalTask);
            assert(results != null);
            assert(results.length == 1);
            retval[i] = results[0];
        }

        return retval;
    }

    private VoltTable[] executeQueriesInABatch(int stmtCount, SQLStmt[] batchStmts, Object[][] batchArgs, boolean finalTask) {
        assert(batchStmts != null);
        assert(batchArgs != null);
        assert(batchStmts.length > 0);
        assert(batchArgs.length > 0);

        if (stmtCount == 0)
            return new VoltTable[] {};

        if (ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) {
            assert(batchStmts.length == 1);
            assert(batchStmts[0].numFragGUIDs == 1);
            ProcedureProfiler.startStatementCounter(batchStmts[0].fragGUIDs[0]);
        }
        else ProcedureProfiler.startStatementCounter(-1);

        final int batchSize = stmtCount;
        int fragmentIdIndex = 0;
        int parameterSetIndex = 0;
        boolean slowPath = false;
        for (int i = 0; i < batchSize; ++i) {
            final SQLStmt stmt = batchStmts[i];

            // check if the statement has been oked by the compiler/loader
            if (stmt.catStmt == null) {
                String msg = "SQLStmt objects cannot be instantiated after";
                msg += " VoltDB initialization. User may have instantiated a SQLStmt";
                msg += " inside a stored procedure's run method.";
                throw new RuntimeException(msg);
            }

            // if any stmt is not single sited in this batch, the
            // full batch must take the slow path through the dtxn
            slowPath = slowPath || !(stmt.catStmt.getSinglepartition());
            final Object[] args = batchArgs[i];
            // check all the params
            final ParameterSet params = getCleanParams(stmt, args);

            final int numFrags = stmt.numFragGUIDs;
            final long fragGUIDs[] = stmt.fragGUIDs;
            for (int ii = 0; ii < numFrags; ii++) {
                fragmentIds[fragmentIdIndex++] = fragGUIDs[ii];
                parameterSets[parameterSetIndex++] = params;
            }
        }

        if (slowPath) {
            return slowPath(batchSize, batchStmts, batchArgs, finalTask);
        }

        VoltTable[] results = null;
        try {
            results = m_site.executeQueryPlanFragmentsAndGetResults(
                fragmentIds,
                fragmentIdIndex,
                parameterSets,
                parameterSetIndex,
                m_currentTxnState.txnId,
                catProc.getReadonly());
        }
        finally {
            ProcedureProfiler.stopStatementCounter();
        }
        return results;
    }

    private ParameterSet getCleanParams(SQLStmt stmt, Object[] args) {
        final int numParamTypes = stmt.numStatementParamJavaTypes;
        final byte stmtParamTypes[] = stmt.statementParamJavaTypes;
        if (args.length != numParamTypes) {
            throw new ExpectedProcedureException(
                    "Number of arguments provided was " + args.length  +
                    " where " + numParamTypes + " was expected for statement " + stmt.getText());
        }
        for (int ii = 0; ii < numParamTypes; ii++) {
            // this only handles null values
            if (args[ii] != null) continue;
            VoltType type = VoltType.get(stmtParamTypes[ii]);
            if (type == VoltType.TINYINT)
                args[ii] = Byte.MIN_VALUE;
            else if (type == VoltType.SMALLINT)
                args[ii] = Short.MIN_VALUE;
            else if (type == VoltType.INTEGER)
                args[ii] = Integer.MIN_VALUE;
            else if (type == VoltType.BIGINT)
                args[ii] = Long.MIN_VALUE;
            else if (type == VoltType.FLOAT)
                args[ii] = DOUBLE_NULL;
            else if (type == VoltType.TIMESTAMP)
                args[ii] = new TimestampType(Long.MIN_VALUE);
            else if (type == VoltType.STRING)
                args[ii] = VoltType.NULL_STRING;
            else if (type == VoltType.DECIMAL)
                args[ii] = VoltType.NULL_DECIMAL;
            else
                throw new ExpectedProcedureException("Unknown type " + type +
                 " can not be converted to NULL representation for arg " + ii + " for SQL stmt " + stmt.getText());
        }

        final ParameterSet params = new ParameterSet(true);
        params.setParameters(args);
        return params;
    }

    /**
     * Derivation of StatsSource to expose timing information of procedure invocations.
     *
     */
    private final class ProcedureStatsCollector extends SiteStatsSource {

        /**
         * Record procedure execution time ever N invocations
         */
        final int timeCollectionInterval = 20;

        /**
         * Number of times this procedure has been invoked.
         */
        private long m_invocations = 0;
        private long m_lastInvocations = 0;

        /**
         * Number of timed invocations
         */
        private long m_timedInvocations = 0;
        private long m_lastTimedInvocations = 0;

        /**
         * Total amount of timed execution time
         */
        private long m_totalTimedExecutionTime = 0;
        private long m_lastTotalTimedExecutionTime = 0;

        /**
         * Shortest amount of time this procedure has executed in
         */
        private long m_minExecutionTime = Long.MAX_VALUE;
        private long m_lastMinExecutionTime = Long.MAX_VALUE;

        /**
         * Longest amount of time this procedure has executed in
         */
        private long m_maxExecutionTime = Long.MIN_VALUE;
        private long m_lastMaxExecutionTime = Long.MIN_VALUE;

        /**
         * Time the procedure was last started
         */
        private long m_currentStartTime = -1;

        /**
         * Count of the number of aborts (user initiated or DB initiated)
         */
        private long m_abortCount = 0;
        private long m_lastAbortCount = 0;

        /**
         * Count of the number of errors that occured during procedure execution
         */
        private long m_failureCount = 0;
        private long m_lastFailureCount = 0;

        /**
         * Whether to return results in intervals since polling or since the beginning
         */
        private boolean m_interval = false;
        /**
         * Constructor requires no args because it has access to the enclosing classes members.
         */
        public ProcedureStatsCollector() {
            super(m_site.getCorrespondingSiteId() + " " + catProc.getClassname(),
                  m_site.getCorrespondingSiteId());
        }

        /**
         * Called when a procedure begins executing. Caches the time the procedure starts.
         */
        public final void beginProcedure() {
            if (m_invocations % timeCollectionInterval == 0) {
                m_currentStartTime = System.nanoTime();
            }
        }

        /**
         * Called after a procedure is finished executing. Compares the start and end time and calculates
         * the statistics.
         */
        public final void endProcedure(boolean aborted, boolean failed) {
            if (m_currentStartTime > 0) {
                final long endTime = System.nanoTime();
                final int delta = (int)(endTime - m_currentStartTime);
                m_totalTimedExecutionTime += delta;
                m_timedInvocations++;
                m_minExecutionTime = Math.min( delta, m_minExecutionTime);
                m_maxExecutionTime = Math.max( delta, m_maxExecutionTime);
                m_lastMinExecutionTime = Math.min( delta, m_lastMinExecutionTime);
                m_lastMaxExecutionTime = Math.max( delta, m_lastMaxExecutionTime);
                m_currentStartTime = -1;
            }
            if (aborted) {
                m_abortCount++;
            }
            if (failed) {
                m_failureCount++;
            }
            m_invocations++;
        }

        /**
         * Update the rowValues array with the latest statistical information.
         * This method is overrides the super class version
         * which must also be called so that it can update its columns.
         * @param values Values of each column of the row of stats. Used as output.
         */
        @Override
        protected void updateStatsRow(Object rowKey, Object rowValues[]) {
            super.updateStatsRow(rowKey, rowValues);
            rowValues[columnNameToIndex.get("PARTITION_ID")] =
                m_site.getCorrespondingPartitionId();
            rowValues[columnNameToIndex.get("PROCEDURE")] = catProc.getClassname();
            long invocations = m_invocations;
            long totalTimedExecutionTime = m_totalTimedExecutionTime;
            long timedInvocations = m_timedInvocations;
            long minExecutionTime = m_minExecutionTime;
            long maxExecutionTime = m_maxExecutionTime;
            long abortCount = m_abortCount;
            long failureCount = m_failureCount;

            if (m_interval) {
                invocations = m_invocations - m_lastInvocations;
                m_lastInvocations = m_invocations;

                totalTimedExecutionTime = m_totalTimedExecutionTime - m_lastTotalTimedExecutionTime;
                m_lastTotalTimedExecutionTime = m_totalTimedExecutionTime;

                timedInvocations = m_timedInvocations - m_lastTimedInvocations;
                m_lastTimedInvocations = m_timedInvocations;

                abortCount = m_abortCount - m_lastAbortCount;
                m_lastAbortCount = m_abortCount;

                failureCount = m_failureCount - m_lastFailureCount;
                m_lastFailureCount = m_failureCount;

                minExecutionTime = m_lastMinExecutionTime;
                maxExecutionTime = m_lastMaxExecutionTime;
                m_lastMinExecutionTime = Long.MAX_VALUE;
                m_lastMaxExecutionTime = Long.MIN_VALUE;
            }

            rowValues[columnNameToIndex.get("INVOCATIONS")] = invocations;
            rowValues[columnNameToIndex.get("TIMED_INVOCATIONS")] = timedInvocations;
            rowValues[columnNameToIndex.get("MIN_EXECUTION_TIME")] = minExecutionTime;
            rowValues[columnNameToIndex.get("MAX_EXECUTION_TIME")] = maxExecutionTime;
            if (timedInvocations != 0) {
                rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] =
                     (totalTimedExecutionTime / timedInvocations);
            } else {
                rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] = 0L;
            }
            rowValues[columnNameToIndex.get("ABORTS")] = abortCount;
            rowValues[columnNameToIndex.get("FAILURES")] = failureCount;
        }

        /**
         * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
         * @param columns List of columns that are in a stats row.
         */
        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns);
            columns.add(new VoltTable.ColumnInfo("PARTITION_ID", VoltType.INTEGER));
            columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
            columns.add(new VoltTable.ColumnInfo("INVOCATIONS", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("ABORTS", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("FAILURES", VoltType.BIGINT));
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            m_interval = interval;
            return new Iterator<Object>() {
                boolean givenNext = false;
                @Override
                public boolean hasNext() {
                    if (!m_interval) {
                        if (m_invocations == 0) {
                            return false;
                        }
                    } else if (m_invocations - m_lastInvocations == 0){
                        return false;
                    }
                    return !givenNext;
                }

                @Override
                public Object next() {
                    if (!givenNext) {
                        givenNext = true;
                        return new Object();
                    }
                    return null;
                }

                @Override
                public void remove() {}

            };
        }

        @Override
        public String toString() {
            return catProc.getTypeName();
        }
    }

    /**
     * Currently unsupported in VoltDB.
     * Write a message to the VoltDB server's log.
     * @param msg Message to log
     * @return <tt>true</tt>
     */
    public boolean voltLog(String msg) {
        return true;
    }

    private VoltTable[] slowPath(int batchSize, SQLStmt[] batchStmts, Object[][] batchArgs, boolean finalTask) {
        VoltTable[] results = new VoltTable[batchSize];
        FastSerializer fs = new FastSerializer();

        // the set of dependency ids for the expected results of the batch
        // one per sql statment
        int[] depsToResume = new int[batchSize];

        // these dependencies need to be received before the local stuff can run
        int[] depsForLocalTask = new int[batchSize];

        // the list of frag ids to run locally
        long[] localFragIds = new long[batchSize];

        // the list of frag ids to run remotely
        ArrayList<Long> distributedFragIds = new ArrayList<Long>();
        ArrayList<Integer> distributedOutputDepIds = new ArrayList<Integer>();

        // the set of parameters for the local tasks
        ByteBuffer[] localParams = new ByteBuffer[batchSize];

        // the set of parameters for the distributed tasks
        ArrayList<ByteBuffer> distributedParams = new ArrayList<ByteBuffer>();

        // check if all local fragment work is non-transactional
        boolean localFragsAreNonTransactional = true;

        // iterate over all sql in the batch, filling out the above data structures
        for (int i = 0; i < batchSize; ++i) {
            SQLStmt stmt = batchStmts[i];

            // check if the statement has been oked by the compiler/loader
            if (stmt.catStmt == null) {
                String msg = "SQLStmt objects cannot be instantiated after";
                msg += " VoltDB initialization. User may have instantiated a SQLStmt";
                msg += " inside a stored procedure's run method.";
                throw new RuntimeException(msg);
            }

            // Figure out what is needed to resume the proc
            int collectorOutputDepId = m_currentTxnState.getNextDependencyId();
            depsToResume[i] = collectorOutputDepId;

            // Build the set of params for the frags
            ParameterSet paramSet = getCleanParams(stmt, batchArgs[i]);
            fs.clear();
            try {
                fs.writeObject(paramSet);
            } catch (IOException e) {
                e.printStackTrace();
                assert(false);
            }
            ByteBuffer params = fs.getBuffer();
            assert(params != null);

            // populate the actual lists of fragments and params
            int numFrags = stmt.catStmt.getFragments().size();
            assert(numFrags > 0);
            assert(numFrags <= 2);

            if (numFrags == 1) {
                for (PlanFragment frag : stmt.catStmt.getFragments()) {
                    assert(frag != null);
                    assert(frag.getHasdependencies() == false);

                    localFragIds[i] = CatalogUtil.getUniqueIdForFragment(frag);
                    localParams[i] = params;

                    // if any frag is transactional, update this check
                    if (frag.getNontransactional() == false)
                        localFragsAreNonTransactional = true;
                }
                depsForLocalTask[i] = -1;
            }
            else {
                for (PlanFragment frag : stmt.catStmt.getFragments()) {
                    assert(frag != null);

                    // frags with no deps are usually collector frags that go to all partitions
                    if (frag.getHasdependencies() == false) {
                        distributedFragIds.add(CatalogUtil.getUniqueIdForFragment(frag));
                        distributedParams.add(params);
                    }
                    // frags with deps are usually aggregator frags
                    else {
                        localFragIds[i] = CatalogUtil.getUniqueIdForFragment(frag);
                        localParams[i] = params;
                        assert(frag.getHasdependencies());
                        int outputDepId =
                            m_currentTxnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
                        depsForLocalTask[i] = outputDepId;
                        distributedOutputDepIds.add(outputDepId);

                        // if any frag is transactional, update this check
                        if (frag.getNontransactional() == false)
                            localFragsAreNonTransactional = true;
                    }
                }
            }
        }

        // convert a bunch of arraylists into arrays
        // this should be easier, but we also want little-i ints rather than Integers
        long[] distributedFragIdArray = new long[distributedFragIds.size()];
        int[] distributedOutputDepIdArray = new int[distributedFragIds.size()];
        ByteBuffer[] distributedParamsArray = new ByteBuffer[distributedFragIds.size()];

        assert(distributedFragIds.size() == distributedParams.size());

        for (int i = 0; i < distributedFragIds.size(); i++) {
            distributedFragIdArray[i] = distributedFragIds.get(i);
            distributedOutputDepIdArray[i] = distributedOutputDepIds.get(i);
            distributedParamsArray[i] = distributedParams.get(i);
        }

        // instruct the dtxn what's needed to resume the proc
        m_currentTxnState.setupProcedureResume(finalTask, depsToResume);

        // create all the local work for the transaction
        FragmentTaskMessage localTask = new FragmentTaskMessage(m_currentTxnState.initiatorSiteId,
                                                  m_site.getCorrespondingSiteId(),
                                                  m_currentTxnState.txnId,
                                                  m_currentTxnState.isReadOnly,
                                                  localFragIds,
                                                  depsToResume,
                                                  localParams,
                                                  false);
        for (int i = 0; i < depsForLocalTask.length; i++) {
            if (depsForLocalTask[i] < 0) continue;
            localTask.addInputDepId(i, depsForLocalTask[i]);
        }

        // note: non-transactional work only helps us if it's final work
        m_currentTxnState.createLocalFragmentWork(localTask, localFragsAreNonTransactional && finalTask);

        // create and distribute work for all sites in the transaction
        FragmentTaskMessage distributedTask = new FragmentTaskMessage(m_currentTxnState.initiatorSiteId,
                                                        m_site.getCorrespondingSiteId(),
                                                        m_currentTxnState.txnId,
                                                        m_currentTxnState.isReadOnly,
                                                        distributedFragIdArray,
                                                        distributedOutputDepIdArray,
                                                        distributedParamsArray,
                                                        finalTask);

        m_currentTxnState.createAllParticipatingFragmentWork(distributedTask);

        // recursively call recurableRun and don't allow it to shutdown
        Map<Integer,List<VoltTable>> mapResults =
            m_site.recursableRun(m_currentTxnState);

        assert(mapResults != null);
        assert(depsToResume != null);
        assert(depsToResume.length == batchSize);

        // build an array of answers, assuming one result per expected id
        for (int i = 0; i < batchSize; i++) {
            List<VoltTable> matchingTablesForId = mapResults.get(depsToResume[i]);
            assert(matchingTablesForId != null);
            assert(matchingTablesForId.size() == 1);
            results[i] = matchingTablesForId.get(0);

            if (batchStmts[i].catStmt.getReplicatedtabledml()) {
                long newVal = results[i].asScalarLong() / numberOfPartitions;
                results[i] = new VoltTable(new VoltTable.ColumnInfo("", VoltType.BIGINT));
                results[i].addRow(newVal);
            }
        }

        return results;
    }

    /**
     *
     * @param e
     * @return A ClientResponse containing error information
     */
    private ClientResponseImpl getErrorResponse(Throwable e) {
        StackTraceElement[] stack = e.getStackTrace();
        ArrayList<StackTraceElement> matches = new ArrayList<StackTraceElement>();
        for (StackTraceElement ste : stack) {
            if (ste.getClassName() == getClass().getName())
                matches.add(ste);
        }

        byte status = ClientResponseImpl.UNEXPECTED_FAILURE;
        StringBuilder msg = new StringBuilder();

        if (e.getClass() == VoltAbortException.class) {
            status = ClientResponseImpl.USER_ABORT;
            msg.append("USER ABORT\n");
        }
        else if (e.getClass() == org.voltdb.exceptions.ConstraintFailureException.class) {
            status = ClientResponseImpl.GRACEFUL_FAILURE;
            msg.append("CONSTRAINT VIOLATION\n");
        }
        else if (e.getClass() == org.voltdb.exceptions.SQLException.class) {
            status = ClientResponseImpl.GRACEFUL_FAILURE;
            msg.append("SQL ERROR\n");
        }
        else if (e.getClass() == org.voltdb.ExpectedProcedureException.class) {
            msg.append("HSQL-BACKEND ERROR\n");
            if (e.getCause() != null)
                e = e.getCause();
        }
        else {
            msg.append("UNEXPECTED FAILURE:\n");
        }

        String exMsg = e.getMessage();
        if (exMsg == null)
            if (e.getClass() == NullPointerException.class)
                exMsg = "Null Pointer Exception";
            else
                exMsg = "Possible Null Pointer Exception";


        msg.append("  ").append(exMsg);

        for (StackTraceElement ste : matches) {
            msg.append("\n    at ");
            msg.append(ste.getClassName()).append(".").append(ste.getMethodName());
            msg.append("(").append(ste.getFileName()).append(":");
            msg.append(ste.getLineNumber()).append(")");
        }

        return getErrorResponse(
                status, msg.toString(),
                e instanceof SerializableException ? (SerializableException)e : null);
    }

    private ClientResponseImpl getErrorResponse(byte status, String msg, SerializableException e) {

        StringBuilder msgOut = new StringBuilder();
        msgOut.append("\n===============================================================================\n");
        msgOut.append("VOLTDB ERROR: ");
        msgOut.append(msg);
        msgOut.append("\n===============================================================================\n");

        log.trace(msgOut);

        return new ClientResponseImpl(status, new VoltTable[0], msgOut.toString(), e);
    }
}
