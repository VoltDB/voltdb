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

package org.voltdb;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.ProcedureCompiler;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class ProcedureRunner {

    private static final VoltLogger log = new VoltLogger("HOST");

    // SQL statement queue info
    //
    // This must match MAX_BATCH_COUNT in src/ee/execution/VoltDBEngine.h
    final static int MAX_BATCH_SIZE = 1000;
    static class QueuedSQL {
        SQLStmt stmt;
        ParameterSet params;
        Expectation expectation;
    }
    protected final ArrayList<QueuedSQL> m_batch = new ArrayList<QueuedSQL>(100);
    // cached fake SQLStmt array for single statement non-java procs
    QueuedSQL m_cachedSingleStmt = new QueuedSQL(); // never null
    boolean m_seenFinalBatch = false;

    // reflected info
    //
    protected final String m_procedureName;
    protected final VoltProcedure m_procedure;
    protected Method m_procMethod;
    protected Class<?>[] m_paramTypes;

    // per txn state (are reset after call)
    //
    protected long m_txnId = -1; // determinism id, not ordering id
    protected TransactionState m_txnState; // used for sysprocs only
    // Status code that can be set by stored procedure upon invocation that will be returned with the response.
    protected byte m_statusCode = Byte.MIN_VALUE;
    protected String m_statusString = null;
    // cached txnid-seeded RNG so all calls to getSeededRandomNumberGenerator() for
    // a given call don't re-seed and generate the same number over and over
    private Random m_cachedRNG = null;

    // hooks into other parts of voltdb
    //
    protected final SiteProcedureConnection m_site;
    protected final SystemProcedureExecutionContext m_systemProcedureContext;

    // per procedure state and catalog info
    //
    protected ProcedureStatsCollector m_statsCollector;
    protected final Procedure m_catProc;
    protected final boolean m_isSysProc;

    // dependency ids for ad hoc
    protected final static int AGG_DEPID = 1;

    // Used to get around the "abstract" for StmtProcedures.
    // Path of least resistance?
    static class StmtProcedure extends VoltProcedure {
        public final SQLStmt sql = new SQLStmt("TBD");
    }


    ProcedureRunner(VoltProcedure procedure,
                    SiteProcedureConnection site,
                    SystemProcedureExecutionContext sysprocContext,
                    Procedure catProc) {
        m_procedureName = procedure.getClass().getSimpleName();
        m_procedure = procedure;
        m_isSysProc = procedure instanceof VoltSystemProcedure;
        m_catProc = catProc;
        m_site = site;
        m_systemProcedureContext = sysprocContext;

        m_procedure.init(this);

        m_statsCollector = new ProcedureStatsCollector(
                m_site.getCorrespondingSiteId(),
                m_site.getCorrespondingPartitionId(),
                m_catProc);
        VoltDB.instance().getStatsAgent().registerStatsSource(
                SysProcSelector.PROCEDURE,
                site.getCorrespondingSiteId(),
                m_statsCollector);

        reflect();
    }

    boolean isSystemProcedure() {
        return m_isSysProc;
    }

    /**
     * Note this fails for Sysprocs that use it in non-coordinating fragment work. Don't.
     * @return The transaction id for determinism, not for ordering.
     */
    long getTransactionId() {
        assert(m_txnId > 0);
        return m_txnId;
    }

    Random getSeededRandomNumberGenerator() {
        // this value is memoized here and reset at the beginning of call(...).
        if (m_cachedRNG == null) {
            m_cachedRNG = new Random(getTransactionId());
        }
        return m_cachedRNG;
    }

    ClientResponseImpl call(long txnId, Object... paramListIn) {
        // verify per-txn state has been reset
        assert(m_txnId == -1);
        assert(m_statusCode == Byte.MIN_VALUE);
        assert(m_statusString == null);
        assert(m_cachedRNG == null);

        // Can be reassigned
        Object[] paramList = paramListIn;

        m_txnId = txnId;
        assert(m_txnId > 0);

        ClientResponseImpl retval = null;
        // assert no sql is queued
        assert(m_batch.size() == 0);

        try {
            m_statsCollector.beginProcedure();

            byte status = ClientResponse.SUCCESS;
            VoltTable[] results = null;

            // inject sysproc execution context as the first parameter.
            if (isSystemProcedure()) {
                final Object[] combinedParams = new Object[paramList.length + 1];
                combinedParams[0] = m_systemProcedureContext;
                for (int i=0; i < paramList.length; ++i) combinedParams[i+1] = paramList[i];
                // swap the lists.
                paramList = combinedParams;
            }

            if (paramList.length != m_paramTypes.length) {
                m_statsCollector.endProcedure( false, true);
                String msg = "PROCEDURE " + m_procedureName + " EXPECTS " + String.valueOf(m_paramTypes.length) +
                    " PARAMS, BUT RECEIVED " + String.valueOf(paramList.length);
                status = ClientResponse.GRACEFUL_FAILURE;
                return getErrorResponse(status, msg, null);
            }

            for (int i = 0; i < m_paramTypes.length; i++) {
                try {
                    paramList[i] = tryToMakeCompatible( i, paramList[i]);
                } catch (Exception e) {
                    m_statsCollector.endProcedure( false, true);
                    String msg = "PROCEDURE " + m_procedureName + " TYPE ERROR FOR PARAMETER " + i +
                            ": " + e.getMessage();
                    status = ClientResponse.GRACEFUL_FAILURE;
                    return getErrorResponse(status, msg, null);
                }
            }

            boolean error = false;
            boolean abort = false;
            // run a regular java class
            if (m_catProc.getHasjava()) {
                try {
                    if (log.isTraceEnabled()) {
                        log.trace("invoking... procMethod=" + m_procMethod.getName() + ", class=" + getClass().getName());
                    }
                    try {
                        Object rawResult = m_procMethod.invoke(m_procedure, paramList);
                        results = getResultsFromRawResults(rawResult);
                    } catch (IllegalAccessException e) {
                        // If reflection fails, invoke the same error handling that other exceptions do
                        throw new InvocationTargetException(e);
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
                        m_statsCollector.endProcedure( false, true);
                        throw (Error)ex;
                    }

                    retval = getErrorResponse(ex);
                }
            }
            // single statement only work
            // (this could be made faster, but with less code re-use)
            else {
                assert(m_catProc.getStatements().size() == 1);
                try {
                    m_cachedSingleStmt.params = getCleanParams(
                            m_cachedSingleStmt.stmt, paramList);
                    if (getHsqlBackendIfExists() != null) {
                        // HSQL handling
                        VoltTable table =
                            getHsqlBackendIfExists().runSQLWithSubstitutions(
                                m_cachedSingleStmt.stmt, m_cachedSingleStmt.params);
                        results = new VoltTable[] { table };
                    }
                    else {
                        m_batch.add(m_cachedSingleStmt);
                        results = voltExecuteSQL(true);
                    }
                }
                catch (SerializableException ex) {
                    retval = getErrorResponse(ex);
                }
            }

            m_statsCollector.endProcedure( abort, error);

            // don't leave empty handed
            if (results == null)
                results = new VoltTable[0];

            if (retval == null)
                retval = new ClientResponseImpl(
                        status,
                        m_statusCode,
                        m_statusString,
                        results,
                        null);
        }
        finally {
            // finally at the call(..) scope to ensure params can be
            // garbage collected and that the queue will be empty for
            // the next call
            m_batch.clear();

            // reset other per-txn state
            m_txnId = -1;
            m_txnState = null;
            m_statusCode = Byte.MIN_VALUE;
            m_statusString = null;
            m_cachedRNG = null;
            m_cachedSingleStmt.params = null;
            m_cachedSingleStmt.expectation = null;
            m_seenFinalBatch = false;
        }

        return retval;
    }

    public void setupTransaction(TransactionState txnState) {
        m_txnState = txnState;
    }

    public TransactionState getTxnState() {
        assert(m_isSysProc);
        return m_txnState;
    }

    /**
     * If returns non-null, then using hsql backend
     */
    public HsqlBackend getHsqlBackendIfExists() {
        return m_site.getHsqlBackendIfExists();
    }

    public void setAppStatusCode(byte statusCode) {
        m_statusCode = statusCode;
    }

    public void setAppStatusString(String statusString) {
        m_statusString = statusString;
    }

    public Date getTransactionTime() {
        long ts = TransactionIdManager.getTimestampFromTransactionId(getTransactionId());
        return new Date(ts);
    }

    public void voltQueueSQL(final SQLStmt stmt, Expectation expectation, Object... args) {
        if (m_batch.size() >= MAX_BATCH_SIZE) {
            throw new RuntimeException("Procedure attempted to queue more than " + MAX_BATCH_SIZE +
                    "statements in a single batch.\n  You may use multiple batches of up to 1000 statements," +
                    "each,\n  but you may also want to consider dividing this work into multiple procedures.");
        }
        QueuedSQL queuedSQL = new QueuedSQL();
        queuedSQL.expectation = expectation;
        queuedSQL.params = getCleanParams(stmt, args);
        queuedSQL.stmt = stmt;
        m_batch.add(queuedSQL);
    }

    public void voltQueueSQL(final SQLStmt stmt, Object... args) {
        voltQueueSQL(stmt, (Expectation) null, args);
    }

    public VoltTable[] voltExecuteSQL(boolean isFinalSQL) {
        if (m_seenFinalBatch) {
            throw new RuntimeException("Procedure " + m_procedureName +
                                       " attempted to execute a batch " +
                                       "after claiming a previous batch was final " +
                                       "and will be aborted.\n  Examine calls to " +
                                       "voltExecuteSQL() and verify that the call " +
                                       "with the argument value 'true' is actually " +
                                       "the final one");
        }
        m_seenFinalBatch = isFinalSQL;
        return executeQueriesInABatch(m_batch, isFinalSQL);
    }

    protected VoltTable[] executeQueriesInABatch(List<QueuedSQL> batch, boolean isFinalSQL) {
        final int batchSize = batch.size();

        VoltTable[] results = null;

        if (batchSize == 0)
            return new VoltTable[] {};

        boolean slowPath = false;
        for (int i = 0; i < batchSize; ++i) {
            final SQLStmt stmt = batch.get(i).stmt;
            // if any stmt is not single sited in this batch, the
            // full batch must take the slow path through the dtxn
            if (!stmt.isSinglePartition()) {
                slowPath = true;
                break;
            }
        }

        // IF THIS IS HSQL, RUN THE QUERIES DIRECTLY IN HSQL
        if (getHsqlBackendIfExists() != null) {
            results = new VoltTable[batchSize];
            int i = 0;
            for (QueuedSQL qs : batch) {
                results[i++] = getHsqlBackendIfExists().runSQLWithSubstitutions(qs.stmt, qs.params);
            }
        }

        // FOR MP-TXNS
        else if (slowPath) {
            results = slowPath(batch, isFinalSQL);
        }

        // FOR SP-TXNS (or all replicated read MPs)
        else {
            results = fastPath(batch);
        }

        // check expectations
        int i = 0; for (QueuedSQL qs : batch) {
            Expectation.check(m_procedureName, qs.stmt.getText(),
                    i, qs.expectation, results[i]);
            i++;
        }

        // clear the queued sql list for the next call
        batch.clear();

        return results;
    }

    public void voltLoadTable(String clusterName, String databaseName,
                              String tableName, VoltTable data)
    throws VoltAbortException
    {
        if (data == null || data.getRowCount() == 0) {
            return;
        }
        try {
            m_site.loadTable(m_txnState.txnId,
                             clusterName, databaseName,
                             tableName, data);
        }
        catch (EEException e) {
            throw new VoltAbortException("Failed to load table: " + tableName);
        }
    }

    DependencyPair executePlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params) {
        setupTransaction(txnState);
        assert (m_procedure instanceof VoltSystemProcedure);
        VoltSystemProcedure sysproc = (VoltSystemProcedure) m_procedure;
        return sysproc.executePlanFragment(dependencies, fragmentId, params, m_systemProcedureContext);
    }

    protected ParameterSet getCleanParams(SQLStmt stmt, Object... args) {
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
                args[ii] = VoltType.NULL_FLOAT;
            else if (type == VoltType.TIMESTAMP)
                args[ii] = new TimestampType(Long.MIN_VALUE);
            else if (type == VoltType.STRING)
                args[ii] = VoltType.NULL_STRING_OR_VARBINARY;
            else if (type == VoltType.VARBINARY)
                args[ii] = VoltType.NULL_STRING_OR_VARBINARY;
            else if (type == VoltType.DECIMAL)
                args[ii] = VoltType.NULL_DECIMAL;
            else
                throw new ExpectedProcedureException("Unknown type " + type +
                 " can not be converted to NULL representation for arg " + ii + " for SQL stmt " + stmt.getText());
        }

        final ParameterSet params = new ParameterSet();
        params.setParameters(args);
        return params;
    }

    public static void initSQLStmt(SQLStmt stmt, Statement catStmt) {
        stmt.catStmt = catStmt;
        stmt.numFragGUIDs = catStmt.getFragments().size();
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

    protected void reflect() {
        // fill in the sql for single statement procs
        if (m_catProc.getHasjava() == false) {
            try {
                Map<String, Field> stmtMap = ProcedureCompiler.getValidSQLStmts(null, m_procedureName, m_procedure.getClass(), true);
                Field f = stmtMap.get(VoltDB.ANON_STMT_NAME);
                assert(f != null);
                SQLStmt stmt = (SQLStmt) f.get(m_procedure);
                Statement statement = m_catProc.getStatements().get(VoltDB.ANON_STMT_NAME);
                stmt.sqlText = statement.getSqltext();
                m_cachedSingleStmt.stmt = stmt;

                int numParams = m_catProc.getParameters().size();
                m_paramTypes = new Class<?>[numParams];

                for (ProcParameter param : m_catProc.getParameters()) {
                    VoltType type = VoltType.get((byte) param.getType());
                    if (type == VoltType.INTEGER) type = VoltType.BIGINT;
                    if (type == VoltType.SMALLINT) type = VoltType.BIGINT;
                    if (type == VoltType.TINYINT) type = VoltType.BIGINT;

                    m_paramTypes[param.getIndex()] = type.classFromType();
                }
            } catch (Exception e) {
                // shouldn't throw anything outside of the compiler
                e.printStackTrace();
            }
        }
        else {
            // parse the java run method
            Method[] methods = m_procedure.getClass().getDeclaredMethods();

            for (final Method m : methods) {
                String name = m.getName();
                if (name.equals("run")) {
                    if (Modifier.isPublic(m.getModifiers()) == false)
                        continue;
                    m_procMethod = m;
                    m_paramTypes = m.getParameterTypes();
                }
            }

            if (m_procMethod == null) {
                log.debug("No good method found in: " + m_procedure.getClass().getName());
            }
        }

        // iterate through the fields and deal with sql statements
        Map<String, Field> stmtMap = null;
        try {
            stmtMap = ProcedureCompiler.getValidSQLStmts(null, m_procedureName, m_procedure.getClass(), true);
        } catch (Exception e1) {
            // shouldn't throw anything outside of the compiler
            e1.printStackTrace();
            return;
        }

        Field[] fields = new Field[stmtMap.size()];
        int index = 0;
        for (Field f : stmtMap.values()) {
            fields[index++] = f;
        }
        for (final Field f : fields) {
            String name = f.getName();
            Statement s = m_catProc.getStatements().get(name);
            if (s != null) {
                try {
                    /*
                     * Cache all the information we need about the statements in this stored
                     * procedure locally instead of pulling them from the catalog on
                     * a regular basis.
                     */
                    SQLStmt stmt = (SQLStmt) f.get(m_procedure);

                    // done in a static method in an abstract class so users don't call it
                    initSQLStmt(stmt, s);

                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                //LOG.fine("Found statement " + name);
            }
        }
    }

    /** @throws Exception with a message describing why the types are incompatible. */
    final protected Object tryToMakeCompatible(int paramTypeIndex, Object param) throws Exception {
        if (param == null || param == VoltType.NULL_STRING_OR_VARBINARY ||
            param == VoltType.NULL_DECIMAL)
        {
            if (m_paramTypes[paramTypeIndex].isPrimitive()) {
                VoltType type = VoltType.typeFromClass(m_paramTypes[paramTypeIndex]);
                switch (type) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                    return type.getNullValue();
                default:
                    // other types return null below
                }
            }

            // Pass null reference to the procedure run() method. These null values will be
            // converted to a serialize-able NULL representation for the EE in getCleanParams()
            // when the parameters are serialized for the plan fragment.
            return null;
        }

        if (param instanceof SystemProcedureExecutionContext) {
            return param;
        }

        Class<?> pclass = param.getClass();

        // hack to make strings work with input as byte[]
        if ((m_paramTypes[paramTypeIndex] == String.class) && (pclass == byte[].class)) {
            String sparam = null;
            sparam = new String((byte[]) param, "UTF-8");
            return sparam;
        }

        // hack to make varbinary work with input as string
        if ((m_paramTypes[paramTypeIndex] == byte[].class) && (pclass == String.class)) {
            return Encoder.hexDecode((String) param);
        }

        boolean slotIsArray = m_paramTypes[paramTypeIndex].isArray();
        if (slotIsArray != pclass.isArray())
            throw new Exception("Array / Scalar parameter mismatch");

        if (slotIsArray) {
            Class<?> pSubCls = pclass.getComponentType();
            Class<?> sSubCls = m_paramTypes[paramTypeIndex].getComponentType();
            if (pSubCls == sSubCls) {
                return param;
            }
            // if it's an empty array, let it through
            // this is a bit ugly as it might hide passing
            //  arrays of the wrong type, but it "does the right thing"
            //  more often that not I guess...
            else if (Array.getLength(param) == 0) {
                return Array.newInstance(sSubCls, 0);
            }
            else {
                /*
                 * Arrays can be quite large so it doesn't make sense to silently do the conversion
                 * and incur the performance hit. The client should serialize the correct invocation
                 * parameters
                 */
                throw new Exception(
                        "tryScalarMakeCompatible: Unable to match parameter array:"
                        + sSubCls.getName() + " to provided " + pSubCls.getName());
            }
        }

        /*
         * inline tryScalarMakeCompatible so we can save on reflection
         */
        final Class<?> slot = m_paramTypes[paramTypeIndex];
        if ((slot == long.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == int.class) && (pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == short.class) && (pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == byte.class) && (pclass == Byte.class)) return param;
        if ((slot == double.class) && (param instanceof Number)) return ((Number)param).doubleValue();
        if ((slot == String.class) && (pclass == String.class)) return param;
        if (slot == TimestampType.class) {
            if (pclass == Long.class) return new TimestampType((Long)param);
            if (pclass == TimestampType.class) return param;
            if (pclass == Date.class) return new TimestampType((Date) param);
            // if a string is given for a date, use java's JDBC parsing
            if (pclass == String.class) {
                try {
                    return new TimestampType((String)param);
                }
                catch (IllegalArgumentException e) {
                    // ignore errors if it's not the right format
                }
            }
        }
        if (slot == BigDecimal.class) {
            if ((pclass == Long.class) || (pclass == Integer.class) ||
                (pclass == Short.class) || (pclass == Byte.class)) {
                BigInteger bi = new BigInteger(param.toString());
                BigDecimal bd = new BigDecimal(bi);
                bd = bd.setScale(VoltDecimalHelper.kDefaultScale, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == BigDecimal.class) {
                BigDecimal bd = (BigDecimal) param;
                bd = bd.setScale(VoltDecimalHelper.kDefaultScale, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == String.class) {
                BigDecimal bd = VoltDecimalHelper.deserializeBigDecimalFromString((String) param);
                return bd;
            }
        }
        if (slot == VoltTable.class && pclass == VoltTable.class) {
            return param;
        }

        // handle truncation for integers

        // Long targeting int parameter
        if ((slot == int.class) && (pclass == Long.class)) {
            long val = ((Number) param).longValue();

            // if it's in the right range, and not null (target null), crop the value and return
            if ((val <= Integer.MAX_VALUE) && (val >= Integer.MIN_VALUE) && (val != VoltType.NULL_INTEGER))
                return ((Number) param).intValue();
        }

        // Long or Integer targeting short parameter
        if ((slot == short.class) && (pclass == Long.class || pclass == Integer.class)) {
            long val = ((Number) param).longValue();

            // if it's in the right range, and not null (target null), crop the value and return
            if ((val <= Short.MAX_VALUE) && (val >= Short.MIN_VALUE) && (val != VoltType.NULL_SMALLINT))
                return ((Number) param).shortValue();
        }

        // Long, Integer or Short targeting byte parameter
        if ((slot == byte.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class)) {
            long val = ((Number) param).longValue();

            // if it's in the right range, and not null (target null), crop the value and return
            if ((val <= Byte.MAX_VALUE) && (val >= Byte.MIN_VALUE) && (val != VoltType.NULL_TINYINT))
                return ((Number) param).byteValue();
        }

        throw new Exception(
                "tryToMakeCompatible: Unable to match parameters or out of range for taget param: "
                + slot.getName() + " to provided " + pclass.getName());
    }

    /**
    *
    * @param e
    * @return A ClientResponse containing error information
    */
   protected ClientResponseImpl getErrorResponse(Throwable eIn) {
       // can be reassigned so use local variable
       Throwable e = eIn;
       boolean expected_failure = true;
       StackTraceElement[] stack = e.getStackTrace();
       ArrayList<StackTraceElement> matches = new ArrayList<StackTraceElement>();
       for (StackTraceElement ste : stack) {
           if (ste.getClassName().equals(m_procedure.getClass().getName()))
               matches.add(ste);
       }

       byte status = ClientResponse.UNEXPECTED_FAILURE;
       StringBuilder msg = new StringBuilder();

       if (e.getClass() == VoltAbortException.class) {
           status = ClientResponse.USER_ABORT;
           msg.append("USER ABORT\n");
       }
       else if (e.getClass() == org.voltdb.exceptions.ConstraintFailureException.class) {
           status = ClientResponse.GRACEFUL_FAILURE;
           msg.append("CONSTRAINT VIOLATION\n");
       }
       else if (e.getClass() == org.voltdb.exceptions.SQLException.class) {
           status = ClientResponse.GRACEFUL_FAILURE;
           msg.append("SQL ERROR\n");
       }
       else if (e.getClass() == org.voltdb.ExpectedProcedureException.class) {
           msg.append("HSQL-BACKEND ERROR\n");
           if (e.getCause() != null)
               e = e.getCause();
       }
       else {
           msg.append("UNEXPECTED FAILURE:\n");
           expected_failure = false;
       }

       // if the error is something we know can happen as part of normal
       // operation, reduce the verbosity.  Otherwise, generate
       // more output for debuggability
       if (expected_failure)
       {
           msg.append("  ").append(e.getMessage());
           for (StackTraceElement ste : matches) {
               msg.append("\n    at ");
               msg.append(ste.getClassName()).append(".").append(ste.getMethodName());
               msg.append("(").append(ste.getFileName()).append(":");
               msg.append(ste.getLineNumber()).append(")");
           }
       }
       else
       {
           Writer result = new StringWriter();
           PrintWriter pw = new PrintWriter(result);
           e.printStackTrace(pw);
           msg.append("  ").append(result.toString());
       }

       return getErrorResponse(
               status, msg.toString(),
               e instanceof SerializableException ? (SerializableException)e : null);
   }

   protected ClientResponseImpl getErrorResponse(byte status, String msg, SerializableException e) {

       StringBuilder msgOut = new StringBuilder();
       msgOut.append("VOLTDB ERROR: ");
       msgOut.append(msg);

       log.trace(msgOut);

       return new ClientResponseImpl(
               status,
               m_statusCode,
               m_statusString,
               new VoltTable[0],
               msgOut.toString(), e);
   }

   /**
    * Given the results of a procedure, convert it into a sensible array of VoltTables.
    * @throws InvocationTargetException
    */
   final private VoltTable[] getResultsFromRawResults(Object result) throws InvocationTargetException {
       if (result == null) {
           return new VoltTable[0];
       }
       if (result instanceof VoltTable[]) {
           VoltTable[] retval = (VoltTable[]) result;
           for (VoltTable table : retval)
               if (table == null) {
                   Exception e = new RuntimeException("VoltTable arrays with non-zero length cannot contain null values.");
                   throw new InvocationTargetException(e);
               }

           return retval;
       }
       if (result instanceof VoltTable) {
           return new VoltTable[] { (VoltTable) result };
       }
       if (result instanceof Long) {
           VoltTable t = new VoltTable(new VoltTable.ColumnInfo("", VoltType.BIGINT));
           t.addRow(result);
           return new VoltTable[] { t };
       }
       throw new RuntimeException("Procedure didn't return acceptable type.");
   }

   VoltTable[] executeQueriesInIndividualBatches(List<QueuedSQL> batch, boolean finalTask) {
       assert(batch.size() > 0);

       VoltTable[] retval = new VoltTable[batch.size()];

       ArrayList<QueuedSQL> microBatch = new ArrayList<QueuedSQL>();

       for (int i = 0; i < batch.size(); i++) {
           QueuedSQL queuedSQL = batch.get(i);
           assert(queuedSQL != null);

           microBatch.add(queuedSQL);

           boolean isThisLoopFinalTask = finalTask && (i == (batch.size() - 1));
           assert(microBatch.size() == 1);
           VoltTable[] results = executeQueriesInABatch(microBatch, isThisLoopFinalTask);
           assert(results != null);
           assert(results.length == 1);
           retval[i] = results[0];

           microBatch.clear();
       }

       return retval;
   }

   private VoltTable[] slowPath(List<QueuedSQL> batch, final boolean finalTask) {
       // executeQueuedSQL() maintains order, but groups planned vs. unplanned.
       return executeQueuedSQL(batch,
           new FragmentExecutor() {
               @Override
               public VoltTable[] onExecutePrePlanned(final List<QueuedSQL> batch, final boolean last) {
                   /*
                    * Determine if reads and writes are mixed. Can't mix reads and writes
                    * because the order of execution is wrong when replicated tables are
                    * involved due to ENG-1232.
                    */
                   boolean hasRead = false;
                   boolean hasWrite = false;
                   for (int i = 0; i < batch.size(); ++i) {
                       final SQLStmt stmt = batch.get(i).stmt;
                       if (stmt.catStmt != null) {
                           if (stmt.catStmt.getReadonly()) {
                               hasRead = true;
                           } else {
                               hasWrite = true;
                           }
                       }
                       /*
                        * If they are all reads or all writes then we can use the batching
                        * slow path Otherwise the order of execution will be interleaved
                        * incorrectly so we have to do each statement individually.
                        */
                       if (hasRead && hasWrite) {
                           return executeQueriesInIndividualBatches(batch, finalTask);
                       }
                   }

                   return executeSlowHomogeneousBatch(batch, finalTask && last);
               }

               @Override
               public VoltTable[] onExecuteUnplanned(final List<QueuedSQL> batch, final boolean last) {
                   /*
                    * Submit individual queries. Get smarter some day. This check breaks
                    * the potentially infinite recursion.
                    */
                   if (batch.size() > 1) {
                       return executeQueriesInIndividualBatches(batch, finalTask && last);
                   }
                   else {
                       return executeSlowHomogeneousBatch(batch, finalTask && last);
                   }
               }
           }
       );
   }

   /**
    * Used by executeSlowHomogeneousBatch() to build messages and keep track
    * of other information as the batch is processed.
    */
   private static class BatchState {

       // needed to size arrays and check index arguments
       final int m_batchSize;

       // needed to get various IDs
       private final TransactionState m_txnState;

       // the set of dependency ids for the expected results of the batch
       // one per sql statment
       final int[] m_depsToResume;

       // these dependencies need to be received before the local stuff can run
       final int[] m_depsForLocalTask;

       // check if all local fragment work is non-transactional
       boolean m_localFragsAreNonTransactional = false;

       // the data and message for locally processed fragments
       final FragmentTaskMessage m_localTask;

       // the data and message for all sites in the transaction
       final FragmentTaskMessage m_distributedTask;

       // holds query results
       final VoltTable[] m_results;

       BatchState(int batchSize, TransactionState txnState, long siteId, boolean finalTask) {
           m_batchSize = batchSize;
           m_txnState = txnState;

           m_depsToResume = new int[batchSize];
           m_depsForLocalTask = new int[batchSize];
           m_results = new VoltTable[batchSize];

           // the data and message for locally processed fragments
           m_localTask = new FragmentTaskMessage(m_txnState.initiatorHSId,
                                                 siteId,
                                                 m_txnState.txnId,
                                                 m_txnState.isReadOnly(),
                                                 false);

           // the data and message for all sites in the transaction
           m_distributedTask = new FragmentTaskMessage(m_txnState.initiatorHSId,
                                                       siteId,
                                                       m_txnState.txnId,
                                                       m_txnState.isReadOnly(),
                                                       finalTask);
       }

       /*
        * Replicated fragment.
        */
       void addFragment(int index, PlanFragment frag, ByteBuffer params) {
           assert(index >= 0);
           assert(index < m_batchSize);
           assert(frag != null);
           assert(frag.getHasdependencies() == false);

           // if any frag is transactional, update this check
           if (frag.getNontransactional() == true)
               m_localFragsAreNonTransactional = true;

           long localFragId = CatalogUtil.getUniqueIdForFragment(frag);
           m_depsForLocalTask[index] = -1;
           // Add the local fragment data.
           m_localTask.addFragment(localFragId, m_depsToResume[index], params);

       }

       /*
        * Multi-partition/non-replicated fragment with collector and aggregator.
        */
       void addFragmentPair(int index,
                            PlanFragment collectorFragment,
                            PlanFragment aggregatorFragment,
                            ByteBuffer params) {
           assert(index >= 0);
           assert(index < m_batchSize);
           assert(collectorFragment != null);
           assert(aggregatorFragment != null);
           assert(collectorFragment.getHasdependencies() == false);
           assert(aggregatorFragment.getHasdependencies() == true);

           // frags with no deps are usually collector frags that go to all partitions
           long distributedFragId = CatalogUtil.getUniqueIdForFragment(collectorFragment);
           long localFragId = CatalogUtil.getUniqueIdForFragment(aggregatorFragment);
           // if any frag is transactional, update this check
           if (aggregatorFragment.getNontransactional() == true) {
               m_localFragsAreNonTransactional = true;
           }
           int outputDepId =
                   m_txnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
           m_depsForLocalTask[index] = outputDepId;
           // Add local and distributed fragments.
           m_localTask.addFragment(localFragId, m_depsToResume[index], params);
           m_distributedTask.addFragment(distributedFragId, outputDepId, params);
       }

       /*
        * Replicated custom fragment.
        */
       void addCustomFragment(int index, String aggregatorFragment, ByteBuffer params) {
           assert(index >= 0);
           assert(index < m_batchSize);
           assert(aggregatorFragment != null);

           m_depsForLocalTask[index] = -1;
           m_localTask.addCustomFragment(m_depsToResume[index], params, aggregatorFragment);
       }

       /*
        * Multi-partition/non-replicated custom fragment with collector and aggregator.
        */
       void addCustomFragmentPair(int index,
                                  String collectorFragment,
                                  String aggregatorFragment,
                                  ByteBuffer params) {
           assert(index >= 0);
           assert(index < m_batchSize);
           assert(collectorFragment != null);
           assert(aggregatorFragment != null);

           int outputDepId =
                   m_txnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
           m_depsForLocalTask[index] = outputDepId;
           // Add the aggegator and collector fragments.
           m_localTask.addCustomFragment(m_depsToResume[index], params, aggregatorFragment);
           m_distributedTask.addCustomFragment(outputDepId, params, collectorFragment);
       }
   }

   /*
    * Execute a batch of homogeneous queries, i.e. all reads or all writes.
    */
   VoltTable[] executeSlowHomogeneousBatch(final List<QueuedSQL> batch, final boolean finalTask) {

       BatchState state = new BatchState(batch.size(), m_txnState, m_site.getCorrespondingSiteId(), finalTask);

       // iterate over all sql in the batch, filling out the above data structures
       for (int i = 0; i < batch.size(); ++i) {
           QueuedSQL queuedSQL = batch.get(i);

           assert(queuedSQL.stmt != null);

           // Figure out what is needed to resume the proc
           int collectorOutputDepId = m_txnState.getNextDependencyId();
           state.m_depsToResume[i] = collectorOutputDepId;

           // Build the set of params for the frags
           FastSerializer fs = new FastSerializer();
           try {
               fs.writeObject(queuedSQL.params);
           } catch (IOException e) {
               throw new RuntimeException("Error serializing parameters for SQL statement: " +
                                          queuedSQL.stmt.getText() + " with params: " +
                                          queuedSQL.params.toJSONString(), e);
           }
           ByteBuffer params = fs.getBuffer();
           assert(params != null);

           // populate the actual lists of fragments and params
           if (queuedSQL.stmt.catStmt != null) {
               // Pre-planned query.

               int numFrags = queuedSQL.stmt.catStmt.getFragments().size();
               assert(numFrags > 0);
               assert(numFrags <= 2);

                /*
                 * This numfrags == 1 code is for routing multi-partition reads of a
                 * replicated table to the local site. This was a broken performance
                 * optimization. see https://issues.voltdb.com/browse/ENG-1232.
                 *
                 * The problem is that the fragments for the replicated read are not correctly
                 * interleaved with the distributed writes to the replicated table that might
                 * be in the same batch of SQL statements. We do end up doing the replicated
                 * read locally but we break up the batches in the face of mixed reads and
                 * writes
                 */
               Iterator<PlanFragment> fragmentIter = queuedSQL.stmt.catStmt.getFragments().iterator();
               if (numFrags == 1) {
                   PlanFragment frag = fragmentIter.next();
                   state.addFragment(i, frag, params);
               }
               else {
                   // collector/aggregator pair (guaranteed above that numFrags==2 here)
                   PlanFragment frag1 = fragmentIter.next();
                   assert(frag1 != null);
                   PlanFragment frag2 = fragmentIter.next();
                   assert(frag2 != null);
                   // frags with no deps are usually collector frags that go to all partitions
                   // figure out which frag is which type
                   if (frag1.getHasdependencies() == false) {
                       state.addFragmentPair(i, frag1, frag2, params);
                   }
                   else {
                       state.addFragmentPair(i, frag2, frag1, params);
                   }
               }
           }
           else {
               /*
                * Unplanned custom query. Requires an attached plan.
                * Set up collector and dependent aggregator fragments.
                */
               SQLStmtPlan plan = queuedSQL.stmt.getPlan();
               assert(plan != null);
               String collectorFragment  = plan.getCollectorFragment();
               String aggregatorFragment = plan.getAggregatorFragment();
               assert(aggregatorFragment != null);

               if (collectorFragment == null) {
                   // Multi-partition/non-replicated with collector and aggregator.
                   state.addCustomFragment(i, aggregatorFragment, params);
               }
               else {
                   // Multi-partition/replicated with just an aggregator fragment.
                   state.addCustomFragmentPair(i, collectorFragment, aggregatorFragment, params);
               }
           }
       }

       // instruct the dtxn what's needed to resume the proc
       m_txnState.setupProcedureResume(finalTask, state.m_depsToResume);

       // create all the local work for the transaction
       for (int i = 0; i < state.m_depsForLocalTask.length; i++) {
           if (state.m_depsForLocalTask[i] < 0) continue;
           state.m_localTask.addInputDepId(i, state.m_depsForLocalTask[i]);
       }

       // note: non-transactional work only helps us if it's final work
       m_txnState.createLocalFragmentWork(state.m_localTask,
                                          state.m_localFragsAreNonTransactional && finalTask);

       m_txnState.createAllParticipatingFragmentWork(state.m_distributedTask);

       // recursively call recursableRun and don't allow it to shutdown
       Map<Integer,List<VoltTable>> mapResults =
           m_site.recursableRun(m_txnState);

       assert(mapResults != null);
       assert(state.m_depsToResume != null);
       assert(state.m_depsToResume.length == batch.size());

       // build an array of answers, assuming one result per expected id
       for (int i = 0; i < batch.size(); i++) {
           List<VoltTable> matchingTablesForId = mapResults.get(state.m_depsToResume[i]);
           assert(matchingTablesForId != null);
           assert(matchingTablesForId.size() == 1);
           state.m_results[i] = matchingTablesForId.get(0);

           final SQLStmt stmt = batch.get(i).stmt;
           boolean isReplicated;
           if (stmt.catStmt != null) {
               isReplicated = stmt.catStmt.getReplicatedtabledml();
           }
           else {
               assert(stmt.plan != null);
               isReplicated = stmt.plan.isReplicatedTableDML();
           }
           if (isReplicated) {
               long newVal = state.m_results[i].asScalarLong() / m_site.getReplicatedDMLDivisor();
               state.m_results[i] = new VoltTable(new VoltTable.ColumnInfo("modified_tuples", VoltType.BIGINT));
               state.m_results[i].addRow(newVal);
           }
       }

       return state.m_results;
   }

   // Batch up pre-planned fragments, but handle ad hoc independently.
   private VoltTable[] fastPath(List<QueuedSQL> batch) {

       return executeQueuedSQL(batch,
           new FragmentExecutor() {
               @Override
               public VoltTable[] onExecutePrePlanned(final List<QueuedSQL> batch, final boolean last) {
                   final int batchSize = batch.size();
                   ParameterSet[] params = new ParameterSet[batchSize];
                   long[] fragmentIds = new long[batchSize];

                   int i = 0;
                   for (final QueuedSQL qs : batch) {
                       assert(qs.stmt.numFragGUIDs == 1);
                       fragmentIds[i] = qs.stmt.fragGUIDs[0];
                       params[i] = qs.params;
                       i++;
                   }
                   return m_site.executeQueryPlanFragmentsAndGetResults(
                       fragmentIds,
                       batchSize,   // 1 frag per stmt
                       params,      // 1 frag per stmt
                       batchSize,   // 1 frag per stmt
                       m_txnState.txnId,
                       m_catProc.getReadonly());
               }

               @Override
               public VoltTable[] onExecuteUnplanned(final List<QueuedSQL> batch, final boolean last) {
                   VoltTable[] results = new VoltTable[batch.size()];
                   for (int i = 0; i < batch.size(); i++) {
                       results[i] = m_site.executeCustomPlanFragment(
                               batch.get(i).stmt.getPlan().getAggregatorFragment(),
                               AGG_DEPID, m_txnState.txnId);
                   }
                   return results;
               }
           }
       );
   }

   private interface FragmentExecutor {
       public abstract VoltTable[] onExecuteUnplanned(final List<QueuedSQL> batch, final boolean last);

       public abstract VoltTable[] onExecutePrePlanned(final List<QueuedSQL> batch, final boolean last);
   }

   // Walk through the batch, process sub-batches, and collect results.
   // For now the batch size for custom (ad hoc) SQL is 1 since that's
   // how it's being handled anyway, plus it simplifies the logic here.
   // This can change if we implement support for larger custom batches.
   static private VoltTable[] executeQueuedSQL(final List<QueuedSQL> batch,
                                               FragmentExecutor executor) {
       int iFrom = 0;
       int iTo = 0;
       List<VoltTable> results = new ArrayList<VoltTable>();
       VoltTable[] subResults;
       for (; iTo < batch.size(); iTo++) {
           final QueuedSQL qs = batch.get(iTo);
           if (qs.stmt.plan != null) {
               if (iTo > iFrom) {
                   subResults = executor.onExecutePrePlanned(batch.subList(iFrom, iTo),
                                                             iTo == batch.size() - 1);
                   results.addAll(Arrays.asList(subResults));
                   iFrom = iTo + 1;
               }
               subResults = executor.onExecuteUnplanned(batch.subList(iTo, iTo + 1),
                                                        iTo == batch.size() - 1);
               results.addAll(Arrays.asList(subResults));
           }
       }
       if (iTo > iFrom) {
           subResults = executor.onExecutePrePlanned(batch.subList(iFrom, iTo), true);
           results.addAll(Arrays.asList(subResults));
           iFrom = iTo + 1;
       }
       return results.toArray(new VoltTable[results.size()]);
   }
}
