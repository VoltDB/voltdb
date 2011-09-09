/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.voltdb.Expectation.Type;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.compiler.ProcedureCompiler;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

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
    private static final VoltLogger log = new VoltLogger(VoltProcedure.class.getName());

    // Used to get around the "abstract" for StmtProcedures.
    // Path of least resistance?
    static class StmtProcedure extends VoltProcedure {}

    // This must match MAX_BATCH_COUNT in src/ee/execution/VoltDBEngine.h
    final static int MAX_BATCH_SIZE = 1000;

    final static Double DOUBLE_NULL = new Double(-1.7976931348623157E+308);

    /**
     * Expect an empty result set (0 rows)
     */
    public static final Expectation EXPECT_EMPTY = new Expectation(Type.EXPECT_EMPTY);

    /**
     * Expect a result set with exactly one row
     */
    public static final Expectation EXPECT_ONE_ROW = new Expectation(Type.EXPECT_ONE_ROW);

    /**
     * Expect a result set with one or no rows
     */
    public static final Expectation EXPECT_ZERO_OR_ONE_ROW = new Expectation(Type.EXPECT_ZERO_OR_ONE_ROW);

    /**
     * Expect a result set with one or more rows
     */
    public static final Expectation EXPECT_NON_EMPTY = new Expectation(Type.EXPECT_NON_EMPTY);

    /**
     * Expect a result set with a single row and a single column (scalar value)
     */
    public static final Expectation EXPECT_SCALAR = new Expectation(Type.EXPECT_SCALAR);

    /**
     * Expect a result with a single row and a single BIGINT column
     */
    public static final Expectation EXPECT_SCALAR_LONG = new Expectation(Type.EXPECT_SCALAR_LONG);

    /**
     * Expect a result with a single row and a single BIGINT column containing
     * the specified value. This factory method constructs an Expectation for the specified
     * value.
     * @param scalar The expected value the single row/column should contain
     * @return An Expectation that will cause an exception to be thrown if the value or schema doesn't match
     */
    public static final Expectation EXPECT_SCALAR_MATCH(long scalar) {
        return new Expectation(Type.EXPECT_SCALAR_MATCH, scalar);
    }

    protected HsqlBackend m_hsql;

    // simple name of this procedure gets set in init()
    private String m_procedureName = "UNKNOWN";

    //For runtime statistics collection
    private ProcedureStatsCollector m_statsCollector;

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
    private Method m_procMethod;
    private Class<?>[] m_paramTypes;
    private boolean m_paramTypeIsPrimitive[];
    private boolean m_paramTypeIsArray[];
    private Class<?> m_paramTypeComponentType[];
    private int m_paramTypesLength;
    private Procedure m_catProc;
    private boolean m_isNative = true;
    private int m_numberOfPartitions;

    // cached fake SQLStmt array for single statement non-java procs
    SQLStmt[] m_cachedSingleStmt = { null };

    // data copied from EE proc wrapper
    private final SQLStmt m_batchQueryStmts[] = new SQLStmt[MAX_BATCH_SIZE];
    private int m_batchQueryStmtIndex = 0;
    private Object[] m_batchQueryArgs[];
    private Expectation[] m_batchQueryExpectations = new Expectation[MAX_BATCH_SIZE];
    private final long m_fragmentIds[] = new long[MAX_BATCH_SIZE];
    private final int m_expectedDeps[] = new int[MAX_BATCH_SIZE];
    private ParameterSet m_parameterSets[];

    /**
     * Status code that can be set by stored procedure upon invocation that will be returned with the response.
     */
    private byte m_statusCode = Byte.MIN_VALUE;
    private String m_statusString = null;

    // data from hsql wrapper
    private final ArrayList<VoltTable> m_queryResults = new ArrayList<VoltTable>();

    // cached txnid-seeded RNG so all calls to getSeededRandomNumberGenerator() for
    // a given call don't re-seed and generate the same number over and over
    private Random m_cachedRNG = null;

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

        m_procedureName = getClass().getSimpleName();
        m_catProc = catProc;
        m_site = site;
        m_isNative = (eeType != BackendTarget.HSQLDB_BACKEND);
        m_hsql = hsql;
        m_cluster = cluster;
        m_numberOfPartitions = numberOfPartitions;
        m_statsCollector = new ProcedureStatsCollector();
        VoltDB.instance().getStatsAgent().registerStatsSource(
                SysProcSelector.PROCEDURE,
                Integer.parseInt(site.getCorrespondingCatalogSite().getTypeName()),
                m_statsCollector);

        // this is a stupid hack to make the EE happy
        for (int i = 0; i < m_expectedDeps.length; i++)
            m_expectedDeps[i] = 1;

        if (catProc.getHasjava()) {
            int tempParamTypesLength = 0;
            Method tempProcMethod = null;
            Method[] methods = getClass().getDeclaredMethods();
            Class<?> tempParamTypes[] = null;
            boolean tempParamTypeIsPrimitive[] = null;
            boolean tempParamTypeIsArray[] = null;
            Class<?> tempParamTypeComponentType[] = null;
            for (final Method m : methods) {
                String name = m.getName();
                if (name.equals("run")) {
                    if (Modifier.isPublic(m.getModifiers()) == false)
                        continue;
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
            m_paramTypesLength = tempParamTypesLength;
            m_procMethod = tempProcMethod;
            m_paramTypes = tempParamTypes;
            m_paramTypeIsPrimitive = tempParamTypeIsPrimitive;
            m_paramTypeIsArray = tempParamTypeIsArray;
            m_paramTypeComponentType = tempParamTypeComponentType;

            if (m_procMethod == null) {
                log.debug("No good method found in: " + getClass().getName());
            }

            // iterate through the fields and deal with
            Map<String, Field> stmtMap = null;
            try {
                stmtMap = ProcedureCompiler.getValidSQLStmts(null, m_procedureName, getClass(), true);
            } catch (Exception e1) {
                // shouldn't throw anything outside of the compiler
                e1.printStackTrace();
            }

            Field[] fields = new Field[stmtMap.size()];
            int index = 0;
            for (Field f : stmtMap.values()) {
                fields[index++] = f;
            }
            for (final Field f : fields) {
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
        // has no java
        else {
            Statement catStmt = catProc.getStatements().get(VoltDB.ANON_STMT_NAME);
            SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
            stmt.catStmt = catStmt;
            initSQLStmt(stmt);
            m_cachedSingleStmt[0] = stmt;

            m_procMethod = null;

            m_paramTypesLength = catProc.getParameters().size();

            m_paramTypes = new Class<?>[m_paramTypesLength];
            m_paramTypeIsPrimitive = new boolean[m_paramTypesLength];
            m_paramTypeIsArray = new boolean[m_paramTypesLength];
            m_paramTypeComponentType = new Class<?>[m_paramTypesLength];
            for (ProcParameter param : catProc.getParameters()) {
                VoltType type = VoltType.get((byte) param.getType());
                if (type == VoltType.INTEGER) type = VoltType.BIGINT;
                if (type == VoltType.SMALLINT) type = VoltType.BIGINT;
                if (type == VoltType.TINYINT) type = VoltType.BIGINT;
                m_paramTypes[param.getIndex()] = type.classFromType();
                m_paramTypeIsPrimitive[param.getIndex()] = m_paramTypes[param.getIndex()].isPrimitive();
                m_paramTypeIsArray[param.getIndex()] = param.getIsarray();
                assert(m_paramTypeIsArray[param.getIndex()] == false);
                m_paramTypeComponentType[param.getIndex()] = null;
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
        m_statusCode = Byte.MIN_VALUE;
        m_statusString = null;
        // kill the cache of the rng
        m_cachedRNG = null;
        m_statsCollector.beginProcedure();

        // in case sql was queued but executed
        m_batchQueryStmtIndex = 0;

        VoltTable[] results = new VoltTable[0];
        byte status = ClientResponseImpl.SUCCESS;

        if (paramList.length != m_paramTypesLength) {
            m_statsCollector.endProcedure( false, true);
            String msg = "PROCEDURE " + m_catProc.getTypeName() + " EXPECTS " + String.valueOf(m_paramTypesLength) +
                " PARAMS, BUT RECEIVED " + String.valueOf(paramList.length);
            status = ClientResponseImpl.GRACEFUL_FAILURE;
            return getErrorResponse(status, msg, null);
        }

        for (int i = 0; i < m_paramTypesLength; i++) {
            try {
                paramList[i] = tryToMakeCompatible( i, paramList[i]);
            } catch (Exception e) {
                m_statsCollector.endProcedure( false, true);
                String msg = "PROCEDURE " + m_catProc.getTypeName() + " TYPE ERROR FOR PARAMETER " + i +
                        ": " + e.getMessage();
                status = ClientResponseImpl.GRACEFUL_FAILURE;
                return getErrorResponse(status, msg, null);
            }
        }

        ClientResponseImpl retval = null;
        boolean error = false;
        boolean abort = false;
        // run a regular java class
        if (m_catProc.getHasjava()) {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("invoking... procMethod=" + m_procMethod.getName() + ", class=" + getClass().getName());
                }
                try {
                    m_batchQueryArgs = new Object[MAX_BATCH_SIZE][];
                    m_parameterSets = new ParameterSet[MAX_BATCH_SIZE];
                    Object rawResult = m_procMethod.invoke(this, paramList);
                    results = getResultsFromRawResults(rawResult);
                } catch (IllegalAccessException e) {
                    // If reflection fails, invoke the same error handling that other exceptions do
                    throw new InvocationTargetException(e);
                } finally {
                    m_batchQueryArgs = null;
                    m_parameterSets = null;
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
                m_batchQueryArgs = new Object[MAX_BATCH_SIZE][];
                m_parameterSets = new ParameterSet[MAX_BATCH_SIZE];
                if (!m_isNative) {
                    // HSQL handling
                    VoltTable table = m_hsql.runSQLWithSubstitutions(m_cachedSingleStmt[0], paramList);
                    results = new VoltTable[] { table };
                }
                else {
                    results = executeQueriesInABatch(1, m_cachedSingleStmt, new Object[][] { paramList } , true);
                }
            }
            catch (SerializableException ex) {
                retval = getErrorResponse(ex);
            } finally {
                m_batchQueryArgs = null;
                m_parameterSets = null;
            }
        }

        m_statsCollector.endProcedure( abort, error);

        if (retval == null)
            retval = new ClientResponseImpl(
                    status,
                    m_statusCode,
                    m_statusString,
                    results,
                    null);

        return retval;
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

    /** @throws Exception with a message describing why the types are incompatible. */
    final private Object tryToMakeCompatible(int paramTypeIndex, Object param) throws Exception {
        if (param == null || param == VoltType.NULL_STRING_OR_VARBINARY ||
            param == VoltType.NULL_DECIMAL)
        {
            if (m_paramTypeIsPrimitive[paramTypeIndex]) {
                VoltType type = VoltType.typeFromClass(m_paramTypes[paramTypeIndex]);
                switch (type) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                    return type.getNullValue();
                }
            }

            // Pass null reference to the procedure run() method. These null values will be
            // converted to a serialize-able NULL representation for the EE in getCleanParams()
            // when the parameters are serialized for the plan fragment.
            return null;
        }

        if (param instanceof ExecutionSite.SystemProcedureExecutionContext) {
            return param;
        }

        // hack to fixup varbinary support for statement procs
        if (m_paramTypes[paramTypeIndex] == byte[].class) {
            m_paramTypeComponentType[paramTypeIndex] = byte.class;
            m_paramTypeIsArray[paramTypeIndex] = true;
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

        boolean slotIsArray = m_paramTypeIsArray[paramTypeIndex];
        if (slotIsArray != pclass.isArray())
            throw new Exception("Array / Scalar parameter mismatch");

        if (slotIsArray) {
            Class<?> pSubCls = pclass.getComponentType();
            Class<?> sSubCls = m_paramTypeComponentType[paramTypeIndex];
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
                    java.sql.Timestamp sqlTS = java.sql.Timestamp.valueOf((String) param);
                    long timeInMicroSecs = sqlTS.getTime() * 1000;
                    timeInMicroSecs += sqlTS.getNanos() / 1000;
                    return new TimestampType(timeInMicroSecs);
                }
                // ignore errors if it's not the right format
                catch (IllegalArgumentException e) {}
            }
        }
        if (slot == BigDecimal.class) {
            if ((pclass == Long.class) || (pclass == Integer.class) ||
                (pclass == Short.class) || (pclass == Byte.class)) {
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
     * Thrown from a stored procedure to indicate to VoltDB
     * that the procedure should be aborted and rolled back.
     */
    public static class VoltAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;
        private String message = "No message specified.";

        /**
         * Constructs a new AbortException
         */
        public VoltAbortException() {}

        /**
         * Constructs a new AbortException from an existing <code>Throwable</code>.
         */
        public VoltAbortException(Throwable t) {
            super(t);
        }

        /**
         * Constructs a new AbortException with the specified detail message.
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
                              String tableName, VoltTable data)
    throws VoltAbortException
    {
        if (data == null || data.getRowCount() == 0) {
            return;
        }
        try {
            m_site.loadTable(m_currentTxnState.txnId,
                             clusterName, databaseName,
                             tableName, data);
        }
        catch (EEException e) {
            throw new VoltAbortException("Failed to load table: " + tableName);
        }
    }

    /**
     * Get a Java RNG seeded with the current transaction id. This will ensure that
     * two procedures for the same transaction, but running on different replicas,
     * can generate an identical stream of random numbers. This is required to endure
     * procedures have deterministic behavior. The RNG is memoized so you can invoke this
     * multiple times within a single procedure.
     *
     * @return A deterministically-seeded java.util.Random instance.
     */
    public Random getSeededRandomNumberGenerator() {
        // this value is memoized here and reset at the beginning of call(...).
        if (m_cachedRNG == null) {
            m_cachedRNG = new Random(m_currentTxnState.txnId);
        }
        return m_cachedRNG;
    }

    /**
     * Get the time that this procedure was accepted into the VoltDB cluster. This is the
     * effective, but not always actual, moment in time this procedure executes. Use this
     * method to get the current time instead of non-deterministic methods. Note that the
     * value will not be unique across transactions as it is only millisecond granularity.
     *
     * @return A java.util.Date instance with deterministic time for all replicas using
     * UTC (Universal Coordinated Time is like GMT).
     */
    public Date getTransactionTime() {
        long ts = TransactionIdManager.getTimestampFromTransactionId(m_currentTxnState.txnId);
        return new Date(ts);
    }

    /*
     * Commented this out and nothing broke? It's cluttering up the javadoc AW 9/2/11
     */
//    public void checkExpectation(Expectation expectation, VoltTable table) {
//        Expectation.check(m_procedureName, "NO STMT", 0, expectation, table);
//    }

    /**
     * Queue the SQL {@link org.voltdb.SQLStmt statement} for execution with the specified argument list,
     * and an Expectation describing the expected results. If the Expectation is not met then VoltAbortException
     * will be thrown with a description of the expecation that was not met. This exception must not be
     * caught from within the procedure.
     *
     * @param stmt {@link org.voltdb.SQLStmt Statement} to queue for execution.
     * @param expectation Expectation describing the expected result of executing this SQL statement.
     * @param args List of arguments to be bound as parameters for the {@link org.voltdb.SQLStmt statement}
     * @see <a href="#allowable_params">List of allowable parameter types</a>
     */
    public void voltQueueSQL(final SQLStmt stmt, Expectation expectation, Object... args) {
        voltQueueSQL(stmt, args);

        if (!m_isNative) {
            VoltTable table = m_queryResults.get(m_queryResults.size() - 1);
            Expectation.check(m_procedureName, stmt.getText(), m_queryResults.size() - 1, expectation, table);
            return;
        }

        m_batchQueryExpectations[m_batchQueryStmtIndex - 1] = expectation;
    }

    /**
     * Queue the SQL {@link org.voltdb.SQLStmt statement} for execution with the specified argument list.
     *
     * @param stmt {@link org.voltdb.SQLStmt Statement} to queue for execution.
     * @param args List of arguments to be bound as parameters for the {@link org.voltdb.SQLStmt statement}
     * @see <a href="#allowable_params">List of allowable parameter types</a>
     */
    public void voltQueueSQL(final SQLStmt stmt, Object... args) {
        if (!m_isNative) {
            //HSQLProcedureWrapper does nothing smart. it just implements this interface with runStatement()
            VoltTable table = m_hsql.runSQLWithSubstitutions(stmt, args);
            m_queryResults.add(table);
            return;
        }

        if (m_batchQueryStmtIndex == m_batchQueryStmts.length) {
            throw new RuntimeException("Procedure attempted to queue more than " + m_batchQueryStmts.length +
                    "statements in a single batch.\n  You may use multiple batches of up to 1000 statements," +
                    "each,\n  but you may also want to consider dividing this work into multiple procedures.");
        } else {
            m_batchQueryStmts[m_batchQueryStmtIndex] = stmt;
            m_batchQueryArgs[m_batchQueryStmtIndex] = args;
            m_batchQueryExpectations[m_batchQueryStmtIndex] = null; // may be set later
            ++m_batchQueryStmtIndex;
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
        if (!m_isNative) {
            VoltTable[] batch_results = m_queryResults.toArray(new VoltTable[m_queryResults.size()]);
            m_queryResults.clear();
            return batch_results;
        }

        VoltTable[] retval = null;

        try {
            retval = executeQueriesInABatch(
                    m_batchQueryStmtIndex, m_batchQueryStmts, m_batchQueryArgs, isFinalSQL);

            // verify expectations, noop if expectation is null
            for (int i = 0; i < retval.length; ++i) {
                Expectation.check(m_procedureName, m_batchQueryStmts[i].getText(),
                        i, m_batchQueryExpectations[i], retval[i]);
            }
        }
        finally {
            m_batchQueryStmtIndex = 0;
        }

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
                m_fragmentIds[fragmentIdIndex++] = fragGUIDs[ii];
                m_parameterSets[parameterSetIndex++] = params;
            }
        }

        if (slowPath) {
            /*
             * No fancy footwork if there is only one statement
             */
            if (batchSize == 1) {
                return slowPath(batchSize, batchStmts, batchArgs, finalTask);
            }

            /*
             * Determine if reads and writes are mixed. Can't mix reads and writes
             * because the order of execution is wrong when replicated tables are involved
             * due to ENG-1232
             */
            boolean hasRead = false;
            boolean hasWrite = false;
            for (int i = 0; i < batchSize; ++i) {
                final SQLStmt stmt = batchStmts[i];
                if (stmt.catStmt.getReadonly()) {
                    hasRead = true;
                } else {
                    hasWrite = true;
                }
            }
            /*
             * If they are all reads or all writes then we can use the batching slow path
             * Otherwise the order of execution will be interleaved incorrectly so we have to do
             * each statement individually.
             */
            if ((hasRead && !hasWrite) || (!hasRead && hasWrite)) {
                return slowPath(batchSize, batchStmts, batchArgs, finalTask);
            } else {
                return executeQueriesInIndividualBatches(batchSize, batchStmts, batchArgs, finalTask);
            }
        }

        VoltTable[] results = null;
        results = m_site.executeQueryPlanFragmentsAndGetResults(
            m_fragmentIds,
            fragmentIdIndex,
            m_parameterSets,
            parameterSetIndex,
            m_currentTxnState.txnId,
            m_catProc.getReadonly());
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
                args[ii] = VoltType.NULL_STRING_OR_VARBINARY;
            else if (type == VoltType.VARBINARY)
                args[ii] = VoltType.NULL_STRING_OR_VARBINARY;
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
            super(m_site.getCorrespondingSiteId() + " " + m_catProc.getClassname(),
                  m_site.getCorrespondingSiteId(), false);
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
                final long delta = endTime - m_currentStartTime;
                if (delta < 0)
                {
                    if (Math.abs(delta) > 1000000000)
                    {
                        log.info("Procedure: " + m_catProc.getTypeName() +
                                 " recorded a negative execution time larger than one second: " +
                                 delta);
                    }
                }
                else
                {
                    m_totalTimedExecutionTime += delta;
                    m_timedInvocations++;
                    m_minExecutionTime = Math.min( delta, m_minExecutionTime);
                    m_maxExecutionTime = Math.max( delta, m_maxExecutionTime);
                    m_lastMinExecutionTime = Math.min( delta, m_lastMinExecutionTime);
                    m_lastMaxExecutionTime = Math.max( delta, m_lastMaxExecutionTime);
                }
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
            rowValues[columnNameToIndex.get("PROCEDURE")] = m_catProc.getClassname();
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
            return m_catProc.getTypeName();
        }
    }

    /**
     * Set the status code that will be returned to the client. This is not the same as the status
     * code returned by the server. If a procedure sets the status code and then rolls back or causes an error
     * the status code will still be propagated back to the client so it is always necessary to check
     * the server status code first.
     * @param statusCode
     */
    public void setAppStatusCode(byte statusCode) {
        m_statusCode = statusCode;
    }

    /**
     * Set the string that will be turned to the client. This is not the same as teh status string
     * returned by the server. If a procedure sets the status string and then rolls back or causes an error
     * the status string will still be propagated back to the client so it is always necessary to check
     * the server status code first.
     * @param statusString
     */
    public void setAppStatusString(String statusString) {
        m_statusString = statusString;
    }

    private VoltTable[] slowPath(int batchSize, SQLStmt[] batchStmts, Object[][] batchArgs, boolean finalTask) {
        VoltTable[] results = new VoltTable[batchSize];

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
        boolean localFragsAreNonTransactional = false;

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
            FastSerializer fs = new FastSerializer();
            try {
                fs.writeObject(paramSet);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing parameters for SQL statement: " +
                                           stmt.getText() + " with params: " +
                                           paramSet.toJSONString(), e);
            }
            ByteBuffer params = fs.getBuffer();
            assert(params != null);

            // populate the actual lists of fragments and params
            int numFrags = stmt.catStmt.getFragments().size();
            assert(numFrags > 0);
            assert(numFrags <= 2);

            /*
             * This numfrags == 1 code is for routing multi-partition reads of a
             * replicated table to the local site. This was a broken performance optimization.
             * see https://issues.voltdb.com/browse/ENG-1232
             * The problem is that the fragments for the replicated read are not correctly interleaved with the
             * distributed writes to the replicated table that might be in the same batch of SQL statements.
             * We do end up doing the replicated read locally but we break up the batches in the face of mixed
             * reads and writes
             */
            if (numFrags == 1) {
                for (PlanFragment frag : stmt.catStmt.getFragments()) {
                    assert(frag != null);
                    assert(frag.getHasdependencies() == false);

                    localFragIds[i] = CatalogUtil.getUniqueIdForFragment(frag);
                    localParams[i] = params;

                    // if any frag is transactional, update this check
                    if (frag.getNontransactional() == true)
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
                        if (frag.getNontransactional() == true)
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
                                                  m_currentTxnState.isReadOnly(),
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
                                                        m_currentTxnState.isReadOnly(),
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
                long newVal = results[i].asScalarLong() / m_numberOfPartitions;
                results[i] = new VoltTable(new VoltTable.ColumnInfo("modified_tuples", VoltType.BIGINT));
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
        boolean expected_failure = true;
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

    private ClientResponseImpl getErrorResponse(byte status, String msg, SerializableException e) {

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
}
