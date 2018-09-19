/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.sysprocs.AdHocNTBase.AdHocPlanningException;

/**
 * A batch of non-DDL SQL queries.
 * A non-DDL batch contains more contextual information for query planning.
 * @since 8.4
 * @author Yiqun Zhang
 */
public final class NonDdlSqlBatch extends SqlBatch {

    boolean m_inferPartitioning;
    List<Object> m_userPartitionKeys;
    /**
     * Take advantage of the planner optimization for inferring single partition work
     * when the batch has one statement.
     */
    StatementPartitioning m_partitioning;
    ExplainMode m_explainMode;
    boolean m_isLargeQuery;
    boolean m_isSwapTables;
    final CatalogContext m_context;

    static final String s_adHocErrorResponseMessage =
            "The @AdHoc stored procedure when called with more than one parameter "
                    + "must be passed a single parameterized SQL statement as its first parameter. "
                    + "Pass each parameterized SQL statement to a separate callProcedure invocation.";

    /**
     * Build a non-DDL SQL batch from a general SQL batch, adding default
     * contextual information.
     * @param batch The {@link SqlBatch} that this is built from.
     */
    public NonDdlSqlBatch(SqlBatch batch) {
        super(batch);

        if (m_userParams != null && m_userParams.length > 0 && m_tasks.size() != 1) {
            throw new RuntimeException(s_adHocErrorResponseMessage);
        }

        m_inferPartitioning = true;
        m_userPartitionKeys = null;
        m_explainMode = ExplainMode.NONE;
        m_isLargeQuery = false;
        m_isSwapTables = false;
        // Record the catalog version that the queries are planned against
        // Catch races vs. updateApplicationCatalog.
        m_context = VoltDB.instance().getCatalogContext();
        setInferPartitioning(true);
    }

    /**
     * Compile this batch.
     * @return a {@link AdHocPlannedStmtBatch}.
     * @throws AdHocPlanningException if the planning went wrong. </br>
     * The {@code AdHocPlanningException} will be aggregated with all exceptions
     * collected from planning the entire batch.
     */
    public AdHocPlannedStmtBatch compile() throws AdHocPlanningException {
        List<String> errorMsgs = new ArrayList<>();
        List<AdHocPlannedStatement> plannedStmts = new ArrayList<>();
        int partitionParamIndex = -1;
        VoltType partitionParamType = null;
        Object partitionParamValue = null;

        for (final SqlTask task : m_tasks) {
            try {
                AdHocPlannedStatement result = compileTask(task);
                // The planning tool may have optimized for the single partition case
                // and generated a partition parameter.
                if (m_inferPartitioning) {
                    partitionParamIndex = result.getPartitioningParameterIndex();
                    partitionParamType = result.getPartitioningParameterType();
                    partitionParamValue = result.getPartitioningParameterValue();
                }
                plannedStmts.add(result);
            }
            catch (AdHocPlanningException e) {
                errorMsgs.add(e.getMessage());
            }
        }

        if ( ! errorMsgs.isEmpty()) {
            String errorSummary = StringUtils.join(errorMsgs, "\n");
            throw new AdHocPlanningException(errorSummary);
        }

        return new AdHocPlannedStmtBatch(m_userParams,
                                         plannedStmts,
                                         partitionParamIndex,
                                         partitionParamType,
                                         partitionParamValue,
                                         getUserPartitioningKeys());
    }

    /**
     * Compile a batch of one or more SQL statements into a set of plans.
     * Parameters are valid iff there is exactly one DML/DQL statement.
     */
    private AdHocPlannedStatement compileTask(SqlTask task) throws AdHocPlanningException {
        // TRAIL [Calcite:3] compileAdHocSQLThroughCalcite
        assert(m_context.m_ptool != null);
        assert(task != null);
        final PlannerTool ptool = m_context.m_ptool;

        try {
            return ptool.planSqlWithCalcite(task,
                                            m_partitioning,
                                            m_explainMode != ExplainMode.NONE,
                                            getUserParameters(),
                                            m_isSwapTables,
                                            m_isLargeQuery);
        } catch (Exception e) {
            throw new AdHocPlanningException(e.getMessage());
        } catch (StackOverflowError error) {
            // Overly long predicate expressions can cause a
            // StackOverflowError in various code paths that may be
            // covered by different StackOverflowError/Error/Throwable
            // catch blocks. The factors that determine which code path
            // and catch block get activated appears to be platform
            // sensitive for reasons we do not entirely understand.
            // To generate a deterministic error message regardless of
            // these factors, purposely defer StackOverflowError handling
            // for as long as possible, so that it can be handled
            // consistently by a minimum number of high level callers like
            // this one.
            // This eliminates the need to synchronize error message text
            // in multiple catch blocks, which becomes a problem when some
            // catch blocks lead to re-wrapping of exceptions which tends
            // to adorn the final error text in ways that are hard to track
            // and replicate.
            // Deferring StackOverflowError handling MAY mean ADDING
            // explicit StackOverflowError catch blocks that re-throw
            // the error to bypass more generic catch blocks
            // for Error or Throwable on the same try block.
            throw new AdHocPlanningException("Encountered stack overflow error. " +
                    "Try reducing the number of predicate expressions in the query.");
        } catch (AssertionError ae) {
            String msg = "An unexpected internal error occurred when planning a statement issued via @AdHoc.  "
                    + "Please contact VoltDB at support@voltdb.com with your log files.";
            StringWriter stringWriter = new StringWriter();
            PrintWriter writer = new PrintWriter(stringWriter);
            ae.printStackTrace(writer);
            String stackTrace = stringWriter.toString();
            s_adHocLog.error(msg + "\n" + stackTrace);
            throw new AdHocPlanningException(msg);
        }
    }

    /**
     * Set if this batch should run in the large query mode.
     * @param value
     * @return this {@link NonDdlSqlBatch} instance itself.
     */
    public NonDdlSqlBatch setLargeQuery(boolean value) {
        m_isLargeQuery = value;
        return this;
    }

    /**
     * Set if the planner should infer the partitioning for this batch. </br>
     * Note: The change takes effect only when the batch has one {@link SqlTask}.
     * @param value
     * @return this {@link NonDdlSqlBatch} instance itself.
     */
    public NonDdlSqlBatch setInferPartitioning(boolean value) {
        m_inferPartitioning = m_tasks.size() == 1 && value;
        updateStatementPartitioning();
        return this;
    }

    /**
     * Supply a user partitioning key.
     * Users can specify as many partitioning keys as they want, but only two
     * partition keys at most are currently supported.
     * @param key a user partitioning key.
     * @return this {@link NonDdlSqlBatch} instance itself.
     */
    public NonDdlSqlBatch addUserPartitioningKey(Object key) {
        if (m_userPartitionKeys == null) {
            m_userPartitionKeys = new ArrayList<>();
        }
        m_userPartitionKeys.add(key);
        updateStatementPartitioning();
        return this;
    }

    /**
     * Internal method to update the statement partitioning scheme based on the
     * latest value of {@code m_inferPartitioning} and {@code m_userPartitionKeys}.
     */
    private void updateStatementPartitioning() {
        if (m_inferPartitioning) {
            m_partitioning = StatementPartitioning.inferPartitioning();
        }
        else if (m_userPartitionKeys == null) {
            m_partitioning = StatementPartitioning.forceMP();
        }
        else {
            m_partitioning = StatementPartitioning.forceSP();
        }
    }

    /**
     * Get the {@link CatalogContext} used by this batch when its tasks
     * were compiled.
     * @return the {@code CatalogContext}.
     */
    public CatalogContext getCatalogContext() {
        return m_context;
    }

    /**
     * Get the explain mode of this batch.
     * @return the {@link ExplainMode}.
     */
    public ExplainMode getExplainMode() {
        return m_explainMode;
    }

    /**
     * @return true if the batch is a swap table statement.
     */
    public boolean isSwapTables() {
        return m_isSwapTables;
    }

    /**
     * Get an array of user partitioning keys.
     * @return an array of user partitioning keys.
     */
    private Object[] getUserPartitioningKeys() {
        if (m_userPartitionKeys == null || m_userPartitionKeys.size() == 0) {
            return null;
        }
        else {
            return m_userPartitionKeys.toArray();
        }
    }
}
