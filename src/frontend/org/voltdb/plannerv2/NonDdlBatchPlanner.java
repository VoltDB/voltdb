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

package org.voltdb.plannerv2;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.utils.VoltTrace;

/**
 * A planner for DQL/DML batches.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class NonDdlBatchPlanner {

    private final NonDdlBatch m_batch;

    /**
     * Record the catalog version that the queries are planned against.
     * Catch races versus UpdateApplicationCatalog.
     */
    private final CatalogContext m_catalogContext;

    NonDdlBatchPlanner(NonDdlBatch batch) {
        m_batch = batch;
        m_catalogContext = VoltDB.instance().getCatalogContext();
    }

    /**
     * Plan this DQL/DML batch.
     *
     * @return a {@link AdHocPlannedStmtBatch}.
     * @throws PlanningErrorException if the planning went wrong. </br>
     *         The {@code PlanningErrorException} will be aggregated with all exceptions
     *         collected from planning the entire batch.
     * @throws PlannerFallbackException if we need to switch back to the legacy parser/planner.
     */
    public AdHocPlannedStmtBatch plan() throws PlanningErrorException, PlannerFallbackException {
        // TRAIL [Calcite-AdHoc-DQL/DML:2] NonDDLBatchPlanner.plan()

        List<String> errorMsgs = new ArrayList<>();
        List<AdHocPlannedStatement> plannedStmts = new ArrayList<>();
        int partitionParamIndex = -1;
        VoltType partitionParamType = null;
        Object partitionParamValue = null;

        for (final SqlTask task : m_batch) {
            try {
                final AdHocPlannedStatement result = planTask(task);
                if (m_batch.inferPartitioning()) {
                    // The planning tool may have optimized for the single partition case
                    // and generated a partition parameter.
                    partitionParamIndex = result.getPartitioningParameterIndex();
                    partitionParamType = result.getPartitioningParameterType();
                    partitionParamValue = result.getPartitioningParameterValue();
                }
                plannedStmts.add(result);
            } catch (PlanningErrorException e) {
                Throwable cause = e.getCause();
                String errorMsg = null;
                if (cause != null) {
                    errorMsg = cause.getMessage();
                }
                if (errorMsg == null) {
                    errorMsg = e.getMessage();
                }
                errorMsgs.add(errorMsg);
            }
        }

        if ( ! errorMsgs.isEmpty()) {
            // Aggregate all the exceptions and re-throw.
            String errorSummary = StringUtils.join(errorMsgs, "\n");
            throw new PlanningErrorException(errorSummary);
        }

        final AdHocPlannedStmtBatch plannedStmtBatch = new AdHocPlannedStmtBatch(
                m_batch.getUserParameters(), plannedStmts, partitionParamIndex,
                partitionParamType, partitionParamValue, m_batch.getUserPartitioningKeys());
        if (m_batch.getContext().getLogger().isDebugEnabled()) {
            m_batch.getContext().logBatch(m_catalogContext, plannedStmtBatch, m_batch.getUserParameters());
        }
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endAsync("planadhoc", m_batch.getContext().getClientHandle()));
        }
        return plannedStmtBatch;
    }

    /**
     * Plan a task into a query plan.
     *
     * @param task SqlTask for one SQL statement
     * @return planned statement
     * @throws PlanningErrorException
     * @throws PlannerFallbackException if we need to switch to the legacy parser/planner.
     */
    private AdHocPlannedStatement planTask(SqlTask task)
            throws PlanningErrorException, PlannerFallbackException {
        // TRAIL [Calcite-AdHoc-DQL/DML:3] NonDdlBatchCompiler.compileTask()

        final PlannerTool ptool = m_catalogContext.m_ptool;
        assert(ptool != null);
        try {
            return ptool.planSqlCalcite(task);
        } catch (PlannerFallbackException ex) { // Let go the PlannerFallbackException so we can fall back to the legacy planner.
            throw ex;
        } catch (Exception ex) {
            Throwable cause = ex.getCause();
            while(cause != null && cause.getCause() != null) {
                cause = cause.getCause();
            }
            throw new PlanningErrorException(ex.getMessage(), cause);
        } catch (StackOverflowError error) {
            // NOTE: This is from AdHocNTBase.compileAdHocSQL()
            // We need it, as long as Calcite could fall back.

            /*
             * Overly long predicate expressions can cause a StackOverflowError in various
             * code paths that may be covered by different StackOverflowError/Error/Throwable
             * catch blocks. The factors that determine which code path and catch block
             * get activated appears to be platform sensitive for reasons we do not fully
             * understand.
             *
             * To generate a deterministic error message regardless of these factors,
             * purposely defer StackOverflowError handling for as long as possible, so that
             * it can be handled consistently by a minimum number of high level callers like
             * this one.
             *
             * This eliminates the need to synchronize error message text in multiple catch
             * blocks, which becomes a problem when some catch blocks lead to re-wrapping of
             * exceptions which tends to adorn the final error text in ways that are hard to
             * track and replicate.
             *
             * Deferring StackOverflowError handling MAY mean ADDING explicit StackOverflowError
             * catch blocks that re-throw the error to bypass more generic catch blocks for
             * Error or Throwable on the same try block.
             */
            throw new PlanningErrorException("Encountered stack overflow error. " +
                    "Try reducing the number of predicate expressions in the query.");
        } catch (AssertionError ae) {
            String msg = "An unexpected internal error occurred when planning a statement issued via @AdHoc. "
                    + "Please contact VoltDB at support@voltactivedata.com with your log files.";
            StringWriter stringWriter = new StringWriter();
            PrintWriter writer = new PrintWriter(stringWriter);
            ae.printStackTrace(writer);
            String stackTrace = stringWriter.toString();
            m_batch.getContext().getLogger().error(msg + "\n" + stackTrace);
            throw new PlanningErrorException(msg);
        }
    }
}
