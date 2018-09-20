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

package org.voltdb.newplanner;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.sysprocs.AdHocNTBase.AdHocPlanningException;

/**
 * A compiler that compiles a non-DDL batch.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class NonDdlBatchCompiler {

    static final VoltLogger s_adHocLog = new VoltLogger("ADHOC");
    private final NonDdlBatch m_batch;

    public NonDdlBatchCompiler(NonDdlBatch batch) {
        m_batch = batch;
    }

    /**
     * Compile this non-DDL batch.
     * @return a {@link AdHocPlannedStmtBatch}.
     * @throws AdHocPlanningException if the planning went wrong. </br>
     * The {@code AdHocPlanningException} will be aggregated with all exceptions
     * collected from planning the entire batch.
     */
    public AdHocPlannedStmtBatch compile() throws AdHocPlanningException {
        // TRAIL [Calcite:3] NonDdlBatchCompiler.compile()
        List<String> errorMsgs = new ArrayList<>();
        List<AdHocPlannedStatement> plannedStmts = new ArrayList<>();
        int partitionParamIndex = -1;
        VoltType partitionParamType = null;
        Object partitionParamValue = null;

        for (final SqlTask task : m_batch) {
            try {
                AdHocPlannedStatement result = compileTask(task);
                // The planning tool may have optimized for the single partition case
                // and generated a partition parameter.
                if (m_batch.inferPartitioning()) {
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
            // Aggregate all the exceptions and re-throw.
            String errorSummary = StringUtils.join(errorMsgs, "\n");
            throw new AdHocPlanningException(errorSummary);
        }

        return new AdHocPlannedStmtBatch(m_batch.getUserParameters(),
                                         plannedStmts,
                                         partitionParamIndex,
                                         partitionParamType,
                                         partitionParamValue,
                                         m_batch.getUserPartitioningKeys());
    }

    /**
     * Compile a batch of one or more SQL statements into a set of plans.
     * Parameters are valid iff there is exactly one DML/DQL statement.
     */
    private AdHocPlannedStatement compileTask(SqlTask task) throws AdHocPlanningException {
        // TRAIL [Calcite:4] NonDdlBatchCompiler.compileTask()
        assert(m_batch.m_catalogContext.m_ptool != null);
        assert(task != null);
        final PlannerTool ptool = m_batch.m_catalogContext.m_ptool;

        try {
            return null;// ptool.planSqlWithCalcite(task, m_batch);
        } catch (Exception e) {
            throw new AdHocPlanningException(e.getMessage());
        } catch (StackOverflowError error) {
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
}
