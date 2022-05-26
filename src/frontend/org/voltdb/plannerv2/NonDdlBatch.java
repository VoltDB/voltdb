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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.VoltTypeException;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannerv2.guards.PlannerFallbackException;

/**
 * DQL/DML query batch.
 *
 * This is a decorator for {@link SqlBatch},
 * adding some additional context information for planning. It cannot be used alone.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public final class NonDdlBatch extends AbstractSqlBatchDecorator {

    /**
     * The user partitioning keys and the infer partitioning flag have a correlated
     * impact on the the final {@link StatementPartitioning} result.
     * They should only be changed via provided APIs.
     *
     * @see #updateStatementPartitioning()
     */
    private List<Object> m_userPartitionKeys;

    /**
     * Whether the planner should try to infer partitioning during planning.
     *
     * @see #updateStatementPartitioning()
     */
    private boolean m_inferPartitioning;

    /**
     * Take advantage of the planner optimization for inferring single partition work
     * when the batch has one statement.
     */
    private StatementPartitioning m_partitioning;

    /**
     * NonDdlBatch is either regular planning, ad hoc explain, default proc explain or ad hoc explain in JSON format.
     */
    private ExplainMode m_explainMode;

    /**
     * Decorate a basic query batch so it becomes a DQL/DML batch, adding the default
     * context information.
     *
     * @param batchToDecorate the {link SqlBatch} this decorator is decorating.
     * @throws IllegalArgumentException if the given batch is in fact a DDL batch.
     */
    NonDdlBatch(SqlBatch batchToDecorate, ExplainMode explainMode) {
        super(batchToDecorate);
        if (batchToDecorate.isDDLBatch()) {
            throw new IllegalArgumentException("Cannot create a non-DDL batch from a batch of DDL statements.");
        }
        m_userPartitionKeys = null;
        m_explainMode = explainMode;
        setInferPartitioning(true);
    }

    @Override public CompletableFuture<ClientResponse> execute()
            throws PlanningErrorException, PlannerFallbackException {
        // TRAIL [Calcite-AdHoc-DQL/DML:1] NonDDLBatch.execute()

        NonDdlBatchPlanner planner = new NonDdlBatchPlanner(this);
        AdHocPlannedStmtBatch plannedStmtBatch = planner.plan();

        // Note - ethan - 12/28/2018:
        // No swap tables now.
        try {
            switch (m_explainMode) {
                case NONE:
                    return getContext().createAdHocTransaction(plannedStmtBatch);
                case EXPLAIN_ADHOC:
                    return getContext().processExplainPlannedStmtBatch(plannedStmtBatch);
                case EXPLAIN_DEFAULT_PROC:
                case EXPLAIN_JSON:
                    throw new PlannerFallbackException("Explain mode: " + m_explainMode.name() + "Not implemented in Calcite.");
                default:
                    throw new PlanningErrorException("invalid explain mode: " + m_explainMode.name());
            }
        } catch (VoltTypeException vte) {
            String msg = "Unable to execute AdHoc SQL statement(s): " + vte.getMessage();
            throw new PlanningErrorException(msg);
        }
    }

    /**
     * Set if the planner should infer the partitioning for this batch. </br>
     * Note: The change takes effect only when the batch has one {@link SqlTask}.
     *
     * @param value the target value.
     * @return this {@link NonDdlBatch} instance itself.
     */
    public NonDdlBatch setInferPartitioning(boolean value) {
        value = m_batchToDecorate.getTaskCount() == 1 && value;
        if (m_inferPartitioning != value) {
            m_inferPartitioning = value;
            updateStatementPartitioning();
        }
        return this;
    }

    /**
     * @return true if the planner will infer partitioning for this batch.
     */
    public boolean inferPartitioning() {
        return m_inferPartitioning;
    }

    /**
     * Supply a user partitioning key.
     * Users can specify as many partitioning keys as they want, but only two
     * partition keys at most are currently supported.
     *
     * @param key a user partitioning key.
     * @return this {@link NonDdlBatch} instance itself.
     */
    public NonDdlBatch addUserPartitioningKey(Object key) {
        if (key == null) {
            return this;
        }
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
        } else if (m_userPartitionKeys == null) {
            m_partitioning = StatementPartitioning.forceMP();
        } else {
            m_partitioning = StatementPartitioning.forceSP();
        }
    }

    /**
     * @return the partitioning scheme.
     */
    public StatementPartitioning getPartitioning() {
        return (StatementPartitioning) m_partitioning.clone();
    }

    /**
     * @return an array of user partitioning keys.
     */
    public Object[] getUserPartitioningKeys() {
        if (m_userPartitionKeys == null || m_userPartitionKeys.size() == 0) {
            return null;
        } else {
            return m_userPartitionKeys.toArray();
        }
    }
}
