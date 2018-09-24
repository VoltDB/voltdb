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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.planner.StatementPartitioning;

/**
 * A batch of non-DDL SQL queries.
 * This class is a decorator for a {@link SqlBatch}, adding
 * some contextual information for planning.
 * @since 8.4
 * @author Yiqun Zhang
 */
public final class NonDdlBatch extends SqlBatch {

    /**
     * The {@link SqlBatch} to be decorated.
     */
    private final SqlBatch m_base;

    /**
     * Record the catalog version that the queries are planned against.
     * Catch races vs. updateApplicationCatalog.
     */
    public final CatalogContext m_catalogContext;

    /**
     * The user partitioning keys and the infer partitioning value affects the final
     * partitioning scheme. They should only be changed via provided APIs.
     */
    private List<Object> m_userPartitionKeys;

    private boolean m_inferPartitioning;

    /**
     * Take advantage of the planner optimization for inferring single partition work
     * when the batch has one statement.
     */
    private StatementPartitioning m_partitioning;

    /**
     * Build a non-DDL SQL batch from a basic SQL batch, adding default
     * contextual information.
     * @param batch the {@link SqlBatch} that this is built from.
     * @throws IllegalArgumentException if the given batch is not a non-DDL batch.
     */
    public NonDdlBatch(SqlBatch batch) {
        if (batch.isDDLBatch()) {
            throw new IllegalArgumentException("Cannot create a non-DDL batch from a batch of DDL statements.");
        }
        m_base = batch;
        m_catalogContext = VoltDB.instance().getCatalogContext();
        m_userPartitionKeys = null;
        setInferPartitioning(true);
    }

    /**
     * Set if the planner should infer the partitioning for this batch. </br>
     * Note: The change takes effect only when the batch has one {@link SqlTask}.
     * @param value the target value.
     * @return this {@link NonDdlBatch} instance itself.
     */
    public NonDdlBatch setInferPartitioning(boolean value) {
        m_inferPartitioning = m_base.getTaskCount() == 1 && value;
        updateStatementPartitioning();
        return this;
    }

    /**
     * Tell if the planner should infer partitioning for this batch.
     * @return true if the planner will infer partitioning for this batch.
     */
    public boolean inferPartitioning() {
        return m_inferPartitioning;
    }

    /**
     * Supply a user partitioning key.
     * Users can specify as many partitioning keys as they want, but only two
     * partition keys at most are currently supported.
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
        }
        else if (m_userPartitionKeys == null) {
            m_partitioning = StatementPartitioning.forceMP();
        }
        else {
            m_partitioning = StatementPartitioning.forceSP();
        }
    }

    /**
     * Get the partitioning scheme.
     * @return the partitioning scheme.
     */
    public StatementPartitioning getPartitioning() {
        return (StatementPartitioning) m_partitioning.clone();
    }

    /**
     * Get an array of user partitioning keys.
     * @return an array of user partitioning keys.
     */
    public Object[] getUserPartitioningKeys() {
        if (m_userPartitionKeys == null || m_userPartitionKeys.size() == 0) {
            return null;
        }
        else {
            return m_userPartitionKeys.toArray();
        }
    }

    @Override
    public Iterator<SqlTask> iterator() {
        return m_base.iterator();
    }

    @Override
    public boolean isDDLBatch() {
        return m_base.isDDLBatch();
    }

    @Override
    public Object[] getUserParameters() {
        return m_base.getUserParameters();
    }

    @Override
    public int getTaskCount() {
        return m_base.getTaskCount();
    }
}
