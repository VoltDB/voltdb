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

package org.voltdb.plannerv2.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.voltdb.plannerv2.VoltTable;

import com.google.common.base.Preconditions;

/**
 * Relational expression representing a scan of a {@link VoltTable}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public abstract class AbstractVoltTableScan extends TableScan {

    protected final VoltTable m_voltTable;

    /**
     * Constructor for the subclasses.
     *
     * @param cluster    Cluster that this relational expression belongs to
     * @param traitSet   Trait set
     * @param table      The corresponding relational dataset in a {@link RelOptSchema}.
     * @param voltTable  VoltDB translatable table
     */
    protected AbstractVoltTableScan(RelOptCluster cluster,
                                      RelTraitSet traitSet,
                                      RelOptTable table,
                                      VoltTable voltTable) {
        super(cluster, traitSet, table);
        Preconditions.checkNotNull(voltTable, "VoltTable cannot be null.");
        this.m_voltTable = voltTable;
    }

    /**
     * @return the {@link VoltTable} this will scan.
     */
    public VoltTable getVoltTable() {
        return m_voltTable;
    }

    /*
     * Note - ethan - 12/29/2018 - about digest:
     * Before the digest is used, computeDigest() will always be called.
     * For this particular class, the digest may be something like
     *  "VoltLogicalTableScan.NONE.[](table=[public, T1])".
     * The table name will be included as part of the digest so there is no need to append the
     * table name manually. Even if you want to change the digest, you should override the explainTerms()
     * instead of overriding the computeDigest().
     */
}
