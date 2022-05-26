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

package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.plannodes.*;

/**
 * This is the type of plan nodes.
 *
 * Note that this implements PlanMatcher.  A PlanNodeType object
 * matches an AbstractPlanNode if the AbstractPlanNode's type is
 * the PlanNodeType object.  This is used in testing plans.
 */
public enum PlanNodeType implements PlanMatcher {
    INVALID         (0, null), // for parsing...

    //
    // Scan Nodes
    //
    SEQSCAN          (10, SeqScanPlanNode.class),
    INDEXSCAN        (11, IndexScanPlanNode.class),
    INDEXCOUNT       (12, IndexCountPlanNode.class),
    TABLECOUNT       (13, TableCountPlanNode.class),
    MATERIALIZEDSCAN (14, MaterializedScanPlanNode.class),
    TUPLESCAN        (15, TupleScanPlanNode.class),

    //
    // Join Nodes
    //
    NESTLOOP        (20, NestLoopPlanNode.class),
    NESTLOOPINDEX   (21, NestLoopIndexPlanNode.class),
    MERGEJOIN       (22, MergeJoinPlanNode.class),

    //
    // Operator Nodes
    //
    UPDATE          (30, UpdatePlanNode.class),
    INSERT          (31, InsertPlanNode.class),
    DELETE          (32, DeletePlanNode.class),
    // UPSERT       (33, UpsertPlanNode.class),// UNUSED: Upserts are inserts.
    SWAPTABLES      (34, SwapTablesPlanNode.class),
    MIGRATE          (35, MigratePlanNode.class),

    //
    // Communication Nodes
    //
    SEND            (40, SendPlanNode.class),
    RECEIVE         (41, ReceivePlanNode.class),
    MERGERECEIVE    (42, MergeReceivePlanNode.class),

    //
    // Misc Nodes
    //
    AGGREGATE       (50, AggregatePlanNode.class),
    HASHAGGREGATE   (51, HashAggregatePlanNode.class),
    UNION           (52, UnionPlanNode.class),
    ORDERBY         (53, OrderByPlanNode.class),
    PROJECTION      (54, ProjectionPlanNode.class),
    MATERIALIZE     (55, MaterializePlanNode.class),
    LIMIT           (56, LimitPlanNode.class),
    PARTIALAGGREGATE(57, PartialAggregatePlanNode.class),
    WINDOWFUNCTION  (58, WindowFunctionPlanNode.class),
    COMMONTABLE     (59, CommonTablePlanNode.class),
    ;

    private final int val;
    private final Class<? extends AbstractPlanNode> planNodeClass;

    private PlanNodeType(int val, Class<? extends AbstractPlanNode> planNodeClass) {
        this.val = val;
        this.planNodeClass = planNodeClass;
    }

    public int getValue() {
        return val;
    }

    public Class<? extends AbstractPlanNode> getPlanNodeClass() {
        return planNodeClass;
    }

    protected static final Map<Integer, PlanNodeType> idx_lookup = new HashMap<>();
    protected static final Map<String, PlanNodeType> name_lookup = new HashMap<>();
    static {
        for (PlanNodeType vt : EnumSet.allOf(PlanNodeType.class)) {
            PlanNodeType.idx_lookup.put(vt.val, vt);
            PlanNodeType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, PlanNodeType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, PlanNodeType> getNameMap() {
        return name_lookup;
    }

    public static PlanNodeType get(Integer idx) {
        PlanNodeType ret = PlanNodeType.idx_lookup.get(idx);
        return (ret == null ? PlanNodeType.INVALID : ret);
    }

    public static PlanNodeType get(String name) {
        PlanNodeType ret = PlanNodeType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? PlanNodeType.INVALID : ret);
    }

    @Override
    public String match(AbstractPlanNode node) {
        if (node.getPlanNodeType() == this) {
            return null;
        }
        return "Expected a plan node of type " + this
                + " in node " + node.getPlanNodeId() + ".";
    }
}
