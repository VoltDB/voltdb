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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.voltdb.plannerv2.guards.AcceptAllSelect;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.plannerv2.metadata.VoltRelMetadataProvider;
import org.voltdb.plannerv2.rules.PlannerRules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/*
 * Some quick notes about "How Calcite Planner work".
 * reference https://www.slideshare.net/JordanHalterman/introduction-to-apache-calcite
 * steps:
 * 1. Optimize the logical plan (the SQL query can directly translate to a initial logical plan,
 *    then we optimize it to a better logical plan)
 * 2. Convert the logical plan into a physical plan (represents the physical execution stages)
 *
 * Common optimizations:
 * Prune unused fields, Merge projections, Convert sub-queries to joins, Reorder joins,
 * Push down projections, Push down filters
 *
 * Key Concepts:
 * # {@link org.apache.calcite.rel.RelNode} represents a relational expression
 * Sort, Join, Project, Filter, Scan...
 * e.g.:
 * select col1 as id, col2 as name from foo where col1=21;
 *
 * Project ( id = [$0], name = [$1] ) <-- expression
 * Filter (condition=[= ($0, 21)])   <-- children/input
 * TableScan (table = [foo])
 *
 * # {@link org.apache.calcite.rex.RexNode} represents a row-level expression:
 * = scalar expression
 * Projection fields, conditions
 * Input column reference  -->  RexInputRef
 * Literal                 -->  RexLiteral
 * Struct field access     -->  RexFieldAccess
 * Function call           -->  RexCall
 * Window expression       -->  RexOver
 *
 * # Traits
 * Defined by the {@link org.apache.calcite.plan.RelTrait} interface
 * Traits are used to validate plan output
 * {@link org.apache.calcite.plan.Convention}
 * {@link org.apache.calcite.rel.RelCollation}
 * {@link org.apache.calcite.rel.RelDistribution}
 *
 * ## Convention
 * Convention is a type of RelTrait, it is associated with a RelNode interface.
 * Conventions are used to represent a single data source,
 * describing how the expression passes data to its consuming relational expression
 * Inputs to a relational expression must be in the same convention.
 *
 * # Rules
 * Rules are used to modify query plans.
 * Defined by the {@link org.apache.calcite.plan.RelOptRule} interface
 *
 * Rules are matched to elements of a query plan using pattern matching
 * {@link org.apache.calcite.plan.RelOptRuleOperand}
 *
 * ## Converter
 * {@link org.apache.calcite.rel.convert.ConverterRule}
 * convert() is called for matched rules
 *
 * {@link org.apache.calcite.rel.convert.Converter}
 * By declaring itself to be a converter, a relational expression is telling the planner
 * about this equivalence, and the planner groups expressions which are logically equivalent
 * but have different physical traits into groups called RelSets.
 *
 * Q: why we need to put logically equivalent RelNode to a RelSet?
 * A: RelSet provides a level of indirection that allows Calcite to optimize queries.
 * If the input to a relational operator is an equivalence class, not a particular relational
 * expression, then Calcite has the freedom to choose the member of the equivalence class that
 * has the cheapest cost.
 *
 *
 * ## Transformer
 * onMatch() is called for matched rules
 * call.transformTo()
 *
 * # Planners
 * {@link org.apache.calcite.plan.volcano.VolcanoPlanner}
 * {@link org.apache.calcite.plan.hep.HepPlanner}
 *
 * # Program
 * {@link org.apache.calcite.tools.Program}
 *
 * Program that transforms a relational expression into another relational expression.
 * A planner is a sequence of programs, each of which is sometimes called a "phase".
 *
 * The most typical program is an invocation of the volcano planner with a particular RuleSet.
 */

/**
 * Implementation of {@link org.apache.calcite.tools.Planner}.
 * You can use this planner for multiple queries unless there is a catalog change.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class VoltPlanner implements Planner {

    private final VoltFrameworkConfig m_config;
    private final SqlValidator m_validator;
    private final RexBuilder m_rexBuilder;
    private final RelOptPlanner m_relPlanner;
    private final SqlToRelConverter m_sqlToRelConverter;
    private final RelBuilder m_relBuilder;

    // Internal states.
    private State m_state;
    private SqlNode m_validatedSqlNode;
    private RelRoot m_relRoot;

    /**
     * Build a {@link VoltPlanner}.
     *
     * @param schema the converted {@code SchemaPlus} from VoltDB catalog.
     */
    public VoltPlanner(SchemaPlus schema) {
        m_config = new VoltFrameworkConfig(schema);
        m_validator = new VoltSqlValidator(m_config);
        m_validator.setIdentifierExpansion(true);
        m_rexBuilder = new RexBuilder(m_config.getTypeFactory());
        m_relPlanner = new VolcanoPlanner(VoltRelOptCost.FACTORY, null);
        for (@SuppressWarnings("rawtypes") RelTraitDef def : m_config.getTraitDefs()) {
            m_relPlanner.addRelTraitDef(def);
        }
        // The RelTraitDefs need to be added to the planner before building the RelOptCluster.
        RelOptCluster cluster = RelOptCluster.create(m_relPlanner, m_rexBuilder);
        cluster.setMetadataProvider(new CachingRelMetadataProvider(
                VoltRelMetadataProvider.INSTANCE, m_relPlanner));
        m_sqlToRelConverter = new SqlToRelConverter(
                null /*view expander*/,
                m_validator,
                m_config.getCatalogReader(),
                cluster,
                m_config.getConvertletTable(),
                m_config.getSqlToRelConverterConfig()
        );
        m_relBuilder = m_config.getSqlToRelConverterConfig().getRelBuilderFactory().create(
                m_sqlToRelConverter.getCluster(), null /*RelOptSchema*/);

        // Initialize internal states.
        m_validatedSqlNode = null;
        m_relRoot = null;
        m_state = State.STATE_1_READY;
    }

    @Override public void reset() {
        m_validatedSqlNode = null;
        m_relRoot = null;
        m_relPlanner.clearRelTraitDefs();
        for (@SuppressWarnings("rawtypes") RelTraitDef def : m_config.getTraitDefs()) {
            m_relPlanner.addRelTraitDef(def);
        }
        m_state = State.STATE_1_READY;
    }

    @Override public SqlNode parse(String sql) throws SqlParseException {
        // parse() does not participate the VoltPlanner state machine because in VoltDB
        // parsing is more isolated from the other steps like validation, conversion, and
        // transformation etc.
        return SqlParser.create(sql, m_config.getParserConfig()).parseQuery(sql);
    }

    @Override public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        ensure(State.STATE_1_READY);
        try {
            // Note: The the data types are validated in this stage.
            // Meanwhile, any identifiers in the query will be fully-qualified.
            // For example: select a from T; -> select T.a from catalog.T as T;
            m_validatedSqlNode = m_validator.validate(sqlNode);
        } catch (CalciteContextException cce) {
            // Some of the validation errors happened because of the lack of support
            // we ought to add to Calcite. We need to fallback for those cases.
            if (AcceptAllSelect.fallback(cce.getLocalizedMessage())) {
                throw new PlannerFallbackException(cce);
            }
            throw cce;
        } catch (RuntimeException e) {
            throw new ValidationException(e);
        }
        m_state = State.STATE_2_VALIDATED;
        return m_validatedSqlNode;
    }

    @Override public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode)
            throws ValidationException {
        final SqlNode validatedNode = validate(sqlNode);
        final RelDataType type = m_validator.getValidatedNodeType(validatedNode);
        return Pair.of(validatedNode, type);
    }

    @Override public RelRoot rel(SqlNode sql) throws RelConversionException {
        if (m_state == State.STATE_1_READY) {
            try {
                validate(sql);
            } catch (ValidationException e) {
                throw new RelConversionException(e);
            }
        }
        ensure(State.STATE_2_VALIDATED);
        Preconditions.checkNotNull(m_validatedSqlNode, "Validated SQL node cannot be null.");

        m_relRoot = m_sqlToRelConverter.convertQuery(
                m_validatedSqlNode, false /*needs validation*/, true /*top*/);

        // Note - ethan - 1/2/2019:
        // Since we do not supported structured (compound) types in VoltDB now,
        // I disabled the type flattening operation here.
        // Enable in the future if structured types become supported in VoltDB.
        // See: org.apache.calcite.sql2rel.RelStructuredTypeFlattener

        // m_relRoot = m_relRoot.withRel(m_sqlToRelConverter.flattenTypes(m_relRoot.rel, true /*restructure*/));

        m_relRoot = m_relRoot.withRel(RelDecorrelator.decorrelateQuery(m_relRoot.rel, m_relBuilder));

        // For each non-aggregate node (i.e. node with no (recursive) input being an aggregate node),
        // projects only the fields required by its consumer
        if (! hasAggregate(m_relRoot.rel)) {
            m_relRoot = m_relRoot.withRel(m_sqlToRelConverter.trimUnusedFields(true, m_relRoot.rel));
        }

        m_state = State.STATE_3_CONVERTED;
        return m_relRoot;
    }

    private static boolean hasAggregate(RelNode node) {
        if (node instanceof Aggregate) {
            return true;
        } else if (node.getInputs().isEmpty()) {
            return false;
        } else {
            return node.getInputs().stream().anyMatch(VoltPlanner::hasAggregate);
        }
    }

    @Override public RelNode convert(SqlNode sql) throws RelConversionException {
        // we want to add an additional {@link LogicalProject} if the root node is not {@link LogicalProject}
        return rel(sql).project();
    }

    @Override public RelDataTypeFactory getTypeFactory() {
        return m_config.getTypeFactory();
    }

    @Override public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel) {
        ensure(State.STATE_3_CONVERTED);
        requiredOutputTraits = requiredOutputTraits.simplify();
        Program program = m_config.getPrograms().get(ruleSetIndex);
        try {
            return program.run(
                    m_relPlanner, rel, requiredOutputTraits,
                    ImmutableList.of() /*materializations*/,
                    ImmutableList.of() /*lattices*/);
        } catch (AssertionError ae) {
            // TODO: PI is not supported in calcite, even it can pass the validation,
            // it will throw an AssertionError "invalid literal: PI" in the conversion phase
            // see ENG-15228
            if (ae.getLocalizedMessage().contains("not a literal: PI")) {
                throw new PlannerFallbackException(ae);
            }
            throw ae;
        }
    }

    /**
     * Use the {@link HepPlanner} to convert one relational expression tree into another relational
     * expression based on a particular rule set and requires set of traits.
     *
     * @param phase  The planner phase
     * @param rel    The root node
     * @return the transformed relational expression tree.
     */
    public static RelNode transformHep(PlannerRules.Phase phase, RelNode rel) {
        return transformHep(phase, HepMatchOrder.BOTTOM_UP, rel, false);
    }

    /**
     * Use the {@link HepPlanner} to convert one relational expression tree into another relational
     * expression based on a particular rule set and requires set of traits.
     *
     * @param phase      The planner phase
     * @param matchOrder The match order.
     * @param rel        The root node
     * @param ordered    If it is true, rules will only apply once in order
     * @return the transformed relational expression tree.
     */
    public static RelNode transformHep(PlannerRules.Phase phase, HepMatchOrder matchOrder, RelNode rel, boolean ordered) {
        final HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        if (matchOrder != null) {
            hepProgramBuilder.addMatchOrder(matchOrder);
        }
        if (ordered) {
            phase.getRules().forEach(hepProgramBuilder::addRuleInstance);
        } else {
            hepProgramBuilder.addGroupBegin();
            phase.getRules().forEach(hepProgramBuilder::addRuleInstance);
            hepProgramBuilder.addGroupEnd();
        }
        HepPlanner planner = new HepPlanner(hepProgramBuilder.build());

        planner.setRoot(rel);
        return planner.findBestExp();
    }

    @Override public void close() {
        reset();
    }

    @Override public RelTraitSet getEmptyTraitSet() {
        return m_relPlanner.emptyTraitSet();
    }

    @SuppressWarnings("rawtypes")
    public void addRelTraitDef(RelTraitDef def) {
        m_relPlanner.addRelTraitDef(def);
    }

    /**
     * Make sure the planner is at a certain state.
     *
     * @param targetedState the expected planner state.
     */
    private void ensure(State targetedState) {
        if (targetedState == m_state) {
            return;
        }
        Preconditions.checkArgument(targetedState.allowTransitFromAny()
                                    || targetedState.ordinal() > m_state.ordinal(),
                "Cannot move to " + targetedState + " from " + m_state);
        targetedState.from(this);
    }

    /**
     * Stage of a statement in the query-preparation life cycle.
     * Adapted but different from {@link org.apache.calcite.prepare.PlannerImpl#State},
     * removing some unused states.
     */
    enum State {
        STATE_1_READY {
            @Override void from(VoltPlanner planner) {
                planner.reset();
            }

            @Override boolean allowTransitFromAny() {
                return true;
            }
        },
        STATE_2_VALIDATED,
        STATE_3_CONVERTED;

        /** Moves planner's state to this state. This must be a higher state. */
        void from(VoltPlanner planner) {
            throw new IllegalArgumentException("Cannot move from " + planner.m_state + " to " + this);
        }

        /** Whether allow all other states to transit to this state. */
        boolean allowTransitFromAny() {
            return false;
        }
    }
}
