/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
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
import org.voltdb.plannerv2.metadata.VoltRelMetadataProvider;
import org.voltdb.plannerv2.rules.PlannerRules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
        m_relPlanner = new VolcanoPlanner();
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

        m_state = State.STATE_3_CONVERTED;
        return m_relRoot;
    }

    @Override public RelNode convert(SqlNode sql) throws RelConversionException {
        return rel(sql).rel;
    }

    @Override public RelDataTypeFactory getTypeFactory() {
        return m_config.getTypeFactory();
    }

    @Override public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel)
            throws RelConversionException {
        ensure(State.STATE_3_CONVERTED);
        requiredOutputTraits = requiredOutputTraits.simplify();
        Program program = m_config.getPrograms().get(ruleSetIndex);
        return program.run(
                m_relPlanner, rel, requiredOutputTraits,
                ImmutableList.of() /*materializations*/,
                ImmutableList.of() /*lattices*/);
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
        return transformHep(phase, null, rel);
    }

    /**
     * Use the {@link HepPlanner} to convert one relational expression tree into another relational
     * expression based on a particular rule set and requires set of traits.
     *
     * @param phase     The planner phase
     * @param bottomUp  Whether to use the bottom-up match order.
     * @param rel       The root node
     * @return the transformed relational expression tree.
     */
    public static RelNode transformHep(PlannerRules.Phase phase, HepMatchOrder matchOrder, RelNode rel) {
        final HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        if (matchOrder != null) {
            hepProgramBuilder.addMatchOrder(matchOrder);
        }
        phase.getRules().forEach(hepProgramBuilder::addRuleInstance);
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
