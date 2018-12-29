/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.plannerv2;

import java.util.Objects;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/**
 * A base class for implementing tests against {@link VoltSqlToRelConverter}.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class VoltConverterTestCase extends VoltSqlValidatorTestCase {

    protected RelRoot parseValidateAndConvert(String sql) {
        Objects.requireNonNull(getValidator(), "m_validator is null");
        Objects.requireNonNull(getSchemaPlus(), "m_schemaPlus is null");
        Objects.requireNonNull(getConfig(), "m_config is null");
        SqlNode node = parseAndValidate(sql);

        RexBuilder rexBuilder = new RexBuilder(getConfig().getTypeFactory());
        VolcanoPlanner planner = new VolcanoPlanner();
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter converter =  new SqlToRelConverter(
                null /* view expander */,
                getValidator(),
                getConfig().getCatalogReader(),
                cluster,
                getConfig().getConvertletTable(),
                getConfig().getSqlToRelConverterConfig());
        RelRoot root = converter.convertQuery(node, false, true);
        root = root.withRel(converter.decorrelate(node, root.rel));
        return root;
    }
}
