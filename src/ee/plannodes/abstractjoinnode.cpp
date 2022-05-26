/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "abstractjoinnode.h"

#include "common/TupleSchema.h"

#include "boost/foreach.hpp"

namespace voltdb {

AbstractJoinPlanNode::AbstractJoinPlanNode()
    : m_preJoinPredicate()
    , m_joinPredicate()
    , m_wherePredicate()
    , m_joinType(JOIN_TYPE_INVALID)
    , m_outputSchemaPreAgg()
    , m_tupleSchemaPreAgg(NULL)
{
}

AbstractJoinPlanNode::~AbstractJoinPlanNode()
{
    BOOST_FOREACH(SchemaColumn* scol, m_outputSchemaPreAgg) {
        delete scol;
    }

    TupleSchema::freeTupleSchema(m_tupleSchemaPreAgg);
}

void AbstractJoinPlanNode::getOutputColumnExpressions(
        std::vector<AbstractExpression*>& outputExpressions) const {
    std::vector<SchemaColumn*> outputSchema;
    if (m_outputSchemaPreAgg.size() > 0) {
        outputSchema = m_outputSchemaPreAgg;
    } else {
        outputSchema = getOutputSchema();
    }
    size_t schemaSize = outputSchema.size();

    for (int i = 0; i < schemaSize; i++) {
        outputExpressions.push_back(outputSchema[i]->getExpression());
    }
}

std::string AbstractJoinPlanNode::debugInfo(const std::string& spacer) const
{
    std::ostringstream buffer;
    buffer << spacer << "JoinType[" << joinToString(m_joinType) << "]\n";
    if (m_preJoinPredicate != NULL)
    {
        buffer << spacer << "Pre-Join Predicate\n";
        buffer << m_preJoinPredicate->debug(spacer);
    }
    if (m_joinPredicate != NULL)
    {
        buffer << spacer << "Join Predicate\n";
        buffer << m_joinPredicate->debug(spacer);
    }
    if (m_wherePredicate != NULL)
    {
        buffer << spacer << "Where Predicate\n";
        buffer << m_wherePredicate->debug(spacer);
    }
    return (buffer.str());
}

void
AbstractJoinPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    m_joinType = stringToJoin(obj.valueForKey("JOIN_TYPE").asStr());

    m_preJoinPredicate.reset(loadExpressionFromJSONObject("PRE_JOIN_PREDICATE", obj));
    m_joinPredicate.reset(loadExpressionFromJSONObject("JOIN_PREDICATE", obj));
    m_wherePredicate.reset(loadExpressionFromJSONObject("WHERE_PREDICATE", obj));

    if (obj.hasKey("OUTPUT_SCHEMA_PRE_AGG")) {
        PlannerDomValue outputSchemaArray = obj.valueForKey("OUTPUT_SCHEMA_PRE_AGG");
        for (int i = 0; i < outputSchemaArray.arrayLen(); i++) {
            PlannerDomValue outputColumnValue = outputSchemaArray.valueAtIndex(i);
            SchemaColumn* outputColumn = new SchemaColumn(outputColumnValue, i);
            m_outputSchemaPreAgg.push_back(outputColumn);
        }
        m_tupleSchemaPreAgg = AbstractPlanNode::generateTupleSchema(m_outputSchemaPreAgg);
    }
    else {
        m_tupleSchemaPreAgg = NULL;
    }

}

} // namespace voltdb
