/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#include "aggregatenode.h"

#include "common/types.h"
#include "storage/table.h"

#include <sstream>
#include <stdexcept>

using namespace std;
using namespace voltdb;

AggregatePlanNode::~AggregatePlanNode()
{
    if (!isInline())
    {
        delete getOutputTable();
        setOutputTable(NULL);
    }
    for (int i = 0; i < m_aggregateInputExpressions.size(); i++)
    {
        delete m_aggregateInputExpressions[i];
    }
    for (int i = 0; i < m_groupByExpressions.size(); i++)
    {
        delete m_groupByExpressions[i];
    }
    delete m_prePredicate;
    delete m_postPredicate;
}

string AggregatePlanNode::debugInfo(const string &spacer) const {
    ostringstream buffer;
    buffer << spacer << "\nAggregates["
           << (int) m_aggregates.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregates.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << "type="
               << expressionToString(m_aggregates[ctr]) << "\n";
        buffer << spacer << "distinct="
               << m_distinctAggregates[ctr] << "\n";
        buffer << spacer << "outcol="
               << m_aggregateOutputColumns[ctr] << "\n";
        buffer << spacer << "expr="
               << (m_aggregateInputExpressions[ctr] ?
                   m_aggregateInputExpressions[ctr]->debug(spacer) :
                   "null")
               << "\n";
    }
    buffer << spacer << "}";

    buffer << spacer << "\nGroupByExpressions[";
    string add = "";
    for (int ctr = 0, cnt = (int) m_groupByExpressions.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << m_groupByExpressions[ctr]->debug(spacer);
        add = ", ";
    }
    buffer << "]\n";

    return buffer.str();
}

void
AggregatePlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    PlannerDomValue aggregateColumnsArray = obj.valueForKey("AGGREGATE_COLUMNS");
    for (int i = 0; i < aggregateColumnsArray.arrayLen(); i++) {
        PlannerDomValue aggregateColumnValue = aggregateColumnsArray.valueAtIndex(i);
        bool containsType = false;
        bool containsDistinct = false;
        bool containsOutputColumn = false;
        bool containsExpression = false;
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_TYPE")) {
            containsType = true;
            string aggregateColumnTypeString = aggregateColumnValue.valueForKey("AGGREGATE_TYPE").asStr();
            m_aggregates.push_back(stringToExpression(aggregateColumnTypeString));
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_DISTINCT")) {
            containsDistinct = true;
            bool distinct = aggregateColumnValue.valueForKey("AGGREGATE_DISTINCT").asInt() == 1;
            m_distinctAggregates.push_back(distinct);
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_OUTPUT_COLUMN")) {
            containsOutputColumn = true;
            int column = aggregateColumnValue.valueForKey("AGGREGATE_OUTPUT_COLUMN").asInt();
            m_aggregateOutputColumns.push_back(column);
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_EXPRESSION")) {
            containsExpression = true;
            PlannerDomValue exprDom = aggregateColumnValue.valueForKey("AGGREGATE_EXPRESSION");
            m_aggregateInputExpressions.push_back(AbstractExpression::buildExpressionTree(exprDom));
        }

        if(!(containsType && containsDistinct && containsOutputColumn)) {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AggregatePlanNode::loadFromJSONObject:"
                                      " Missing type, distinct, or outputcolumn.");
        }
        if ( ! containsExpression) {
            m_aggregateInputExpressions.push_back(NULL);
        }
    }

    if (obj.hasNonNullKey("GROUPBY_EXPRESSIONS")) {
        PlannerDomValue groupByExpressionsArray = obj.valueForKey("GROUPBY_EXPRESSIONS");
        for (int i = 0; i < groupByExpressionsArray.arrayLen(); i++) {
            m_groupByExpressions.push_back(AbstractExpression::buildExpressionTree(groupByExpressionsArray.valueAtIndex(i)));
        }
    }

    if (obj.hasNonNullKey("PRE_PREDICATE")) {
        m_prePredicate = AbstractExpression::buildExpressionTree(obj.valueForKey("PRE_PREDICATE"));
    }

    if (obj.hasNonNullKey("POST_PREDICATE")) {
        m_postPredicate = AbstractExpression::buildExpressionTree(obj.valueForKey("POST_PREDICATE"));
    }
}

void AggregatePlanNode::collectOutputExpressions(std::vector<AbstractExpression*>& outputColumnExpressions) const
{
    const std::vector<SchemaColumn*>& outputSchema = getOutputSchema();
    size_t size = outputSchema.size();
    outputColumnExpressions.resize(size);
    for (int ii = 0; ii < size; ii++) {
        SchemaColumn* outputColumn = outputSchema[ii];
        outputColumnExpressions[ii] = outputColumn->getExpression();
    }
}

// definitions of public test methods

void
AggregatePlanNode::setAggregates(vector<ExpressionType> &aggregates)
{
    m_aggregates = aggregates;
}

void
AggregatePlanNode::setAggregateOutputColumns(vector<int> outputColumns)
{
    m_aggregateOutputColumns = outputColumns;
}
