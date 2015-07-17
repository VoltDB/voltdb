/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include <sstream>

#include "boost/foreach.hpp"

#include "receivenode.h"

namespace voltdb {

namespace {
    void schemaDebugInfo(std::ostringstream& buffer, const std::vector<SchemaColumn*>& schema,
        const std::string& schema_name, const std::string& spacer) {
        buffer << spacer << schema_name << " Table Columns[" << schema.size() << "]:\n";
        for (int ctr = 0, cnt = (int)schema.size(); ctr < cnt; ctr++) {
            SchemaColumn* col = schema[ctr];
            buffer << spacer << "  [" << ctr << "] ";
            buffer << "name=" << col->getColumnName() << " : ";
            buffer << "size=" << col->getExpression()->getValueSize() << " : ";
            buffer << "type=" << col->getExpression()->getValueType() << "\n";
        }
    }
}


ReceivePlanNode::ReceivePlanNode() :
    m_mergeReceive(false), m_outputSchemaPreAgg()
{ }

ReceivePlanNode::~ReceivePlanNode()
{
    if (m_mergeReceive) {
        BOOST_FOREACH(SchemaColumn* scol, m_outputSchemaPreAgg) {
            delete scol;
        }
    }
}

PlanNodeType ReceivePlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_RECEIVE; }

std::string ReceivePlanNode::debugInfo(const std::string& spacer) const
{
    std::ostringstream buffer;
    if (!m_mergeReceive) {
        schemaDebugInfo(buffer, getOutputSchema(), "Incoming", spacer);
    } else {
        buffer << spacer << "Merge Receive\n";
        schemaDebugInfo(buffer, m_outputSchemaPreAgg, "Incoming", spacer);
        schemaDebugInfo(buffer, getOutputSchema(), "Outgoing", spacer);
    }
    return buffer.str();
}

void ReceivePlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    if (obj.hasNonNullKey("MERGE_RECEIVE")) {
        m_mergeReceive = obj.valueForKey("MERGE_RECEIVE").asBool();
        if (m_mergeReceive) {
            if (obj.hasNonNullKey("OUTPUT_SCHEMA_PRE_AGG")) {
                PlannerDomValue outputSchemaArray = obj.valueForKey("OUTPUT_SCHEMA_PRE_AGG");
                m_outputSchemaPreAgg.reserve(outputSchemaArray.arrayLen());

                for (int i = 0; i < outputSchemaArray.arrayLen(); i++) {
                    PlannerDomValue outputColumnValue = outputSchemaArray.valueAtIndex(i);
                    SchemaColumn* outputColumn = new SchemaColumn(outputColumnValue, i);
                    m_outputSchemaPreAgg.push_back(outputColumn);
                }
            }
        }
    }
}

TupleSchema* ReceivePlanNode::allocateTupleSchemaPreAgg() const
{
    return AbstractPlanNode::generateTupleSchema(m_outputSchemaPreAgg);
}

} // namespace voltdb
