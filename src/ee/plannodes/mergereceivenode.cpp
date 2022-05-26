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

#include "mergereceivenode.h"

#include "boost/foreach.hpp"

namespace voltdb {

MergeReceivePlanNode::MergeReceivePlanNode()
    : AbstractReceivePlanNode()
    , m_outputSchemaPreAgg()
{
}

MergeReceivePlanNode::~MergeReceivePlanNode()
{
    BOOST_FOREACH(SchemaColumn* scol, m_outputSchemaPreAgg) {
        delete scol;
    }
}

PlanNodeType MergeReceivePlanNode::getPlanNodeType() const { return PlanNodeType::MergeReceive; }

void MergeReceivePlanNode::setScratchTable(AbstractTempTable* table) {
    m_scratchTable.setTable(table);
}

std::string MergeReceivePlanNode::debugInfo(const std::string& spacer) const
{
    std::ostringstream buffer;
    if (m_outputSchemaPreAgg.empty()) {
        buffer << spacer << "Incoming Table effectively the same as Outgoing\n";
    } else {
        schemaDebugInfo(buffer, m_outputSchemaPreAgg, "Incoming", spacer);
    }
    schemaDebugInfo(buffer, getOutputSchema(), "Outgoing", spacer);
    return buffer.str();
}

void MergeReceivePlanNode::loadFromJSONObject(PlannerDomValue obj)
{
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

TupleSchema* MergeReceivePlanNode::allocateTupleSchemaPreAgg() const
{
    return AbstractPlanNode::generateTupleSchema(m_outputSchemaPreAgg);
}

} // namespace voltdb
