/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#include "projectionnode.h"

#include "storage/table.h"

using namespace std;
using namespace voltdb;

ProjectionPlanNode::ProjectionPlanNode(CatalogId id) : AbstractPlanNode(id)
{
    // Do nothing
}

ProjectionPlanNode::ProjectionPlanNode() : AbstractPlanNode()
{
    // Do nothing
}

ProjectionPlanNode::~ProjectionPlanNode()
{
    delete getOutputTable();
    setOutputTable(NULL);
}

PlanNodeType
ProjectionPlanNode::getPlanNodeType() const
{
    return PLAN_NODE_TYPE_PROJECTION;
}

void
ProjectionPlanNode::setOutputColumnNames(vector<string>& names)
{
    m_outputColumnNames = names;
}

vector<string>&
ProjectionPlanNode::getOutputColumnNames()
{
    return m_outputColumnNames;
}

const vector<string>&
ProjectionPlanNode::getOutputColumnNames() const
{
    return m_outputColumnNames;
}

void
ProjectionPlanNode::setOutputColumnTypes(vector<ValueType>& types)
{
    m_outputColumnTypes = types;
}

vector<ValueType>&
ProjectionPlanNode::getOutputColumnTypes()
{
    return m_outputColumnTypes;
}

const vector<ValueType>&
ProjectionPlanNode::getOutputColumnTypes() const
{
    return m_outputColumnTypes;
}


void ProjectionPlanNode::setOutputColumnSizes(vector<int32_t>& sizes)
{
    m_outputColumnSizes = sizes;

}

vector<int32_t>&
ProjectionPlanNode::getOutputColumnSizes()
{
    return m_outputColumnSizes;
}

const vector<int32_t>&
ProjectionPlanNode::getOutputColumnSizes() const
{
    return m_outputColumnSizes;
}

void
ProjectionPlanNode::setOutputColumnExpressions(vector<AbstractExpression*>& exps)
{
    m_outputColumnExpressions = exps;
}

vector<AbstractExpression*>&
ProjectionPlanNode::getOutputColumnExpressions()
{
    return m_outputColumnExpressions;
}

const vector<AbstractExpression*>&
ProjectionPlanNode::getOutputColumnExpressions() const
{
    return m_outputColumnExpressions;
}

string
ProjectionPlanNode::debugInfo(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "Projection Output["
           << m_outputColumnNames.size() << "]:\n";
    for (int ctr = 0, cnt = (int)m_outputColumnNames.size(); ctr < cnt; ctr++)
    {
        buffer << spacer << "  [" << ctr << "] ";
        buffer << "name=" << m_outputColumnNames[ctr] << " : ";
        buffer << "size=" << m_outputColumnSizes[ctr] << " : ";
        buffer << "type=" << getTypeName(m_outputColumnTypes[ctr]) << "\n";
        if (m_outputColumnExpressions[ctr] != NULL)
        {
            buffer << m_outputColumnExpressions[ctr]->debug(spacer + "   ");
        }
        else
        {
            buffer << spacer << "  " << "<NULL>" << "\n";
        }
    }
    return buffer.str();
}


void
ProjectionPlanNode::loadFromJSONObject(json_spirit::Object& obj)
{
    // XXX-IZZY move this to init at some point
    for (int ii = 0; ii < getOutputSchema().size(); ii++)
    {
        SchemaColumn* outputColumn = getOutputSchema()[ii];
        m_outputColumnNames.push_back(outputColumn->getColumnName());
        m_outputColumnTypes.push_back(outputColumn->getType());
        m_outputColumnSizes.push_back(outputColumn->getSize());
        m_outputColumnExpressions.push_back(outputColumn->getExpression());
    }
}
