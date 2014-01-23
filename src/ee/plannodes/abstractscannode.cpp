/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "abstractscannode.h"

#include "storage/table.h"
#include "catalog/table.h"
#include "catalog/column.h"

using namespace std;
using namespace voltdb;

AbstractScanPlanNode::AbstractScanPlanNode(int32_t id)
    : AbstractPlanNode(id), m_predicate(NULL)
{
    m_tcd = NULL;
}

AbstractScanPlanNode::AbstractScanPlanNode()
    : AbstractPlanNode(), m_predicate(NULL)
{
    m_tcd = NULL;
}

AbstractScanPlanNode::~AbstractScanPlanNode()
{
    delete m_predicate;
}

void
AbstractScanPlanNode::setPredicate(AbstractExpression* predicate)
{
    assert(!m_predicate);
    if (m_predicate != predicate)
    {
        delete m_predicate;
    }
    m_predicate = predicate;
}

AbstractExpression*
AbstractScanPlanNode::getPredicate() const
{
    return m_predicate;
}

Table*
AbstractScanPlanNode::getTargetTable() const
{
    if (m_tcd == NULL) {
        return NULL;
    }
    return m_tcd->getTable();
}

void
AbstractScanPlanNode::setTargetTableDelegate(TableCatalogDelegate* tcd)
{
    m_tcd = tcd;
}

string
AbstractScanPlanNode::getTargetTableName() const
{
    return m_targetTableName;
}

void
AbstractScanPlanNode::setTargetTableName(string table_name)
{
    m_targetTableName = table_name;
}

string
AbstractScanPlanNode::debugInfo(const string &spacer) const
{
    ostringstream buffer;
    buffer << spacer << "TargetTable[" << m_targetTableName << "]\n";
    return buffer.str();
}

void
AbstractScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    m_targetTableName = obj.valueForKey("TARGET_TABLE_NAME").asStr();

    if (obj.hasNonNullKey("PREDICATE")) {
        m_predicate = AbstractExpression::buildExpressionTree(obj.valueForKey("PREDICATE"));
    }
}
