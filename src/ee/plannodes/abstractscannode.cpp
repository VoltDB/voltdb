/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#include "abstractscannode.h"

#include "PlanColumn.h"
#include "storage/table.h"
#include "catalog/table.h"
#include "catalog/column.h"

using namespace json_spirit;
using namespace std;
using namespace voltdb;

AbstractScanPlanNode::AbstractScanPlanNode(int32_t id)
    : AbstractPlanNode(id), m_predicate(NULL)
{
    m_targetTable = NULL;
}

AbstractScanPlanNode::AbstractScanPlanNode()
    : AbstractPlanNode(), m_predicate(NULL)
{
    m_targetTable = NULL;
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
    return m_targetTable;
}

void
AbstractScanPlanNode::setTargetTable(Table* table)
{
    m_targetTable = table;
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

int
AbstractScanPlanNode::getColumnIndexFromName(string name,
                                             const catalog::Database* db) const
{
    catalog::Table* table = db->tables().get(m_targetTableName);
    assert (table != NULL);
    if (NULL == table)
    {
        return -1;
    }

    catalog::CatalogMap<catalog::Column> columns = table->columns();
    catalog::Column* column = columns.get(name);
    assert (NULL != column);
    if (NULL == column)
    {
        return -1;
    }
    return column->index();
}

int
AbstractScanPlanNode::getColumnIndexFromGuid(int guid,
                                             const catalog::Database* db) const
{
    // if the scan node has an inlined projection, then we
    // want to look up the column index in the projection
    AbstractPlanNode* projection =
        getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION);
    if (projection != NULL)
    {
        return projection->getColumnIndexFromGuid(guid, db);
    }
    else
    {
        string name = "";
        for (int i = 0; i < m_outputColumnGuids.size(); i++)
        {
            if (guid == m_outputColumnGuids[i])
            {
                return getColumnIndexFromName(m_outputColumnNames[i], db);
            }
        }
    }
    return -1;
}

void
AbstractScanPlanNode::loadFromJSONObject(json_spirit::Object& obj,
                                         const catalog::Database* catalog_db)
{
    Value outputColumnsValue = find_value(obj, "OUTPUT_COLUMNS");
    if (outputColumnsValue == Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractScanPlanNode::loadFromJSONObject:"
                                      " Can't find OUTPUT_COLUMNS value");
    }
    Array outputColumnsArray = outputColumnsValue.get_array();

    for (int ii = 0; ii < outputColumnsArray.size(); ii++)
    {
        Value outputColumnValue = outputColumnsArray[ii];
        PlanColumn outputColumn = PlanColumn(outputColumnValue.get_obj());
        m_outputColumnGuids.push_back(outputColumn.getGuid());
        m_outputColumnNames.push_back(outputColumn.getName());
        m_outputColumnTypes.push_back(outputColumn.getType());
        m_outputColumnSizes.push_back(outputColumn.getSize());
    }

    json_spirit::Value targetTableNameValue =
        json_spirit::find_value(obj, "TARGET_TABLE_NAME");
    if (targetTableNameValue == json_spirit::Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractScanPlanNode::loadFromJSONObject:"
                                      " Couldn't find TARGET_TABLE_NAME value");
    }

    m_targetTableName = targetTableNameValue.get_str();

    json_spirit::Value predicateValue = json_spirit::find_value(obj, "PREDICATE");
    if (!(predicateValue == json_spirit::Value::null))
    {
        json_spirit::Object predicateObject = predicateValue.get_obj();
        m_predicate = AbstractExpression::buildExpressionTree(predicateObject);
    }
}
