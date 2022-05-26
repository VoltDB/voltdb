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

#include "abstractscannode.h"

#include "execution/VoltDBEngine.h"
#include "storage/TableCatalogDelegate.hpp"

namespace voltdb {

AbstractScanPlanNode::~AbstractScanPlanNode() { }

Table* AbstractScanPlanNode::getTargetTable() const
{
    if (m_tcd == NULL) {
        return NULL;
    }
    return m_tcd->getTable();
}

std::string AbstractScanPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << spacer << "TargetTable[" << m_target_table_name << "], scanType[";
    switch (m_scanType) {
    case SUBQUERY_SCAN:
        buffer << "SUBQUERY_SCAN";
        break;
    case PERSISTENT_TABLE_SCAN:
        buffer << "PERSISTENT_TABLE_SCAN";
        break;
    case CTE_SCAN:
        buffer << "CTE_SCAN";
        break;
    case INVALID_SCAN:
        buffer << "INVALID_SCAN";
        break;
    default:
        buffer << "<<unknown scan type>>";
        break;
    }
    buffer << "]\n";
    return buffer.str();
}

void AbstractScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    m_target_table_name = obj.valueForKey("TARGET_TABLE_NAME").asStr();

    m_isEmptyScan = obj.hasNonNullKey("PREDICATE_FALSE");

    // Set the predicate (if any) only if it's not a trivial FALSE expression
    if (!m_isEmptyScan) {
        m_predicate.reset(loadExpressionFromJSONObject("PREDICATE", obj));
    }

    m_tcd = NULL;
    m_cteStmtId = -1;
    if (obj.hasKey("CTE_STMT_ID")) {
        m_cteStmtId = obj.valueForKey("CTE_STMT_ID").asInt();
        if (m_cteStmtId > -1) {
            m_scanType = CTE_SCAN;
        }
    }
    else if (obj.hasNonNullKey("SUBQUERY_INDICATOR")) {
        m_scanType = SUBQUERY_SCAN;
    }
    else {
        m_scanType = PERSISTENT_TABLE_SCAN;
        VoltDBEngine* engine = ExecutorContext::getEngine();
        m_tcd = engine->getTableDelegate(m_target_table_name);
        if ( ! m_tcd) {
            VOLT_ERROR("Failed to retrieve target table from execution engine for PlanNode '%s'",
                       debug().c_str());
            //TODO: throw something
        }
    }
}

} // namespace voltdb
