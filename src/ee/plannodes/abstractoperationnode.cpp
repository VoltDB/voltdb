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

#include "abstractoperationnode.h"

#include "common/debuglog.h"
#include "execution/VoltDBEngine.h"
#include "storage/TableCatalogDelegate.hpp"

#include <sstream>

namespace voltdb {

AbstractOperationPlanNode::~AbstractOperationPlanNode() { }

Table* AbstractOperationPlanNode::getTargetTable() const
{
    if (m_tcd == NULL) {
        return NULL;
    }
    return m_tcd->getTable();
}

std::string AbstractOperationPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << spacer << "TargetTable[" << m_target_table_name << "]\n";
    return buffer.str();
}

void AbstractOperationPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    m_target_table_name = obj.valueForKey("TARGET_TABLE_NAME").asStr();
    VoltDBEngine* engine = ExecutorContext::getEngine();
    m_tcd = engine->getTableDelegate(m_target_table_name);
    if ( ! m_tcd) {
        VOLT_ERROR("Failed to retrieve target table from execution engine for PlanNode '%s'",
                   debug().c_str());
        //TODO: throw something
    }
}

} // namespace voltdb
