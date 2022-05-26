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
#include "swaptablesnode.h"

namespace voltdb {

SwapTablesPlanNode::~SwapTablesPlanNode() { }

PlanNodeType SwapTablesPlanNode::getPlanNodeType() const { return PlanNodeType::SwapTables; }

PersistentTable* SwapTablesPlanNode::getOtherTargetTable() const {
    if (m_otherTcd == NULL) {
        return NULL;
    }
    return m_otherTcd->getPersistentTable();
}

static std::string commaJoined(const std::vector<std::string>& array) {
    std::ostringstream buffer;
    const char* prefix = "";
    int len = array.size();
    for (int i = 0; i < len; ++i) {
        buffer << prefix << array[i];
        prefix = ",";
    }
    return buffer.str();
}

std::string SwapTablesPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "OtherTargetTable[" << m_otherTargetTableName << "]\n";
    buffer << spacer << "INDEXES[" << commaJoined(m_theIndexes) << "]\n";
    buffer << spacer << "OTHER_INDEXES[" << commaJoined(m_otherIndexes) << "]\n";
    return buffer.str();
}

void SwapTablesPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    AbstractOperationPlanNode::loadFromJSONObject(obj);
    m_otherTargetTableName = obj.valueForKey("OTHER_TARGET_TABLE_NAME").asStr();
    loadStringArrayFromJSONObject("INDEXES", obj, m_theIndexes);
    loadStringArrayFromJSONObject("OTHER_INDEXES", obj, m_otherIndexes);

    VoltDBEngine* engine = ExecutorContext::getEngine();
    m_otherTcd = engine->getTableDelegate(m_otherTargetTableName);
    if ( ! m_otherTcd) {
        VOLT_ERROR("Failed to retrieve second target table from execution engine for PlanNode: %s",
                   debug().c_str());
        //TODO: throw something
    }

}

} // namespace voltdb
