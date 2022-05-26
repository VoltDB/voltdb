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
#include "unionnode.h"

#include <sstream>

namespace voltdb {

UnionPlanNode::~UnionPlanNode() { }

PlanNodeType UnionPlanNode::getPlanNodeType() const { return PlanNodeType::Union; }

std::string UnionPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "UnionType[" << m_unionType << "]\n";
    return buffer.str();
}

void UnionPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    std::string unionTypeStr = obj.valueForKey("UNION_TYPE").asStr();
    if (unionTypeStr == "UNION") {
        m_unionType = UNION_TYPE_UNION;
    } else if (unionTypeStr == "UNION_ALL") {
        m_unionType = UNION_TYPE_UNION_ALL;
    } else if (unionTypeStr == "INTERSECT") {
        m_unionType = UNION_TYPE_INTERSECT;
    } else if (unionTypeStr == "INTERSECT_ALL") {
        m_unionType = UNION_TYPE_INTERSECT_ALL;
    } else if (unionTypeStr == "EXCEPT") {
        m_unionType = UNION_TYPE_EXCEPT;
    } else if (unionTypeStr == "EXCEPT_ALL") {
        m_unionType = UNION_TYPE_EXCEPT_ALL;
    } else if (unionTypeStr == "NOUNION") {
        m_unionType = UNION_TYPE_NOUNION;
    } else {
        throwSerializableEEException(
                "UnionPlanNode::loadFromJSONObject: Unsupported UNION_TYPE value %s",
                unionTypeStr.c_str());
    }
}

} // namespace voltdb
