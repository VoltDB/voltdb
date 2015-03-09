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

#ifndef HSTOREPROJECTIONNODE_H
#define HSTOREPROJECTIONNODE_H

#include "plannodes/abstractplannode.h"

#include "expressions/abstractexpression.h"

namespace voltdb {

class ProjectionPlanNode : public AbstractPlanNode
{
public:
    ProjectionPlanNode() { }
    ~ProjectionPlanNode();

    PlanNodeType getPlanNodeType() const;

    const std::vector<std::string>& getOutputColumnNames() const
    { return m_outputColumnNames; }

    const std::vector<ValueType>& getOutputColumnTypes() const
    { return m_outputColumnTypes; }

    const std::vector<int32_t>& getOutputColumnSizes() const
    { return m_outputColumnSizes; }

    const std::vector<AbstractExpression*>& getOutputColumnExpressions() const
    { return m_outputColumnExpressions; }

    std::string debugInfo(const std::string& spacer) const;

protected:
    void loadFromJSONObject(PlannerDomValue obj);
    //
    // The node must define what the columns in the output table are
    // going to look like
    //
    std::vector<std::string> m_outputColumnNames;
    std::vector<ValueType> m_outputColumnTypes;
    std::vector<int32_t> m_outputColumnSizes;

    // indicate how to project (or replace) each column value. indices
    // are same as output table's.
    // note that this might be a PlaceholderExpression (for substituted value)
    // It will become ConstantValueExpression for implanted value
    // or TupleValueExpression for pure projection,
    // or CalculatedValueExpression for projection with arithmetic calculation.
    // in ProjectionPlanNode
    std::vector<AbstractExpression*> m_outputColumnExpressions;
};

} // namespace voltdb

#endif
