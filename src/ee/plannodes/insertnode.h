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

#ifndef HSTOREINSERTNODE_H
#define HSTOREINSERTNODE_H

#include <sstream>
#include <vector>
#include "abstractoperationnode.h"

namespace voltdb {

class VoltDBEngine;
class Pool;

/**
 *
 */
class InsertPlanNode : public AbstractOperationPlanNode {
public:
    InsertPlanNode()
        : AbstractOperationPlanNode()
        , m_multiPartition(false)
        , m_fieldMap()
        , m_isUpsert(false)
        , m_sourceIsPartitioned(false) {
    }

    PlanNodeType getPlanNodeType() const;

    bool isMultiPartition() const { return m_multiPartition; }

    bool isUpsert() const { return m_isUpsert; }

    bool sourceIsPartitioned() const { return m_sourceIsPartitioned; }

    bool isMultiRowInsert() const {
        // Materialize nodes correspond to INSERT INTO ... VALUES syntax.
        // Otherwise this may be a multi-row insert via INSERT INTO ... SELECT.
        return m_children[0]->getPlanNodeType() != PlanNodeType::Materialize;
    }

    void initTupleWithDefaultValues(VoltDBEngine* engine,
                                    Pool* pool,
                                    const std::set<int>& fieldsExplicitlySet,
                                    TableTuple& templateTuple,
                                    std::vector<int> &nowFields);

    const std::vector<int>& getFieldMap() const { return m_fieldMap; }

protected:
    void loadFromJSONObject(PlannerDomValue obj);

private:
    bool m_multiPartition;
    std::vector<int> m_fieldMap;
    bool m_isUpsert;
    bool m_sourceIsPartitioned;
};

} // namespace voltdb

#endif
