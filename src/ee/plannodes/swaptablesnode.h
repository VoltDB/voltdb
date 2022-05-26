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

#ifndef HSTORESWAPTABLESNODE_H
#define HSTORESWAPTABLESNODE_H

#include "abstractoperationnode.h"

namespace voltdb {
class PersistentTable;

/**
 * A mostly self-sufficient plan for swapping the content of two identically
 * shaped and indexed tables, such as would be required to implement a
 * hypothetical extended SQL "SWAP TABLE A B;" statement.
 */
class SwapTablesPlanNode : public AbstractOperationPlanNode {
public:
    SwapTablesPlanNode()
        : m_otherTcd(NULL)
        , m_otherTargetTableName("NOT SPECIFIED")
        , m_theIndexes()
        , m_otherIndexes()
    {
    }

    virtual ~SwapTablesPlanNode();
    virtual PlanNodeType getPlanNodeType() const;
    virtual std::string debugInfo(std::string const& spacer) const;

    PersistentTable* getOtherTargetTable() const;
    std::string const& getOtherTargetTableName() const {
        return m_otherTargetTableName;
    }

    std::vector<std::string> const& theIndexes() const {
        return m_theIndexes;
    }

    std::vector<std::string> const& otherIndexes() const {
        return m_otherIndexes;
    }

protected:
     virtual void loadFromJSONObject(PlannerDomValue obj);

     // Other Target Table
     // These tables are different from the input and the output tables
     // The plannode can read in tuples from the input table(s) and apply them to the target table
     // The results of the operations will be written to the the output table
     TableCatalogDelegate* m_otherTcd;
     std::string m_otherTargetTableName;

     std::vector<std::string> m_theIndexes;
     std::vector<std::string> m_otherIndexes;
};

} // namepace voltdb

#endif
