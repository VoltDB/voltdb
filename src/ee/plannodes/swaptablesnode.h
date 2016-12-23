/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
    SwapTablesPlanNode() : AbstractOperationPlanNode()
        , m_otherTcd(NULL)
        , m_otherTargetTableName("NOT SPECIFIED")
    { }

    virtual ~SwapTablesPlanNode();
    virtual PlanNodeType getPlanNodeType() const;
    virtual std::string debugInfo(const std::string &spacer) const;

    PersistentTable* getTargetTable1() const;
    PersistentTable* getTargetTable2() const;
    void setOtherTargetTableDelegate(TableCatalogDelegate * tcd) { m_tcd = tcd; }
    const std::string& getOtherTargetTableName() const { return m_otherTargetTableName; }

    std::vector<std::string> const& indexesToSwap() {
        return m_indexesToSwap;
    }
    std::vector<std::string> const& otherIndexesToSwap() {
        return m_otherIndexesToSwap;
    }
    std::vector<std::string> const& indexesToRebuild() {
        return m_indexesToRebuild;
    }
    std::vector<std::string> const& otherIndexesToRebuild() {
        return m_otherIndexesToRebuild;
    }
    std::vector<std::string> const& viewsToSwap() {
        return m_viewsToSwap;
    }
    std::vector<std::string> const& otherViewsToSwap() {
        return m_otherViewsToSwap;
    }
    std::vector<std::string> const& viewsToRebuild() {
        return m_viewsToRebuild;
    }
    std::vector<std::string> const& otherViewsToRebuild() {
        return m_otherViewsToRebuild;
    }

protected:
     virtual void loadFromJSONObject(PlannerDomValue obj);

     // Other Target Table
     // These tables are different from the input and the output tables
     // The plannode can read in tuples from the input table(s) and apply them to the target table
     // The results of the operations will be written to the the output table
     TableCatalogDelegate * m_otherTcd;
     std::string m_otherTargetTableName;

     std::vector<std::string> m_indexesToSwap;
     std::vector<std::string> m_otherIndexesToSwap;
     std::vector<std::string> m_indexesToRebuild;
     std::vector<std::string> m_otherIndexesToRebuild;
     std::vector<std::string> m_viewsToSwap;
     std::vector<std::string> m_otherViewsToSwap;
     std::vector<std::string> m_viewsToRebuild;
     std::vector<std::string> m_otherViewsToRebuild;
};

} // namepace voltdb

#endif
