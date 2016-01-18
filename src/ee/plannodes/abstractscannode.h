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

#ifndef HSTORESCANNODE_H
#define HSTORESCANNODE_H

#include "abstractplannode.h"

#include "expressions/abstractexpression.h"

namespace voltdb {

class TableCatalogDelegate;

class AbstractScanPlanNode : public AbstractPlanNode
{
public:
    ~AbstractScanPlanNode();
    std::string debugInfo(const std::string& spacer) const;

    Table* getTargetTable() const;
    void setTargetTableDelegate(TableCatalogDelegate* tcd) { m_tcd = tcd; } // DEPRECATED?

    std::string getTargetTableName() const { return m_target_table_name; } // DEPRECATED?
    AbstractExpression* getPredicate() const { return m_predicate.get(); }

    bool isSubQuery() const { return m_isSubQuery; }

    bool isEmptyScan() const { return m_isEmptyScan; }

protected:
    AbstractScanPlanNode() { }

    void loadFromJSONObject(PlannerDomValue obj);

    // Target Table
    // These tables are different from the input and the output tables
    // The plannode can read in tuples from the input table(s) and
    // apply them to the target table
    // The results of the operations will be written to the the output table
    //
    std::string m_target_table_name;
    TableCatalogDelegate* m_tcd;
    //
    // This is the predicate used to filter out tuples during the scan
    //
    boost::scoped_ptr<AbstractExpression> m_predicate;
    // True if this scan represents a sub query
    bool m_isSubQuery;
    // True if this scan has a predicate that always evaluates to FALSE
    bool m_isEmptyScan;
};

} // namespace voltdb

#endif
