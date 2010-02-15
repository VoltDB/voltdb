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
#ifndef HSTOREORDERBYNODE_H
#define HSTOREORDERBYNODE_H

#include "abstractplannode.h"

#include <vector>

namespace voltdb
{

class OrderByPlanNode : public AbstractPlanNode
{
public:
    OrderByPlanNode(CatalogId id);
    OrderByPlanNode();
    ~OrderByPlanNode();

    virtual PlanNodeType getPlanNodeType() const;

    void setSortColumns(std::vector<int> &cols);
    std::vector<int>& getSortColumns();
    const std::vector<int>& getSortColumns() const;

    std::vector<int>& getSortColumnGuids();

    void setSortColumnNames(std::vector<std::string> &column_names);
    std::vector<std::string>& getSortColumnNames();
    const std::vector<std::string>& getSortColumnNames() const;

    void setSortDirections(std::vector<SortDirectionType> &dirs);
    std::vector<SortDirectionType>& getSortDirections();
    const std::vector<SortDirectionType>& getDirections() const;

    std::string debugInfo(const std::string &spacer) const;

protected:
    friend AbstractPlanNode*
        AbstractPlanNode::fromJSONObject(json_spirit::Object& obj,
                                         const catalog::Database* catalog_db);
    virtual void loadFromJSONObject(json_spirit::Object& obj,
                                    const catalog::Database* catalog_db);
    /**
     * Sort Columns Indexes
     * The column index in the table that we should sort on
     */
    std::vector<int> m_sortColumns;
    std::vector<int> m_sortColumnGuids;
    std::vector<std::string> m_sortColumnNames;
    /**
     * Sort Directions
     * If true, sort in ASC order
     * If false, sort in DESC order
     */
    std::vector<SortDirectionType> m_sortDirections;
};

}

#endif
