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

#ifndef COMMONTABLENODE_H
#define COMMONTABLENODE_H

#include <string>

#include "plannodes/abstractplannode.h"

namespace voltdb {

/**
 * This plan node represents the semantics needed to evaluate common
 * table expressions, or CTEs, including recursive CTEs.
 *
 * Example:
 *
 * WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS (
 *   SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME
 *     FROM EMPLOYEES
 *     WHERE MANAGER_ID IS NULL
 *   UNION ALL
 *   SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || ‘/’ || E.LAST_NAME
 *     FROM EMPLOYEES E JOIN EMP_PATH EP ON E.MANAGER_ID = EP.EMP_ID
 * )
 * SELECT * FROM EMP_PATH;
 *
 * The structure of a plan for a statement like this is as follows:
 * The main query (the last "SELECT *" statement in the example above)
 * will contain one or more SeqScanNodes that are CTE scans, and those
 * nodes will contain a "stmt ID" for a subplan that has this node at
 * the root.  When the subplan rooted at this node is executed, it
 * will produce the common table, which can be referenced multiple
 * times in the main statement.

 * The child of this node is the "base query", i.e., the LHS of the
 * UNION ALL in the example above.
 *
 * In the case of recursive common table expressions (the example
 * above is one), there will be a separate statement that is the
 * recursive part of the CTE (this is the RHS of the UNION ALL in the
 * example).  The stmt ID of the recursive statement is an attribute
 * of this node.
 *
 * The recursive part of the query (which references the CTE, EMP_PATH
 * in the example) will initially see EMP_PATH as containing the
 * output of the base query, and is then executed repeatedly, with
 * EMP_PATH containing the result of the previous iteration, until no
 * more rows are produced.
 */
class CommonTablePlanNode : public AbstractPlanNode {
public:
    virtual PlanNodeType getPlanNodeType() const {
        return PlanNodeType::CommonTable;
    }

    virtual void loadFromJSONObject(PlannerDomValue obj);

    virtual std::string debugInfo(const std::string& spacer) const;

    /**
     * Return the statement ID for the recursive part of the CTE.
     * Will be -1 if not a recursive CTE.
     */
    int getRecursiveStmtId() const {
        return m_recursiveStmtId;
    }

    /**
     * The name of the CTE which may be referenced in the main query.
     */
    std::string getCommonTableName() const {
        return m_commonTableName;
    }

private:
    int m_recursiveStmtId;
    std::string m_commonTableName;
};

} // end namespace voltdb

#endif // COMMOMTABLENODE_H
