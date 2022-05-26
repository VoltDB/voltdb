/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#ifndef SUBQUERYCONTEXT_H_
#define SUBQUERYCONTEXT_H_

#include <vector>

#include "common/NValue.hpp"

namespace voltdb {

/*
* Keep track of the actual parameter values coming into a subquery invocation
* and if they have not changed since last invocation reuses the cached result
* from the prior invocation.
* This approach has several interesting effects:
* -- non-correlated subqueries are always executed once
* -- subquery filters that had to be applied after a join but that were only correlated
*    by columns from the join's OUTER side would effectively get run once per OUTER row.
* -- subqueries that were correlated by a parent's indexed column (producing ordered values)
*    could get executed once per unique value.
* The subquery context is registered with the global executor context as candidates for
* post-fragment cleanup, allowing results to be retained between invocations.
*/
struct SubqueryContext {
    SubqueryContext(std::vector<NValue> lastParams)
      : m_hasValidResult(false)
      , m_lastParams(lastParams)
    { }

    SubqueryContext(const SubqueryContext& other)
      : m_hasValidResult(other.m_hasValidResult)
      , m_lastParams(other.m_lastParams)
    {
        if (m_hasValidResult) {
            m_lastResult = other.m_lastResult;
        }
    }

    bool hasValidResult() const { return m_hasValidResult; }
    void invalidateResult() { m_hasValidResult = false; }

    const NValue& getResult() const { return m_lastResult; }

    void setResult(const NValue& result)
    {
        m_lastResult = result.copyNValue();
        m_hasValidResult = true;
    }

    std::vector<NValue>& accessLastParams() { return m_lastParams; }

private:
    bool m_hasValidResult;
    NValue m_lastResult;
    // The parameter values that were used to obtain the last result in the ascending
    // order of the parameter indexes
    std::vector<NValue> m_lastParams;
};

}
#endif
