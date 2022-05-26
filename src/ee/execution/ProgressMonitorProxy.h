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

#ifndef PROGRESSMONITORPROXY_H
#define PROGRESSMONITORPROXY_H

#include "common/executorcontext.hpp"

namespace voltdb {

class AbstractExecutor;

class ProgressMonitorProxy {
public:
    ProgressMonitorProxy(ExecutorContext* executorContext, AbstractExecutor* exec);

    void countdownProgress()
    {
        if (--m_countDown <= 0) {
            m_tuplesRemainingUntilReport =
                m_executorContext->pushTuplesProcessedForProgressMonitoring(m_limits,
                                                                            m_tuplesRemainingUntilReport);
            m_countDown = m_tuplesRemainingUntilReport;
        }
    }

    ~ProgressMonitorProxy()
    {
        // Report progress against next target
        m_executorContext->pushFinalTuplesProcessedForProgressMonitoring(m_limits,
                                                                m_tuplesRemainingUntilReport - m_countDown);
    }

private:
    ExecutorContext* const m_executorContext;
    const TempTableLimits * m_limits;
    int64_t m_tuplesRemainingUntilReport;
    int64_t m_countDown;
};

} // namespace voltdb

#endif // PROGRESSMONITORPROXY_H
