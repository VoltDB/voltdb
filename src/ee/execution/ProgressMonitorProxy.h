/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "execution/VoltDBEngine.h"

namespace voltdb {

class ProgressMonitorProxy {
public:
    ProgressMonitorProxy(VoltDBEngine* engine, AbstractExecutor* exec)
        : m_engine(engine)
        , m_limits(NULL)
        , m_tuplesRemainingUntilReport(
              engine->pullTuplesRemainingUntilProgressReport(exec->getPlanNode()->getPlanNodeType()))
        , m_countDown(m_tuplesRemainingUntilReport)
    {
        const TempTable *tt = exec->getTempOutputTable();
        if (tt != NULL) {
            m_limits = tt->getTempTableLimits();
        }
    }

    void countdownProgress()
    {
        if (--m_countDown == 0) {
            m_tuplesRemainingUntilReport =
                m_engine->pushTuplesProcessedForProgressMonitoring(m_limits,
                                                                   m_tuplesRemainingUntilReport);
            m_countDown = m_tuplesRemainingUntilReport;
        }
    }

    ~ProgressMonitorProxy()
    {
        // Report progress against next target
        m_engine->pushFinalTuplesProcessedForProgressMonitoring(m_limits,
                                                                m_tuplesRemainingUntilReport - m_countDown);
    }

private:
    VoltDBEngine* const m_engine;
    const TempTableLimits * m_limits;
    int64_t m_tuplesRemainingUntilReport;
    int64_t m_countDown;
};

} // namespace voltdb

#endif // PROGRESSMONITORPROXY_H
