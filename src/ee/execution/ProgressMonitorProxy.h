/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
    ProgressMonitorProxy(VoltDBEngine* engine, AbstractExecutor* exec, Table* target_table = NULL)
        : m_engine(engine)
        , m_tuplesRemainingUntilReport(engine->pullTuplesRemainingUntilProgressReport(exec, target_table))
        , m_countDown(m_tuplesRemainingUntilReport)
    { }

    void countdownProgress()
    {
        if (--m_countDown == 0) {
            m_tuplesRemainingUntilReport =
                m_engine->pushTuplesProcessedForProgressMonitoring(m_tuplesRemainingUntilReport);
            m_countDown = m_tuplesRemainingUntilReport;
        }
    }

    ~ProgressMonitorProxy()
    {
        // Report progress against next target
        m_engine->pushFinalTuplesProcessedForProgressMonitoring(m_tuplesRemainingUntilReport - m_countDown);
    }

private:
    VoltDBEngine* const m_engine;
    int64_t m_tuplesRemainingUntilReport;
    int64_t m_countDown;
};

} // namespace voltdb

#endif // PROGRESSMONITORPROXY_H
