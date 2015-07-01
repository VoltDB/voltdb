/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "storage/TempTableLimits.h"

#include "harness.h"
#include "common/SQLException.h"
#include "logging/LogManager.h"

#include <sstream>

using namespace voltdb;

class TestProxy : public LogProxy
{
public:
    LoggerId lastLoggerId;
    LogLevel lastLogLevel;
    char *lastStatement;
    /**
     * Log a statement on behalf of the specified logger at the specified log level
     * @param LoggerId ID of the logger that received this statement
     * @param level Log level of the statement
     * @param statement null terminated UTF-8 string containing the statement to log
     */
    virtual void log(LoggerId loggerId, LogLevel level,
                     const char *statement) const {
        //cout << "Logged.  ID: " << loggerId << ", level: " << level << ", statement: " << statement << endl;
        const_cast<TestProxy*>(this)->lastLoggerId = loggerId;
        const_cast<TestProxy*>(this)->lastLogLevel = level;
        const_cast<TestProxy*>(this)->lastStatement = const_cast<char*>(statement);
    };

    void reset()
    {
        lastLoggerId = LOGGERID_INVALID;
        lastLogLevel = LOGLEVEL_OFF;
        lastStatement = NULL;
    }
};

class TempTableLimitsTest : public Test
{
public:
    TempTableLimitsTest() : m_logManager(new TestProxy())
    {
        m_logManager.setLogLevels(0);
    }

    LogManager m_logManager;
};

TEST_F(TempTableLimitsTest, CheckLogLatch)
{
    TestProxy* proxy = dynamic_cast<TestProxy*>(const_cast<LogProxy*>(m_logManager.getLogProxy()));
    proxy->reset();

    TempTableLimits dut(1024 * 10, 1024 * 5); // Set 10K hard limit, 5K warn level
    // check that bump over threshold gets us logged
    dut.increaseAllocated(1024 * 6);
    EXPECT_EQ(proxy->lastLoggerId, LOGGERID_SQL);
    EXPECT_EQ(proxy->lastLogLevel, LOGLEVEL_INFO);
    proxy->reset();
    // next bump still over does not, however
    dut.increaseAllocated(1024);
    EXPECT_EQ(proxy->lastLoggerId, LOGGERID_INVALID);
    EXPECT_EQ(proxy->lastLogLevel, LOGLEVEL_OFF);
    proxy->reset();
    // dip below and back up, get new log
    dut.reduceAllocated(1024 * 3);
    dut.increaseAllocated(1024 * 2);
    EXPECT_EQ(proxy->lastLoggerId, LOGGERID_SQL);
    EXPECT_EQ(proxy->lastLogLevel, LOGLEVEL_INFO);
    proxy->reset();
}

TEST_F(TempTableLimitsTest, CheckLimitException)
{
    TestProxy* proxy = dynamic_cast<TestProxy*>(const_cast<LogProxy*>(m_logManager.getLogProxy()));
    proxy->reset();

    TempTableLimits dut(1024 * 10); // Set 10K hard limit, but no warn level.
    dut.increaseAllocated(1024 * 6);
    bool threw = false;
    try {
        dut.increaseAllocated(1024 * 6);
    }
    catch (SQLException& sqle) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    // no logging with -1 threshold
    EXPECT_EQ(proxy->lastLoggerId, LOGGERID_INVALID);
    EXPECT_EQ(proxy->lastLogLevel, LOGLEVEL_OFF);
    proxy->reset();
    // And check that we can dip below and rethrow
    dut.reduceAllocated(1024 * 6);
    threw = false;
    try {
        dut.increaseAllocated(1024 * 6);
    }
    catch (SQLException& sqle) {
        threw = true;
    }
    EXPECT_TRUE(threw);
}

int main()
{
    return TestSuite::globalInstance()->runAll();
}
