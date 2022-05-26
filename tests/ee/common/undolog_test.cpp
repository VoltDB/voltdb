/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include "harness.h"
#include "common/UndoReleaseAction.h"
#include "common/UndoLog.h"
#include "common/UndoQuantum.h"
#include "common/Pool.hpp"
#include <vector>
#include <stdint.h>

static int staticReleaseIndex = 0;
static int staticUndoneIndex = 0;

class MockUndoActionHistory {
public:
    MockUndoActionHistory() : m_released(false), m_undone(false), m_releasedIndex(-1), m_undoneIndex(-1) {}
    bool m_released;
    bool m_undone;
    int m_releasedIndex;
    int m_undoneIndex;
};

class MockUndoAction : public voltdb::UndoReleaseAction {
public:
    MockUndoAction(MockUndoActionHistory *history) : m_history(history) {}

    void undo() {
        m_history->m_undone = true;
        m_history->m_undoneIndex = staticUndoneIndex++;
    }

    void release() {
        m_history->m_released = true;
        m_history->m_releasedIndex = staticReleaseIndex++;
    }

    bool isReplicatedTable() {
        return false;
    }
private:
    MockUndoActionHistory *m_history;
};

class UndoLogTest : public Test {
public:

    UndoLogTest() {
        m_undoLog = new voltdb::UndoLog();
        staticReleaseIndex = 0;
        staticUndoneIndex = 0;
    }

    std::vector<int64_t> generateQuantumsAndActions(int numUndoQuantums, int numUndoActions) {
        std::vector<int64_t> undoTokens;
        for (int ii = 0; ii < numUndoQuantums; ii++) {
            const int64_t undoToken = (INT64_MIN + 1) + (ii * 3);
            undoTokens.push_back(undoToken);
            voltdb::UndoQuantum *quantum = m_undoLog->generateUndoQuantum(undoToken);
            std::vector<MockUndoActionHistory*> histories;
            for (int qq = 0; qq < numUndoActions; qq++) {
                MockUndoActionHistory *history = new MockUndoActionHistory();
                histories.push_back(history);
                quantum->registerUndoAction(new (*quantum) MockUndoAction(history));
            }
            m_undoActionHistoryByQuantum.push_back(histories);
        }
        return undoTokens;
    }

    ~UndoLogTest() {
        delete m_undoLog;
        for(std::vector<std::vector<MockUndoActionHistory*> >::iterator i = m_undoActionHistoryByQuantum.begin();
            i != m_undoActionHistoryByQuantum.end(); i++) {
            for(std::vector<MockUndoActionHistory*>::iterator q = (*i).begin();
                q != (*i).end(); q++) {
                delete (*q);
            }
        }
    }

    /*
     * Confirm all actions were undone in FILO order.
     */
    void confirmUndoneActionHistoryOrder(std::vector<MockUndoActionHistory*> histories, int &expectedStartingIndex) {
        for (std::vector<MockUndoActionHistory*>::reverse_iterator i = histories.rbegin();
             i != histories.rend();
             i++) {
            const MockUndoActionHistory *history = *i;
            ASSERT_TRUE(history->m_undone);
            ASSERT_FALSE(history->m_released);
            ASSERT_EQ(history->m_undoneIndex, expectedStartingIndex);
            expectedStartingIndex++;
        }
    }

    /*
     * Confirm all actions were released in FIFO order.
     */
    void confirmReleaseActionHistoryOrder(std::vector<MockUndoActionHistory*> histories, int &expectedStartingIndex) {
        for (std::vector<MockUndoActionHistory*>::iterator i = histories.begin();
             i != histories.end();
             i++) {
            const MockUndoActionHistory *history = *i;
            ASSERT_TRUE(history->m_released);
            ASSERT_FALSE(history->m_undone);
            ASSERT_EQ(history->m_releasedIndex, expectedStartingIndex);
            expectedStartingIndex++;
        }
    }

    voltdb::UndoLog *m_undoLog;
    std::vector<std::vector<MockUndoActionHistory*> > m_undoActionHistoryByQuantum;
};

/*
 * A series of tests to make sure the UndoLog and such can construct and destruct successfully without leaking memory.
 */
TEST_F(UndoLogTest, TestZeroQuantumZeroAction) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 0, 0);
    ASSERT_EQ( 0, undoTokens.size());
}

TEST_F(UndoLogTest, TestOneQuantumZeroAction) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 1, 0);
    ASSERT_EQ( 1, undoTokens.size());
}

TEST_F(UndoLogTest, TestOneQuantumOneAction) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 1, 1);
    ASSERT_EQ( 1, undoTokens.size());
}

TEST_F(UndoLogTest, TestOneQuantumTenAction) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 1, 10);
    ASSERT_EQ( 1, undoTokens.size());
}

TEST_F(UndoLogTest, TestTenQuantumOneAction) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 10, 1);
    ASSERT_EQ( 10, undoTokens.size());
}

TEST_F(UndoLogTest, TestTenQuantumTenAction) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 10, 10);
    ASSERT_EQ( 10, undoTokens.size());
}

TEST_F(UndoLogTest, TestOneQuantumOneActionRelease) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 1, 1);
    ASSERT_EQ( 1, undoTokens.size());

    m_undoLog->release(undoTokens[0]);
    const MockUndoActionHistory *undoActionHistory = m_undoActionHistoryByQuantum[0][0];
    ASSERT_TRUE(undoActionHistory->m_released);
    ASSERT_FALSE(undoActionHistory->m_undone);

    ASSERT_EQ(undoActionHistory->m_releasedIndex, 0);
}

TEST_F(UndoLogTest, TestOneQuantumOneActionUndo) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 1, 1);
    ASSERT_EQ( 1, undoTokens.size());

    m_undoLog->undo(undoTokens[0]);
    const MockUndoActionHistory *undoActionHistory = m_undoActionHistoryByQuantum[0][0];
    ASSERT_FALSE(undoActionHistory->m_released);
    ASSERT_TRUE(undoActionHistory->m_undone);

    ASSERT_EQ(undoActionHistory->m_undoneIndex, 0);
}

/*
 * Check that the three actions are undone in the correct reverse order for a single quantum.
 */
TEST_F(UndoLogTest, TestOneQuantumThreeActionUndoOrdering) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 1, 3);
    ASSERT_EQ( 1, undoTokens.size());

    m_undoLog->undo(undoTokens[0]);
    std::vector<MockUndoActionHistory*> histories = m_undoActionHistoryByQuantum[0];
    int startingIndex = 0;
    confirmUndoneActionHistoryOrder(histories, startingIndex);
}

TEST_F(UndoLogTest, TestOneQuantumThreeActionReleaseOrdering) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 1, 3);
    ASSERT_EQ( 1, undoTokens.size());

    m_undoLog->release(undoTokens[0]);
    std::vector<MockUndoActionHistory*> histories = m_undoActionHistoryByQuantum[0];
    int startingIndex = 0;
    confirmReleaseActionHistoryOrder(histories, startingIndex);
}

/*
 * Now do the same for three quantums.
 */
TEST_F(UndoLogTest, TestThreeQuantumThreeActionUndoOrdering) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 3, 3);
    ASSERT_EQ( 3, undoTokens.size());

    m_undoLog->undo(undoTokens[0]);
    int startingIndex = 0;
    for (int ii = 2; ii >= 0; ii--) {
        confirmUndoneActionHistoryOrder(m_undoActionHistoryByQuantum[ii], startingIndex);
    }
}

/*
 * The order of releasing doesn't really matter unlike undo, assume it goes forward through the quantums
 */
TEST_F(UndoLogTest, TestThreeQuantumThreeActionReleaseOrdering) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 3, 3);
    ASSERT_EQ( 3, undoTokens.size());

    m_undoLog->release(undoTokens[2]);
    int startingIndex = 0;
    for (int ii = 0; ii < 3; ii++) {
        confirmReleaseActionHistoryOrder(m_undoActionHistoryByQuantum[ii], startingIndex);
    }
}

TEST_F(UndoLogTest, TestThreeQuantumThreeActionReleaseOneUndoOne) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 3, 3);
    ASSERT_EQ( 3, undoTokens.size());

    m_undoLog->release(undoTokens[0]);
    int startingIndex = 0;
    confirmReleaseActionHistoryOrder(m_undoActionHistoryByQuantum[0], startingIndex);

    m_undoLog->undo(undoTokens[2]);
    startingIndex = 0;
    confirmUndoneActionHistoryOrder(m_undoActionHistoryByQuantum[2], startingIndex);
}

TEST_F(UndoLogTest, TestThreeQuantumThreeActionUndoOneReleaseOne) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 3, 3);
    ASSERT_EQ( 3, undoTokens.size());

    m_undoLog->undo(undoTokens[2]);
    int startingIndex = 0;
    confirmUndoneActionHistoryOrder(m_undoActionHistoryByQuantum[2], startingIndex);

    m_undoLog->release(undoTokens[0]);
    startingIndex = 0;
    confirmReleaseActionHistoryOrder(m_undoActionHistoryByQuantum[0], startingIndex);
}

TEST_F(UndoLogTest, TestTwoQuantumTwoActionReleaseOneUndoOne) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 2, 2);
    ASSERT_EQ( 2, undoTokens.size());

    m_undoLog->release(undoTokens[0]);
    int startingIndex = 0;
    confirmReleaseActionHistoryOrder(m_undoActionHistoryByQuantum[0], startingIndex);

    m_undoLog->undo(undoTokens[1]);
    startingIndex = 0;
    confirmUndoneActionHistoryOrder(m_undoActionHistoryByQuantum[1], startingIndex);
}

TEST_F(UndoLogTest, TestTwoQuantumTwoActionUndoOneReleaseOne) {
    std::vector<int64_t> undoTokens = generateQuantumsAndActions( 2, 2);
    ASSERT_EQ( 2, undoTokens.size());

    m_undoLog->undo(undoTokens[1]);
    int startingIndex = 0;
    confirmUndoneActionHistoryOrder(m_undoActionHistoryByQuantum[1], startingIndex);

    m_undoLog->release(undoTokens[0]);
    startingIndex = 0;
    confirmReleaseActionHistoryOrder(m_undoActionHistoryByQuantum[0], startingIndex);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
