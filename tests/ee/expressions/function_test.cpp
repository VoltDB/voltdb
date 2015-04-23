/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
#include <iostream>
#include <limits.h>
#include "harness.h"

#include "expressions/abstractexpression.h"
#include "expressions/expressions.h"
#include "common/common.h"
#include "common/valuevector.h"
#include "common/ValueFactory.hpp"
#include "common/types.h"
#include "common/ValuePeeker.hpp"
#include "common/PlannerDomValue.h"
#include "common/NValue.hpp"
#include "expressions/expressions.h"
#include "expressions/expressionutil.h"
#include "expressions/functionexpression.h"
#include "expressions/constantvalueexpression.h"

using namespace voltdb;

struct FunctionTest : public Test {
        FunctionTest() :
                Test(),
                m_executorContext(new ExecutorContext(0,
                                                      0,
                                                      (UndoQuantum *)0,
                                                      (Topend *)0,
                                                      new Pool(),
                                                      (VoltDBEngine *)0,
                                                      "localhost",
                                                      0,
                                                      (DRTupleStream *)0,
                                                      (DRTupleStream *)0)) {}
        ~FunctionTest() {
                delete m_executorContext;
        }
        ExecutorContext *m_executorContext;
};

static int testBin(uint64_t input, std::string output) {
        std::vector<AbstractExpression *> *argument = new std::vector<AbstractExpression *>();
        ConstantValueExpression *const_val_exp = new ConstantValueExpression(ValueFactory::getBigIntValue(0xffULL));
        argument->push_back(const_val_exp);
        NValue expected = ValueFactory::getTempStringValue("11111111");
    AbstractExpression* bin_exp = ExpressionUtil::functionFactory(FUNC_VOLT_BIN, argument);
    NValue answer = bin_exp->eval();
    int cmpout = answer.compare(expected);
    expected.free();
    delete bin_exp;
    // std::cout << "input: " << std::hex << input << " vs. " << output << " == " << cmpout << "\n";
    return cmpout;
}

TEST_F(FunctionTest, BinTest) {
        ASSERT_EQ(testBin(0xffULL, "11111111"), 0);
        ASSERT_EQ(testBin(0ULL, "0"), 0);
        const size_t BIGINT_SIZE = sizeof(uint64_t) * CHAR_BIT;
        // Walking ones.
        std::string expected("1");
        std::string expectedz("1111111111111111111111111111111111111111111111111111111111111111");
        for (int idx = 0; idx < (BIGINT_SIZE-1); idx += 1) {
                uint64_t input = 1ULL << idx;
                ASSERT_EQ(testBin(input, expected), 0);
                expected = expected + "0";
                expectedz[idx] = '0';
                ASSERT_EQ(testBin(~input, expectedz), 0);
                expectedz[idx] = '1';
        }
}

int main() {
     return TestSuite::globalInstance()->runAll();
}
