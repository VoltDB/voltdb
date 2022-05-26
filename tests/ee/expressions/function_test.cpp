/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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
#include <limits>
#include <sstream>
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
#include "common/SQLException.h"
#include "expressions/expressions.h"
#include "expressions/expressionutil.h"
#include "expressions/functionexpression.h"
#include "expressions/constantvalueexpression.h"

using namespace voltdb;

static bool staticVerboseFlag = false;

namespace {
bool findString(const std::string &string, std::string pattern) {
    std::string::size_type found = string.find(pattern);
    return (found != std::string::npos);
}
}

struct FunctionTest : public Test {
        FunctionTest() :
                Test(),
                m_pool(),
                m_executorContext(0,
                                  0,
                                  (UndoQuantum *)0,
                                  (Topend *)0,
                                  &m_pool,
                                  (VoltDBEngine *)0,
                                  "localhost",
                                  0,
                                  (AbstractDRTupleStream *)0,
                                  (AbstractDRTupleStream *)0,
                                  0) {}

        /**
         * A template for calling nullary function call expressions.
         */
        template <typename OUTPUT_TYPE>
        int testNullary(int operation, OUTPUT_TYPE, bool expect_null = false);
        /**
         * A template for calling unary function call expressions.  For any C++
         * type T, define the function "NValue getSomeValue(T val)" to
         * convert the T value to an NValue below.
         */
        template <typename INPUT_TYPE, typename OUTPUT_TYPE>
        int testUnary(int operation, INPUT_TYPE input, OUTPUT_TYPE output, bool expect_null = false);

        /**
         * Similar to the above function, but returns "success" if the function threw a
         * SQLException with the given input.
         */
        template <typename INPUT_TYPE>
        std::string testUnaryThrows(int operation, INPUT_TYPE input, const std::string& expectedMessage);

        /**
         * A template for calling binary function call expressions.  For any C++
         * type T, define the function "NValue getSomeValue(T val)" to
         * convert the T value to an NValue below.
         */
        template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE, typename OUTPUT_TYPE>
        int testBinary(int operation, LEFT_INPUT_TYPE left_input, RIGHT_INPUT_TYPE right_input, OUTPUT_TYPE output, bool expect_null = false);

        /**
         * Similar to the above function, but returns "success" if the function threw a
         * SQLException with the given inputs.
         */
        template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE>
        std::string testBinaryThrows(int operation, LEFT_INPUT_TYPE left_input, RIGHT_INPUT_TYPE right_input, const std::string& expectedMessage);
        template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE>
        std::string testBinaryThrows(int operation, LEFT_INPUT_TYPE left_input, RIGHT_INPUT_TYPE right_input, const std::string& expectedMessage1,
              const std::string& expectedMessage2);

        /**
         * A template for calling ternary functions.  This follows the pattern
         * of testUnary and testBinary.
         */
        template <typename LEFT_INPUT_TYPE, typename MIDDLE_INPUT_TYPE, typename RIGHT_INPUT_TYPE, typename OUTPUT_TYPE>
        int testTernary(int operation, LEFT_INPUT_TYPE left_input, MIDDLE_INPUT_TYPE middle_input, RIGHT_INPUT_TYPE right_input, OUTPUT_TYPE output, bool expect_null = false);

        static const int64_t BIGINT_SIZE = int64_t(sizeof(int64_t) * CHAR_BIT);
private:
        Pool            m_pool;
        ExecutorContext m_executorContext;
};

static NValue getSomeValue(const char* &val)
{
    return ValueFactory::getTempStringValue(std::string(val));
}

static NValue getSomeValue(const std::string &val)
{
    return ValueFactory::getTempStringValue(val);
}

static NValue getSomeValue(const int64_t val)
{
    return ValueFactory::getBigIntValue(val);
}

static NValue getSomeValue(const TTInt &val)
{
    return ValueFactory::getDecimalValueFromString(val.ToString());
}

/**
 * C++ thinks bool is a kind of integer.  This means we can't really
 * force it to disambiguate between "getSomeValue(False)" and
 * "getSomeValue(100)" using only types.  This is logically unneeded,
 * but it's pretty useful.
 */
enum Boolean {
    False = 0,
    True  = 1
};

static NValue getSomeValue(const Boolean value)
{
    return ValueFactory::getBooleanValue(static_cast<bool>(value));
}

static NValue getSomeValue(const in6_addr *addr)
{
    return ValueFactory::getTempBinaryValue((const char *)addr, sizeof(in6_addr));
}

static NValue& getSomeValue(NValue &val)
{
    return val;
}

// So that NValues can be dumped to stdout when
// staticVerboseFlag is enabled below...
std::ostream& operator<<(std::ostream& os, const NValue& val)
{
    os << val.debug();
    return os;
}

/**
 * Test a nullary function call.
 * @returns: -1 if the result of the function evaluation is less than the expected result.
 *            0 if the result of the function evaluation is as expected.
 *            1 if the result of the function evaluation is greater than the expected result.
 * Note that this function may throw an exception from the call to AbstractExpression::eval().
 */
template <typename OUTPUT_TYPE>
int FunctionTest::testNullary(int operation,
                              OUTPUT_TYPE output,
                              bool expect_null) {
    if (staticVerboseFlag) {
        std::cout << "\n *** *** ***\n";
        std::cout << "operation:     " << operation << std::endl;
        std::cout << "Expected out:  " << output << std::endl;
    }
    // This is an empty vector, but functionFactory wants it.
    std::vector<AbstractExpression*> argument;
    AbstractExpression* const_exp = functionFactory(operation, argument);
    int cmpout;
    NValue expected = getSomeValue(output);
    NValue answer;
    try {
        answer = const_exp->eval();
        if (expect_null) {
            // An unexpected non-null can return any non-0. Arbitrarily return 1 as if (answer > expected).
            cmpout = answer.isNull() ? 0 : 1;
        } else {
            cmpout = answer.compare(expected);
        }
    } catch (SQLException &ex) {
        delete const_exp;
        expected.free();
        throw;
    }
    if (staticVerboseFlag) {
        std::cout << ", answer: \"" << answer.debug() << "\""
                  << ", expected: \"" << (expect_null ? "<NULL>" : expected.debug()) << "\""
                  << ", comp:     " << std::dec << cmpout << std::endl;
    }
    delete const_exp;
    expected.free();
    return cmpout;
}

/**
 * Test a unary function call expression.
 * @returns: -1 if the result of the function evaluation is less than the expected result.
 *            0 if the result of the function evaluation is as expected.
 *            1 if the result of the function evaluation is greater than the expected result.
 * Note that this function may throw an exception from the call to AbstractExpression::eval().
 */
template <typename INPUT_TYPE, typename OUTPUT_TYPE>
int FunctionTest::testUnary(int operation, INPUT_TYPE input, OUTPUT_TYPE output, bool expect_null) {
    if (staticVerboseFlag) {
        std::cout << "\n *** *** ***\n";
        std::cout << "operation:     " << operation << std::endl;
        std::cout << "Operand:       " << input << std::endl;

        std::cout << "Expected out:  " << output << std::endl;
    }
    std::vector<AbstractExpression *> argument;
    ConstantValueExpression *const_val_exp = new ConstantValueExpression(getSomeValue(input));
    argument.push_back(const_val_exp);
    AbstractExpression* unary_exp = functionFactory(operation, argument);
    int cmpout;
    NValue expected = getSomeValue(output);
    NValue answer;
    try {
        answer = unary_exp->eval();
        if (expect_null) {
            // An unexpected non-null can return any non-0. Arbitrarily return 1 as if (answer > expected).
            cmpout = answer.isNull() ? 0 : 1;
        } else {
            cmpout = answer.compare(expected);
        }
    } catch (SQLException &ex) {
        delete unary_exp;
        expected.free();
        throw;
    }
    if (staticVerboseFlag) {
        std::cout << "input: " << std::hex << input
                  << ", answer: \"" << answer.debug() << "\""
                  << ", expected: \"" << (expect_null ? "<NULL>" : expected.debug()) << "\""
                  << ", comp:     " << std::dec << cmpout << std::endl;
    }
    delete unary_exp;
    expected.free();
    return cmpout;
}

template <typename INPUT_TYPE>
std::string FunctionTest::testUnaryThrows(int operation,
                                          INPUT_TYPE input,
                                          const std::string& expectedMessage) {
    std::string diagnostic = "success";
    try {
        testUnary(operation, input, -1);
        diagnostic = "Failed to throw an exception";
    }
    catch (const SQLException& exc) {
        if (exc.message().find(expectedMessage) == std::string::npos) {
            diagnostic = "Expected message \"" + expectedMessage + "\", but found \"" +
                exc.message() + "\"";
        }
    }
    catch (...) {
        diagnostic = "Caught some unexpected kind of exception";
    }

    if (diagnostic.compare("success") != 0) {
        std::cerr << "\n***  " << diagnostic << "  ***\n";
    }

    return diagnostic;
}


/**
 * Test a binary function call expression.
 * @returns: -1 if the result of the function evaluation is less than the expected result.
 *            0 if the result of the function evaluation is as expected.
 *            1 if the result of the function evaluation is greater than the expected result.
 * Note that this function may throw an exception from the call to AbstractExpression::eval().
 */
template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE, typename OUTPUT_TYPE>
int FunctionTest::testBinary(int operation, LEFT_INPUT_TYPE linput, RIGHT_INPUT_TYPE rinput, OUTPUT_TYPE output, bool expect_null) {
    if (staticVerboseFlag) {
        std::cout << "operation:     " << operation << std::endl;
        std::cout << "Left:          " << linput << std::endl;
        std::cout << "Right:         " << rinput << std::endl;
        std::cout << "Expected out:  " << output << std::endl;
    }
    std::vector<AbstractExpression *>argument;
    ConstantValueExpression *lhsexp = new ConstantValueExpression(getSomeValue(linput));
    ConstantValueExpression *rhsexp = new ConstantValueExpression(getSomeValue(rinput));
    argument.push_back(lhsexp);
    argument.push_back(rhsexp);

    NValue expected = getSomeValue(output);
    AbstractExpression *bin_exp = functionFactory(operation, argument);
    int cmpout;
    NValue answer;
    try {
        answer = bin_exp->eval();
        if (expect_null) {
            // An unexpected non-null can return any non-0. Arbitrarily return 1 as if (answer > expected).
            cmpout = answer.isNull() ? 0 : 1;
        } else {
            cmpout = answer.compare(expected);
        }
    } catch (SQLException &ex) {
        expected.free();
        delete bin_exp;
        throw;
    }
    if (staticVerboseFlag) {
        std::cout << std::hex << "input: test(" << linput << ", " << rinput << ")"
                  << ", answer: \"" << answer.debug() << "\""
                  << ", expected: \"" << (expect_null ? "<NULL>" : expected.debug()) << "\""
                  << ", comp:     " << std::dec << cmpout << std::endl;
    }
    expected.free();
    delete bin_exp;
    return cmpout;
}

template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE>
std::string FunctionTest::testBinaryThrows(
      int operation, LEFT_INPUT_TYPE left_input, RIGHT_INPUT_TYPE right_input,
      const std::string& expectedMessage) {
    std::string diagnostic = "success";
    try {
        testBinary(operation, left_input, right_input, -1);
        diagnostic = "Failed to throw an exception";
    }
    catch (const SQLException& exc) {
        if (exc.message().find(expectedMessage) == std::string::npos) {
            diagnostic = "Expected message \"" + expectedMessage + "\", but found \"" +
                exc.message() + "\"";
        }
    }
    catch (...) {
        diagnostic = "Caught some unexpected kind of exception";
    }

    if (diagnostic.compare("success") != 0) {
        std::cerr << "\n***  " << diagnostic << "  ***\n";
    }

    return diagnostic;
}

template <typename LEFT_INPUT_TYPE, typename RIGHT_INPUT_TYPE>
std::string FunctionTest::testBinaryThrows(
      int operation, LEFT_INPUT_TYPE left_input, RIGHT_INPUT_TYPE right_input,
      const std::string& expectedMessage1, const std::string& expectedMessage2) {
    std::string diagnostic = "success";
    try {
        testBinary(operation, left_input, right_input, -1);
        diagnostic = "Failed to throw an exception";
    }
    catch (const SQLException& exc) {
        if (exc.message().find(expectedMessage1) == std::string::npos &&
              exc.message().find(expectedMessage2) == std::string::npos) {
            diagnostic = "Expected message \"" + expectedMessage1 + "\" or \"" + expectedMessage2 +"\", but found \"" +
                exc.message() + "\"";
        }
    }
    catch (...) {
        diagnostic = "Caught some unexpected kind of exception";
    }

    if (diagnostic.compare("success") != 0) {
        std::cerr << "\n***  " << diagnostic << "  ***\n";
    }

    return diagnostic;
}


/**
 * Test a ternary function call expression.
 * @returns: -1 if the result of the function evaluation is less than the expected result.
 *            0 if the result of the function evaluation is as expected.
 *            1 if the result of the function evaluation is greater than the expected result.
 * Note that this function may throw an exception from the call to AbstractExpression::eval().
 */
template <typename LEFT_INPUT_TYPE, typename MIDDLE_INPUT_TYPE, typename RIGHT_INPUT_TYPE, typename OUTPUT_TYPE>
int FunctionTest::testTernary(int operation, LEFT_INPUT_TYPE linput, MIDDLE_INPUT_TYPE minput, RIGHT_INPUT_TYPE rinput, OUTPUT_TYPE output, bool expect_null) {
    if (staticVerboseFlag) {
        std::cout << "operation:     " << operation << std::endl;
        std::cout << "Left:          " << linput << std::endl;
        std::cout << "Middle:        " << minput << std::endl;
        std::cout << "Right:         " << rinput << std::endl;
        std::cout << "Expected out:  " << output << std::endl;
    }
    std::vector<AbstractExpression *> argument;
    ConstantValueExpression *lhsexp = new ConstantValueExpression(getSomeValue(linput));
    ConstantValueExpression *mdlexp = new ConstantValueExpression(getSomeValue(minput));
    ConstantValueExpression *rhsexp = new ConstantValueExpression(getSomeValue(rinput));
    argument.push_back(lhsexp);
    argument.push_back(mdlexp);
    argument.push_back(rhsexp);

    NValue expected = getSomeValue(output);
    AbstractExpression *ternary_exp = functionFactory(operation, argument);
    int cmpout;
    NValue answer;
    try {
        answer = ternary_exp->eval();
        if (expect_null) {
            // An unexpected non-null can return any non-0. Arbitrarily return 1 as if (answer > expected).
            cmpout = answer.isNull() ? 0 : 1;
        } else {
            cmpout = answer.compare(expected);
        }
    } catch (SQLException &ex) {
        expected.free();
        delete ternary_exp;
        throw;
    }
    if (staticVerboseFlag) {
        std::cout << std::hex << "input: test(" << linput << ", " << minput << ", " << rinput << ")"
                  << ", answer: \"" << answer.debug() << "\""
                  << ", expected: \"" << (expect_null ? "<NULL>" : expected.debug()) << "\""
                  << ", comp:     " << std::dec << cmpout << std::endl;
    }
    expected.free();
    delete ternary_exp;
    return cmpout;
}

TEST_F(FunctionTest, BinTest) {
    ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                        0xffLL,
                        "11111111"),
              0);
    ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                        0x0LL,
                        "0"),
              0);
    ASSERT_EQ(testUnary(FUNC_VOLT_BIN, int64_t(0x8000000000000000), "", true), 0);

    // Walking ones.
    std::string expected("1");
    std::string expectedz("1111111111111111111111111111111111111111111111111111111111111111");
    for (int idx = 0; idx < (BIGINT_SIZE-1); idx += 1) {
            int64_t input = 1ULL << idx;
            ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                                input,
                                expected),
                      0);
            expected = expected + "0";
            expectedz[(BIGINT_SIZE-1) - idx] = '0';
            ASSERT_EQ(testUnary(FUNC_VOLT_BIN,
                                ~input,
                                expectedz),
                      0);
            expectedz[(BIGINT_SIZE-1) - idx] = '1';
    }
}

TEST_F(FunctionTest, NaturalLogTest) {
    ASSERT_EQ(testUnary(FUNC_LN, 1, 0),
              0);

    bool sawException = false;
    try {
        testUnary(FUNC_LN, -1, 0);
    } catch(SQLException &sqlExcp) {
        const std::string message = sqlExcp.message();
        sawException = findString(message, "Invalid result value (nan)")
                || findString(message, "Invalid result value (-nan)");
    }
    ASSERT_EQ(sawException, true);

    sawException = false;
    try {
        testUnary(FUNC_LN, 0, 0);
    } catch(SQLException &sqlExcp) {
    sawException = findString(sqlExcp.message(), "Invalid result value (-inf)");
    }
    ASSERT_EQ(sawException, true);
}

TEST_F(FunctionTest, NaturalLog10Test) {
    ASSERT_EQ(testUnary(FUNC_LOG10, 100, 2), 0);
    ASSERT_EQ(testUnary(FUNC_LOG10, 100.0, 2.0), 0);

    //invalid parameter value
    bool sawException = false;
    try {
        testUnary(FUNC_LOG10, -100, 0);
    } catch(SQLException &sqlExcp) {
    sawException = findString(sqlExcp.message(), "Invalid result value (nan)");
    }
    ASSERT_EQ(sawException, true);

    //invalid parameter value
    sawException = false;
    try {
        testUnary(FUNC_LOG10, -1, 0);
    } catch(SQLException &sqlExcp) {
    sawException = findString(sqlExcp.message(), "Invalid result value (nan)");
    }
    ASSERT_EQ(sawException, true);

    //invalid parameter type
    sawException = false;
    try {
        testUnary(FUNC_LOG10, "100", 0);
    } catch(SQLException &sqlExcp) {
    sawException = findString(sqlExcp.message(), "Type VARCHAR can't be cast as FLOAT");
    }
    ASSERT_EQ(sawException, true);
}

TEST_F(FunctionTest, NaturalModTest) {
    ASSERT_EQ(testBinary(FUNC_MOD, 2, 1, 0), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, 3, 2, 1), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, 0, 2, 0), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("3.0"), TTInt("2.0"), TTInt("1.000000000000")), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("-3.0"), TTInt("2.0"), TTInt("-1.000000000000")), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("3.0"), TTInt("-2.0"), TTInt("1.000000000000")), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("-3.0"), TTInt("-2.0"), TTInt("-1.000000000000")), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("25.2"), TTInt("7.4"), TTInt("4.000000000000")), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("25.2"), TTInt("-7.4"), TTInt("4.000000000000")), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("-25.2"), TTInt("-7.4"), TTInt("-4.000000000000")), 0);
    ASSERT_EQ(testBinary(FUNC_MOD, TTInt("-25.2"), TTInt("-7.4"), TTInt("-4.000000000000")), 0);

    //invalid parameter value
    bool sawException = false;
    try {
        testBinary(FUNC_MOD, "-100", 3, 1);
    } catch(SQLException &sqlExcp) {
    sawException = findString(sqlExcp.message(),
                                  "unsupported non-numeric type for SQL MOD function");
    }
    ASSERT_EQ(sawException, true);
}

TEST_F(FunctionTest, inet6NtoATest) {
    in6_addr addr;
    const char *addrStr = "ab01:cd02:ef03:1ef:2cd:3ab:a0b0:c0d";
    inet_pton(AF_INET6, addrStr, &addr);
    ASSERT_EQ(testUnary(FUNC_INET6_NTOA,
                        &addr,
                        addrStr),
              0);
}
TEST_F(FunctionTest, HexTest) {
        ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                            0xffLL,
                            "FF"),
                  0);
        ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                            0x0LL,
                            "0"),
                  0);
        ASSERT_EQ(testUnary(FUNC_VOLT_HEX, int64_t(0x8000000000000000),"", true),
                    0);
        // Walking ones.
        // Apparently it's unrecommended to reuse std::stringstream,
        // so we allocate ss and ssz both.
        for (int idx = 0; idx < (BIGINT_SIZE-1) ; idx += 1) {
                std::stringstream ss;
                int64_t input = 1ULL << idx;
                ss << std::hex << std::uppercase << input; // decimal_value
                ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                                    input,
                                    ss.str()),
                          0);
                std::stringstream ssz;
                ssz << std::hex << std::uppercase << ~input;
                ASSERT_EQ(testUnary(FUNC_VOLT_HEX,
                                    ~input,
                                    ssz.str()),
                          0);
        }
}

TEST_F(FunctionTest, BitAndTest) {
    const int64_t allones = 0xFFFFFFFFFFFFFFFFLL;
    const int64_t nullmarker = 0x8000000000000000LL;
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x0LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x0LL, 0x1LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x1LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, 0x1LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITAND, nullmarker, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all ones.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_BITAND, allones, (1<<idx), (1<<idx)), 0);
    }
}

TEST_F(FunctionTest, BitOrTest) {
    const int64_t allzeros = 0x0;
    const int64_t nullmarker = 0x8000000000000000LL;
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x0LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x1LL, 0x0LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x0LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, 0x1LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITOR, nullmarker, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_BITOR, allzeros, (1<<idx), (1<<idx)), 0);
    }
}

TEST_F(FunctionTest, BitXorTest) {
    const int64_t allzeros = 0x0LL;
    const int64_t allones = 0xFFFFFFFFFFFFFFFFLL;
    const int64_t nullmarker = 0x8000000000000000LL;
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x0LL, 0x0LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x1LL, 0x0LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x0LL, 0x1LL, 0x1LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, 0x1LL, 0x1LL, 0x0LL), 0);
    ASSERT_EQ(testBinary(FUNC_BITXOR, nullmarker, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_BITXOR, allzeros, (1<<idx), (1<<idx)), 0);
        ASSERT_EQ(testBinary(FUNC_BITXOR, allones, (1<<idx), (allones ^ (1 << idx))), 0);
    }
}

TEST_F(FunctionTest, BitLshTest) {
    const int64_t nullmarker = 0x8000000000000000LL;
    const int64_t one = 0x1LL;
    const int64_t three = 0x3LL;

    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 0,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 1,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, one,        nullmarker, 0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, one,        nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    // Don't put the bit all the way at the left end, though.
    for (int idx = 0; idx < BIGINT_SIZE-1; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, 0x1LL, idx, 0x1LL << idx), 0);
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, three, idx, three << idx), 0);
    }
    // Test shifting all the way to the right.
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, three, BIGINT_SIZE - 2, (three << (BIGINT_SIZE-2))), 0);
}

TEST_F(FunctionTest, BitRshTest) {
    const int64_t nullmarker = 0x8000000000000000LL;
    const int64_t maxleftbit = 0x4000000000000000LL;
    const int64_t three = 0x3LL;

    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 0,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, nullmarker, 1,          0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, maxleftbit, nullmarker, 0LL, true), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_LEFT, maxleftbit, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE-1; idx += 1) {
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_RIGHT, maxleftbit, idx, (maxleftbit >> idx)), 0);
        ASSERT_EQ(testBinary(FUNC_VOLT_BIT_SHIFT_RIGHT, (three << idx), idx, three), 0);
    }
}

TEST_F(FunctionTest, BitNotTest) {
    const int64_t nullmarker = 0x8000000000000000LL;

    ASSERT_EQ(testUnary(FUNC_VOLT_BITNOT, nullmarker, 0LL, true), 0);
    // Walk a one through a vector of all zeros.
    for (int idx = 0; idx < BIGINT_SIZE; idx += 1) {
        ASSERT_EQ(testUnary(FUNC_VOLT_BITNOT, (1<<idx), ~(1<<idx)), 0);
    }
}

TEST_F(FunctionTest, RepeatTooBig) {
    bool sawexception = false;
    try {
        ASSERT_EQ(testBinary(FUNC_REPEAT, "amanaplanacanalpanama", int64_t(1), "amanaplanacanalpanama", false), 0);
    } catch (voltdb::SQLException &ex) {
        sawexception = true;
    }
    ASSERT_FALSE(sawexception);
    sawexception = false;
    try {
        ASSERT_EQ(testBinary(FUNC_REPEAT, "amanaplanacanalpanama", 1000000, "", false), 1);
    } catch (voltdb::SQLException &ex) {
        sawexception = true;
    }
    ASSERT_TRUE(sawexception);
}

TEST_F(FunctionTest, RegularExpressionMatch) {
    bool sawexception = false;
    std::string testString("TEST reGexp_poSiTion123456Test");
    std::string testUTF8String("vVoltDBBB贾贾贾");
    ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, testString, std::string("TEST"), 1), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, testString, std::string("[a-z](\\d+)[a-z]"), 0), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, testString, std::string("[a-z](\\d+)[A-Z]"), 20), 0);
    ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testString, std::string("[a-z](\\d+)[a-z]"), "i", 20), 0);

    // Test an illegal pattern.
    sawexception = false;
    try {
        ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, testString, std::string("[a-z](a]"), 0), 0);
    } catch (voltdb::SQLException &ex) {
        sawexception = true;
    }
    ASSERT_TRUE(sawexception);

    // Test an illegal option character.
    sawexception = false;
    try {
        ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testString, "[a-z](\\d+)[A-Z]", "k", 0), 0);
    } catch (voltdb::SQLException &ex) {
        sawexception = true;
    }
    ASSERT_TRUE(sawexception);

#if 0
    const char *NULL_STRING = 0;
    // I'm not sure how to represent null strings here.  These are tested in
    // the junit tests, though.
    //
    // Test a null pattern.
    ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, testString, NULL_STRING, 0, true), 1);

    // Test a null subject.
    ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, NULL_STRING, "TEST", 0, true), 1);
#endif /* 0 */

     // Test utf-8 strings.
    ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[A-Z]贾", 9), 0);
    ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[A-Z]贾", "c", 9), 0);
    ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[A-Z]贾", "ic", 9), 0);
    ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[A-Z]贾", "ccciiiic", 9), 0);
    ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[a-z]贾", "i", 9), 0);
    ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[a-z]贾", "ci", 9), 0);
    ASSERT_EQ(testTernary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[a-z]贾", "iiccii", 9), 0);
    ASSERT_EQ(testBinary(FUNC_VOLT_REGEXP_POSITION, testUTF8String, "[a-z]家", 0), 0);
}

static NValue timestampFromString(const std::string& dateString) {
    return ValueFactory::getTimestampValue(NValue::parseTimestampString(dateString));
}

static const NValue nullTimestamp = ValueFactory::getTimestampValue(std::numeric_limits<int64_t>::min());
static const NValue minInt64 = ValueFactory::getTimestampValue(std::numeric_limits<int64_t>::min() + 1);
static const NValue tooSmallTimestamp = ValueFactory::getTimestampValue(GREGORIAN_EPOCH - 1);
static const NValue minValidTimestamp = ValueFactory::getTimestampValue(GREGORIAN_EPOCH);
static const NValue maxValidTimestamp = ValueFactory::getTimestampValue(NYE9999);
static const NValue tooBigTimestamp = ValueFactory::getTimestampValue(NYE9999 + 1);
static const NValue maxInt64 = ValueFactory::getTimestampValue(std::numeric_limits<int64_t>::max());

static std::string getInputOutOfRangeMessage(const std::string& func) {
    std::ostringstream oss;
    oss << "Input to SQL function " << func << " is outside of the supported range (years 1583 to 9999, inclusive).";
    return oss.str();
}

static std::string getOutputOutOfRangeMessage(const std::string& func) {
    std::ostringstream oss;
    oss << "SQL function " << func << " would produce a value outside of the supported range (years 1583 to 9999, inclusive).";
    return oss.str();
}


TEST_F(FunctionTest, DateFunctionsTruncate) {
    std::vector<int> funcs {
        FUNC_TRUNCATE_YEAR,
        FUNC_TRUNCATE_QUARTER,
        FUNC_TRUNCATE_MONTH,
        FUNC_TRUNCATE_DAY,
        FUNC_TRUNCATE_HOUR,
        FUNC_TRUNCATE_MINUTE,
        FUNC_TRUNCATE_SECOND,
        FUNC_TRUNCATE_MILLISECOND,
        FUNC_TRUNCATE_MICROSECOND
    };

    std::vector<string> maxExpected {
        "9999-01-01",                 // year
        "9999-10-01",                 // quarter
        "9999-12-01",                 // month
        "9999-12-31",                 // day
        "9999-12-31 23:00:00.000000", // hour
        "9999-12-31 23:59:00.000000", // minute
        "9999-12-31 23:59:59.000000", // second
        "9999-12-31 23:59:59.999000", // millisecond
        "9999-12-31 23:59:59.999999"  // microsecond
    };

    const std::string outOfRangeMessage = getInputOutOfRangeMessage("TRUNCATE");
    int i = 0;
    BOOST_FOREACH(int func, funcs) {
        ASSERT_EQ(testUnary(func, nullTimestamp, nullTimestamp, true), 0);
        ASSERT_EQ("success", testUnaryThrows(func, minInt64, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, tooSmallTimestamp, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, tooBigTimestamp, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, maxInt64, outOfRangeMessage));

        // truncate on the min valid timestamp is always a no-op,
        // except for bug ENG-10507, which is related to TRUNCATE MILLISECOND.
        if (func != FUNC_TRUNCATE_MILLISECOND) {
            ASSERT_EQ(testUnary(func, minValidTimestamp, minValidTimestamp), 0);
        }
        else {
            ASSERT_EQ(testUnary(func, minValidTimestamp, minValidTimestamp), -1);
        }

        ASSERT_EQ(testUnary(func, maxValidTimestamp,
                            timestampFromString(maxExpected[i])), 0);

        ++i;
    }
}

TEST_F(FunctionTest, DateFunctionsExtract) {

    std::vector<int> funcs {
        FUNC_EXTRACT_YEAR,
        FUNC_EXTRACT_MONTH,
        FUNC_EXTRACT_DAY,
        FUNC_EXTRACT_DAY_OF_WEEK,
        FUNC_EXTRACT_WEEKDAY,
        FUNC_EXTRACT_WEEK_OF_YEAR,
        FUNC_EXTRACT_DAY_OF_YEAR,
        FUNC_EXTRACT_QUARTER,
        FUNC_EXTRACT_HOUR,
        FUNC_EXTRACT_MINUTE,
        FUNC_EXTRACT_SECOND
    };

    std::vector<string> funcNames {
        "YEAR",
        "MONTH",
        "DAY",
        "DAY_OF_WEEK",
        "WEEKDAY",
        "WEEK_OF_YEAR",
        "DAY_OF_YEAR",
        "QUARTER",
        "HOUR",
        "MINUTE",
        "SECOND"
    };

    std::vector<int> minExpected {
        1583, // year
        1,    // month
        1,    // day
        7,    // day of week: Saturday
        5,    // weekday: Saturday
        52,   // week of year (consistent with ISO-8601)
        1,    // day of year
        1,    // quarter
        0,    // hour
        0,    // minute
        0     // second
    };

    std::vector<int> maxExpected {
        9999, // year
        12,   // month
        31,   // day
        6,    // day of week: Friday
        4,    // weekday: Friday
        52,   // week of year
        365,  // day of year
        4,    // quarter
        23,   // hour
        59,   // minute
        -1    // second  (EXTRACT second produces a decimal, see below)
    };

    int i = 0;
    BOOST_FOREACH(int func, funcs) {
        const std::string outOfRangeMessage = getInputOutOfRangeMessage(funcNames[i]);
        ASSERT_EQ("success", testUnaryThrows(func, minInt64, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, tooSmallTimestamp, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, tooBigTimestamp, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, maxInt64, outOfRangeMessage));

        ASSERT_EQ(testUnary(func, nullTimestamp, nullTimestamp, true), 0);

        ASSERT_EQ(testUnary(func, minValidTimestamp, minExpected[i]), 0);

        if (func != FUNC_EXTRACT_SECOND) {
            ASSERT_EQ(testUnary(func, maxValidTimestamp, maxExpected[i]), 0);
        }
        else {
            ASSERT_EQ(testUnary(func, maxValidTimestamp,
                                ValueFactory::getDecimalValueFromString("59.999999")), 0);
        }

        ++i;
    }
}

TEST_F(FunctionTest, DateFunctionsAdd) {
    const std::string intervalTooLargeMsg = "interval is too large for DATEADD function";

    std::vector<int> funcs {
        FUNC_VOLT_DATEADD_YEAR,
        FUNC_VOLT_DATEADD_QUARTER,
        FUNC_VOLT_DATEADD_MONTH,
        FUNC_VOLT_DATEADD_DAY,
        FUNC_VOLT_DATEADD_HOUR,
        FUNC_VOLT_DATEADD_MINUTE,
        FUNC_VOLT_DATEADD_SECOND,
        FUNC_VOLT_DATEADD_MILLISECOND,
        FUNC_VOLT_DATEADD_MICROSECOND
    };

    std::vector<int64_t> maxIntervals {
        PTIME_MAX_YEAR_INTERVAL,
        PTIME_MAX_QUARTER_INTERVAL,
        PTIME_MAX_MONTH_INTERVAL,
        PTIME_MAX_DAY_INTERVAL,
        PTIME_MAX_HOUR_INTERVAL,
        PTIME_MAX_MINUTE_INTERVAL,
        PTIME_MAX_SECOND_INTERVAL,
        PTIME_MAX_MILLISECOND_INTERVAL,
        PTIME_MAX_MICROSECOND_INTERVAL
    };

    std::vector<int64_t> minIntervals {
        PTIME_MIN_YEAR_INTERVAL,
        PTIME_MIN_QUARTER_INTERVAL,
        PTIME_MIN_MONTH_INTERVAL,
        PTIME_MIN_DAY_INTERVAL,
        PTIME_MIN_HOUR_INTERVAL,
        PTIME_MIN_MINUTE_INTERVAL,
        PTIME_MIN_SECOND_INTERVAL,
        PTIME_MIN_MILLISECOND_INTERVAL,
        PTIME_MIN_MICROSECOND_INTERVAL
    };

    const std::string outOfRangeMessage = getInputOutOfRangeMessage("DATEADD");
    const std::string outputOutOfRangeMessage = getOutputOutOfRangeMessage("DATEADD");
    int i = 0;
    BOOST_FOREACH(int func, funcs) {
        // test null values
        ASSERT_EQ(0, testBinary(func, 1, nullTimestamp, nullTimestamp, true));
        ASSERT_EQ(0, testBinary(func, NValue::getNullValue(ValueType::tBIGINT),
                                minValidTimestamp, nullTimestamp, true));

        ASSERT_EQ("success", testBinaryThrows(func, 1, minInt64, outOfRangeMessage));
        ASSERT_EQ("success", testBinaryThrows(func, 1, tooSmallTimestamp, outOfRangeMessage));
        ASSERT_EQ("success", testBinaryThrows(func, 1, tooBigTimestamp, outOfRangeMessage));
        ASSERT_EQ("success", testBinaryThrows(func, 1, maxInt64, outOfRangeMessage));

        ASSERT_EQ("success", testBinaryThrows(func, maxIntervals[i] + 1,
                                              minValidTimestamp, intervalTooLargeMsg));
        ASSERT_EQ("success", testBinaryThrows(func, minIntervals[i] - 1,
                                              maxValidTimestamp, intervalTooLargeMsg));

        ASSERT_EQ(testBinary(func, 0, minValidTimestamp, minValidTimestamp), 0);
        ASSERT_EQ(testBinary(func, 0, maxValidTimestamp, maxValidTimestamp), 0);

        // This just asserts that if we add 1 unit (year, month, whatever)
        // that the result is larger than the input.
        ASSERT_EQ(testBinary(func, 1, minValidTimestamp, minValidTimestamp), 1);

        // Likewise for subtracting a unit
        ASSERT_EQ(testBinary(func, -1, maxValidTimestamp, maxValidTimestamp), -1);

        // DATEADD that would produce an out of range timestamp should throw
        ASSERT_EQ("success", testBinaryThrows(func, -1, minValidTimestamp, outputOutOfRangeMessage));
        ASSERT_EQ("success", testBinaryThrows(func, 1, maxValidTimestamp, outputOutOfRangeMessage, intervalTooLargeMsg));

        ++i;
    }
}

static const int64_t MIN_INT64 = std::numeric_limits<int64_t>::min() + 1;
static const int64_t MAX_INT64 = std::numeric_limits<int64_t>::max();


TEST_F(FunctionTest, DateFunctionsSinceEpoch) {
    std::vector<int> funcs {
        FUNC_SINCE_EPOCH_SECOND,
        FUNC_SINCE_EPOCH_MILLISECOND,
        FUNC_SINCE_EPOCH_MICROSECOND
    };

    std::vector<int> scale {
        1000000,
        1000,
        1
    };

    const std::string outOfRangeMessage = getInputOutOfRangeMessage("SINCE_EPOCH");
    int i = 0;
    BOOST_FOREACH(int func, funcs) {
        ASSERT_EQ(0, testUnary(func, nullTimestamp, nullTimestamp, true));

        // SINCE_EPOCH does no range checking on timestamps, just simple division
        // by 1, 1000 or 1000000.  Therefore it doesn't throw an exception for
        // out of range values.

        ASSERT_EQ("success", testUnaryThrows(func, minInt64, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, tooSmallTimestamp, outOfRangeMessage));
        ASSERT_EQ(0, testUnary(func, minValidTimestamp, GREGORIAN_EPOCH / scale[i]));
        ASSERT_EQ(0, testUnary(func, maxValidTimestamp, NYE9999 / scale[i]));
        ASSERT_EQ("success", testUnaryThrows(func, tooBigTimestamp, outOfRangeMessage));
        ASSERT_EQ("success", testUnaryThrows(func, maxInt64, outOfRangeMessage));

        ++i;
    }
}

TEST_F(FunctionTest, DateFunctionsToTimestamp) {
    const std::string overflowMessage = "Input to TO_TIMESTAMP would overflow TIMESTAMP data type";

    std::vector<int> funcs {
        FUNC_TO_TIMESTAMP_SECOND,
        FUNC_TO_TIMESTAMP_MILLISECOND,
        FUNC_TO_TIMESTAMP_MICROSECOND
    };

    std::vector<int> scale {
        1000000,
        1000,
        1
    };

    const NValue nullBigint = NValue::getNullValue(ValueType::tNULL);

    const std::string outOfRangeMessage = getInputOutOfRangeMessage("TO_TIMESTAMP");
    const std::string outputOutOfRangeMessage = getOutputOutOfRangeMessage("TO_TIMESTAMP");
    int i = 0;
    BOOST_FOREACH(int func, funcs) {
        ASSERT_EQ(0, testUnary(func, nullBigint, nullTimestamp, true));

        // These functions really just multiply their argument by a constant
        // and produce a timestamp, so there are no range checks, except to avoid
        // overflow of the 64-bit timestamp storage.

        if (scale[i] != 1) {
            ASSERT_EQ("success", testUnaryThrows(func, MIN_INT64, overflowMessage));
            ASSERT_EQ("success", testUnaryThrows(func, (MIN_INT64 / scale[i]) - 1, overflowMessage));
            ASSERT_EQ("success", testUnaryThrows(func, MAX_INT64, overflowMessage));
            ASSERT_EQ("success", testUnaryThrows(func, (MAX_INT64 / scale[i]) + 1, overflowMessage));
        }

        const int64_t TRUNCATED_MIN_VALID_TS = (GREGORIAN_EPOCH / scale[i]) * scale[i];
        const int64_t TRUNCATED_MAX_VALID_TS = (NYE9999 / scale[i]) * scale[i];

        ASSERT_EQ("success", testUnaryThrows(func, MIN_INT64 / scale[i], outputOutOfRangeMessage));
        ASSERT_EQ(0, testUnary(func, GREGORIAN_EPOCH / scale[i],
                               ValueFactory::getTimestampValue(TRUNCATED_MIN_VALID_TS)));
        ASSERT_EQ(0, testUnary(func, NYE9999/ scale[i],
                               ValueFactory::getTimestampValue(TRUNCATED_MAX_VALID_TS)));
        ASSERT_EQ("success", testUnaryThrows(func, MAX_INT64 / scale[i], outputOutOfRangeMessage));

        ++i;
    }
}

TEST_F(FunctionTest, TestTimestampValidity)
{
    // Test the two constant functions.
    ASSERT_EQ(0, testNullary(FUNC_VOLT_MIN_VALID_TIMESTAMP,
                             ValueFactory::getTimestampValue(GREGORIAN_EPOCH)));
    ASSERT_EQ(0, testNullary(FUNC_VOLT_MAX_VALID_TIMESTAMP,
                             ValueFactory::getTimestampValue(NYE9999)));
    // Test of of range below.
    ASSERT_EQ(0, testUnary(FUNC_VOLT_IS_VALID_TIMESTAMP,
                           ValueFactory::getTimestampValue(GREGORIAN_EPOCH - 1000),
                           False));
    // Test out of range above.
    ASSERT_EQ(0, testUnary(FUNC_VOLT_IS_VALID_TIMESTAMP,
                           ValueFactory::getTimestampValue(NYE9999 + 1000),
                           False));
    // Test in range, including the endpoints
    ASSERT_EQ(0, testUnary(FUNC_VOLT_IS_VALID_TIMESTAMP,
                           ValueFactory::getTimestampValue(0),
                           True));
    ASSERT_EQ(0, testUnary(FUNC_VOLT_IS_VALID_TIMESTAMP,
                           ValueFactory::getTimestampValue(GREGORIAN_EPOCH),
                           True));
    ASSERT_EQ(0, testUnary(FUNC_VOLT_IS_VALID_TIMESTAMP,
                           ValueFactory::getTimestampValue(NYE9999),
                           True));
    // Test null input
    ASSERT_EQ(0, testUnary(FUNC_VOLT_IS_VALID_TIMESTAMP,
                           NValue::getNullValue(ValueType::tTIMESTAMP),
                           NValue::getNullValue(ValueType::tTIMESTAMP),
                           True));
}

int main(int argc, char **argv) {
    for (argv++; *argv; argv++) {
        if (strcmp(*argv, "--verbose") == 0) {
            staticVerboseFlag = true;
        } else {
            std::cerr << "Unknown command line parameter: " << *argv << std::endl;
        }
    }
     return TestSuite::globalInstance()->runAll();
}
