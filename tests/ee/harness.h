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

// A stupid and simple unit test framework for C++ code.
// Evan Jones <ej@evanjones.ca>

#ifndef STUPIDUNIT_H__
#define STUPIDUNIT_H__

#include <string>
#include <vector>
#include <iostream>

class Test;

// Contains and runs a collection of tests.
class TestSuite {
public:
    typedef Test * (*test_factory_t)();
    void registerTest(test_factory_t);

    // Returns the number of failed tests.
    int runAll();

    // Returns a properly initialized static global TestSuite. This is the "standard" test suite
    // used by the TEST and TEST_F macros.
    static TestSuite* globalInstance();

private:
    std::vector<test_factory_t> test_factories_;
};

// Base class for a single test. Each test creates a subclass of this that
// implements run(). Users create subclasses via the TEST_F helper macro.
class Test {
public:
    Test() {}
    virtual ~Test() {}

    // Run the actual test.
    virtual void run() = 0;

    virtual const char* suiteName() const = 0;
    virtual const char* testName() const = 0;
    bool testSuccess() const { return errors_.empty(); }

    // Fail the test with error.
    void fail(const char* file, int line, const char* message);

    // Output the errors for this test to standard output.
    void printErrors() const;

    size_t stupidunitNumErrors() const { return errors_.size(); }
    const std::string& stupidunitError(int i) const;

private:
    // Contains error messages if the test failed.
    std::vector<std::string> errors_;
};

// A class used to statically register test instances with the global test suite.
template <typename T>
class RegisterTest {
public:
    RegisterTest(TestSuite* suite) {
        if (suite != NULL)
            suite->registerTest(&RegisterTest<T>::create);
    }

    ~RegisterTest() {
    }

    static Test* create() {
        return new T();
    }
};

// Creates a test subclass.
#define MAGIC_TEST_MACRO(parent_class, suite_name, test_name, _suite_) \
    class suite_name ## _ ## test_name : public parent_class { \
    public: \
        virtual ~suite_name ## _ ## test_name() {} \
        virtual void run(); \
        virtual const char* suiteName() const { return suite_name_; } \
        virtual const char* testName() const { return test_name_; } \
\
    private: \
        static const char suite_name_[]; \
        static const char test_name_[]; \
    }; \
    const char suite_name ## _ ## test_name::suite_name_[] =  #suite_name; \
    const char suite_name ## _ ## test_name::test_name_[] =  #test_name; \
    static RegisterTest<suite_name ## _ ## test_name> suite_name ## _ ## test_name ## _register(_suite_); \
    void suite_name ## _ ## test_name::run()

// A magic macro to make a test part of a user-defined test subclass.
#define TEST_F(harness_name, test_name) MAGIC_TEST_MACRO(harness_name, harness_name, test_name, TestSuite::globalInstance())

// A magic macro to make a test subclass for a block of code.
#define TEST(suite_name, test_name) MAGIC_TEST_MACRO(Test, suite_name, test_name, TestSuite::globalInstance())

/*
 * Define STUPID_UNIT_TWEAK to enable these special capabilities.
 * Selectively disable tests by prepending the TEST* macros with "NO", e.g.
 *      NOTEST_F(...).
 * Define STUPID_UNIT_SOLO to disable all tests except for the one prepended with "SOLO", e.g.
 *      SOLOTEST_F(...)
 * Force a breakpoint when any unit test assertion fails.
 *      #define STUPIDUNIT_ASSERT_BREAKPOINT
 * IMPORTANT: The build will intentionally fail if STUPIDUNIT_TWEAK is not
 * defined while there are NOTEST* macros in code. Similarly, it will fail if
 * STUPID_UNIT_SOLO is not defined while there are SOLOTEST* macros in code.
 * This prevents accidentally leaving tests disabled.
 *** DO NOT PUT a STUPIDUNIT_TWEAK #define in the code, or tests may be
 *** accidentally disabled in the real build! Only define it in an IDE
 *** configuration or use it manually in a special command line build.
 */
#ifdef STUPIDUNIT_TWEAK
#define NOTEST_F(harness_name, test_name) MAGIC_TEST_MACRO(harness_name, harness_name, test_name, NULL)
#define NOTEST(suite_name, test_name) MAGIC_TEST_MACRO(Test, suite_name, test_name, NULL)
#endif
#ifdef STUPIDUNIT_SOLO
#ifndef STUPIDUNIT_TWEAK
#error STUPIDUNIT_SOLO is defined without STUPIDUNIT_TWEAK. Do not leave either #define in code!
#endif
#undef TEST_F
#undef TEST
#define TEST_F(harness_name, test_name) MAGIC_TEST_MACRO(harness_name, harness_name, test_name, NULL)
#define TEST(suite_name, test_name) MAGIC_TEST_MACRO(Test, suite_name, test_name, NULL)
#define SOLOTEST_F(harness_name, test_name) MAGIC_TEST_MACRO(harness_name, harness_name, test_name, TestSuite::globalInstance())
#define SOLOTEST(suite_name, test_name) MAGIC_TEST_MACRO(Test, suite_name, test_name, TestSuite::globalInstance())
#endif

// Optionally force a gdb-compatible breakpoint when an assertion triggers.
#ifdef STUPIDUNIT_ASSERT_BREAKPOINT
#define STUPIDUNIT_ASSERT_BREAKPOINT_CODE asm volatile("int3;");
#else
#define STUPIDUNIT_ASSERT_BREAKPOINT_CODE
#endif

// A simple macro to fail a test and print out the file and line number
#define FAIL(msg) fail(__FILE__, __LINE__, msg)

// Abuse macros to easily define all the EXPECT and ASSERT variants
#define STUPIDUNIT_MAKE_EXPECT_MACRO(operation, one, two) \
do { \
    if (!((one) operation (two))) { \
        STUPIDUNIT_ASSERT_BREAKPOINT_CODE \
        fail(__FILE__, __LINE__, #one " " #operation " " #two); \
    } \
} while (false)

#define EXPECT_EQ(one, two) STUPIDUNIT_MAKE_EXPECT_MACRO(==, one, two)
#define EXPECT_NE(one, two) STUPIDUNIT_MAKE_EXPECT_MACRO(!=, one, two)
#define EXPECT_LT(one, two) STUPIDUNIT_MAKE_EXPECT_MACRO(<, one, two)
#define EXPECT_LE(one, two) STUPIDUNIT_MAKE_EXPECT_MACRO(<=, one, two)
#define EXPECT_GT(one, two) STUPIDUNIT_MAKE_EXPECT_MACRO(>, one, two)
#define EXPECT_GE(one, two) STUPIDUNIT_MAKE_EXPECT_MACRO(>=, one, two)

#define EXPECT_TRUE(value) \
do { \
    if (!(value)) { \
        STUPIDUNIT_ASSERT_BREAKPOINT_CODE \
        fail(__FILE__, __LINE__, "Expected true; " #value " is false"); \
    } \
} while (0)
#define EXPECT_FALSE(value) \
do { \
    if ((value)) {\
        STUPIDUNIT_ASSERT_BREAKPOINT_CODE \
        fail(__FILE__, __LINE__, "Expected false; " #value " is true"); \
    } \
} while (0)

// The only difference between EXPECT and ASSERT is that ASSERT returns from
// the test method if the test fails
#define STUPIDUNIT_MAKE_ASSERT_MACRO(operation, one, two, ...) \
do { \
    if (!((one) operation (two))) { \
        STUPIDUNIT_ASSERT_BREAKPOINT_CODE \
        fail(__FILE__, __LINE__, #one " " #operation " " #two); \
        return __VA_ARGS__; \
    } \
} while (0)

// ASSERT macros which require two arguments and an optional third which will be used as the return value
#define ASSERT_EQ(one, two, ...) STUPIDUNIT_MAKE_ASSERT_MACRO(==, one, two, ##__VA_ARGS__)
#define ASSERT_NE(one, two, ...) STUPIDUNIT_MAKE_ASSERT_MACRO(!=, one, two, ##__VA_ARGS__)
#define ASSERT_LT(one, two, ...) STUPIDUNIT_MAKE_ASSERT_MACRO(<, one, two, ##__VA_ARGS__)
#define ASSERT_LE(one, two, ...) STUPIDUNIT_MAKE_ASSERT_MACRO(<=, one, two, ##__VA_ARGS__)
#define ASSERT_GT(one, two, ...) STUPIDUNIT_MAKE_ASSERT_MACRO(>, one, two, ##__VA_ARGS__)
#define ASSERT_GE(one, two, ...) STUPIDUNIT_MAKE_ASSERT_MACRO(>=, one, two, ##__VA_ARGS__)

#define ASSERT_TRUE_WITH_MESSAGE(value, msg, ...)    \
    do {                                        \
        if (!(value)) {                         \
            STUPIDUNIT_ASSERT_BREAKPOINT_CODE   \
                fail(__FILE__, __LINE__, msg);  \
            return __VA_ARGS__;                 \
        }                                       \
    } while (0)

// ASSERT macros which require one argument and an optional second which will be used as the return value
#define ASSERT_TRUE(value, ...) ASSERT_TRUE_WITH_MESSAGE(value, "Expected true; " #value " is false", ##__VA_ARGS__)
#define ASSERT_FALSE(value, ...) ASSERT_TRUE_WITH_MESSAGE(!(value), "Expected false; " #value " is true", ##__VA_ARGS__)

#define ASSERT_FATAL_EXCEPTION(msgFragment, expr)                       \
    do {                                                                \
        try {                                                           \
            expr;                                                       \
            fail(__FILE__, __LINE__,                                    \
                 "expected FatalException that did not occur");         \
        }                                                               \
        catch (FatalException& exc) {                                   \
            std::ostringstream oss;                                     \
            oss << "did not find \""                                    \
                << (msgFragment) << "\" in \""                          \
                << exc.m_reason << "\"";                                \
            ASSERT_TRUE_WITH_MESSAGE(exc.m_reason.find(msgFragment) != std::string::npos, \
                                     oss.str().c_str());                \
        }                                                               \
    } while(false)


namespace stupidunit {

enum ExpectDeathStatus {
    // The caller is the child: run the block and exit.
    EXECUTE_BLOCK,
    SUCCESS,
    FAILED
};

// Implements EXPECT_DEATH.
ExpectDeathStatus expectDeath();

// Helper that creates a temporary directory then changes into it. The
// directory will be automatically removed in the destructor.
class ChTempDir {
public:
    ChTempDir();
    ~ChTempDir();

    const std::string& name() const { return name_; }
    std::string tempFile(const std::string &prefix) const;

private:

    // The name of the temporary directory.
    std::string name_;
};

extern const char OUT_FILE_ENVIRONMENT_VARIABLE[];

}  // namespace stupidunit

#define EXPECT_DEATH(block) do { \
    stupidunit::ExpectDeathStatus status = stupidunit::expectDeath(); \
    if (status == stupidunit::EXECUTE_BLOCK) { \
        block; \
        exit(0); \
    } else if (status == stupidunit::FAILED) { \
        fail(__FILE__, __LINE__, "EXPECT_DEATH(" #block "): did not die"); \
    } \
} while (0)

#endif  // STUPIDUNIT_H__
