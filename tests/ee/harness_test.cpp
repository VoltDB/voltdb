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

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cassert>
#include <cstdlib>
#include <cstdio>
#include "harness.h"

// A custom assert macro that avoids "unused variable" warnings when compiled
// away.
#ifdef NDEBUG
#undef assert
#define assert(x) ((void)(x))
#endif


using std::string;
using stupidunit::ChTempDir;

// NOTE: This is a very bizarre test, since it is testing the unit test library. To minimize
// weirdness, we avoid using stupidunit in parts of this file.


// utility function to test for trailing string.
bool endsWith(const string& input, const string& end) {
    int start = (int)(input.size() - end.size());
    // Can't be true if the ending is larger than the input
    if (start < 0) return false;
    return input.compare(start, end.size(), end) == 0;
}


// Abstract class for testing EXPECT and ASSERT statements
class TestTemplate : public Test {
public:
    virtual const char* suiteName() const { return "Test"; }
    virtual const char* testName() const { return "Test"; }
};

class SuccessTest : public TestTemplate {
public:
    virtual void run() {
        // This test does nothing: it is successful
    }
};

// Returns true if the Test::fail() method works.
bool testSuccess() {
    SuccessTest test;
    test.run();
    return test.testSuccess();
}

static const char FailTestE1[] = "fail called once\"\\";
static const char FailTestE2[] = "fail called twice";
class FailTest : public TestTemplate {
public:
    virtual void run() {
        fail(__FILE__, __LINE__, FailTestE1);
        fail(__FILE__, __LINE__, FailTestE2);
    }
};

// Returns true if the Test::fail() method works.
bool testFail() {
    FailTest test;
    test.run();

    if (test.testSuccess()) return false;
    if (test.stupidunitNumErrors() != 2) return false;
    if (!endsWith(test.stupidunitError(0), FailTestE1)) return false;
    if (!endsWith(test.stupidunitError(1), FailTestE2)) return false;

    return true;
}

bool validateBinaryOperation(const Test& test, const char* op_string, bool test1_success, bool test2_success, bool test3_success) {
    if (test.testSuccess()) return false;

    int expected_errors = 3 - test1_success - test2_success - test3_success;
    if (test.stupidunitNumErrors() != expected_errors) return false;

    string message1 = string("3 ") + op_string + " 5";
    string message2 = string("5 ") + op_string + " 5";
    string message3 = string("6 ") + op_string + " 5";

    int error = 0;
    if (!test1_success) {
        if (!endsWith(test.stupidunitError(error), message1)) return false;
        error += 1;
    }
    if (!test2_success) {
        if (!endsWith(test.stupidunitError(error), message2)) return false;
        error += 1;
    }
    if (!test3_success) {
        if (!endsWith(test.stupidunitError(error), message3)) return false;
        error += 1;
    }
    return true;
}

// Abuse templates to macros to test all operations
#define TEST_EXPECT_OP(operation, op_string, test1_success, test2_success, test3_success) \
bool testExpect ##operation () { \
    class ExpectTest : public TestTemplate { \
    public: \
        virtual void run() { \
            EXPECT_ ##operation (3, 5); \
            EXPECT_ ##operation (5, 5); \
            EXPECT_ ##operation (6, 5); \
        } \
    }; \
    ExpectTest test; \
    test.run(); \
    return validateBinaryOperation(test, op_string, test1_success, test2_success, test3_success); \
}

TEST_EXPECT_OP(EQ, "==", false, true, false)
TEST_EXPECT_OP(NE, "!=", true, false, true)
TEST_EXPECT_OP(LT, "<", true, false, false)
TEST_EXPECT_OP(LE, "<=", true, true, false)
TEST_EXPECT_OP(GT, ">", false, false, true)
TEST_EXPECT_OP(GE, ">=", false, true, true)
#undef TEST_EXPECT_OP

// Returns true if EXPECT_TRUE and EXPECT_FALSE work.
bool testExpectTrue() {
    class ExpectTest : public TestTemplate {
    public:
        virtual void run() {
            EXPECT_TRUE(0);
            EXPECT_FALSE(1);
        }
    };
    ExpectTest test;
    test.run();

    if (test.testSuccess()) return false;
    if (test.stupidunitNumErrors() != 2) return false;
    if (!endsWith(test.stupidunitError(0), "is false")) return false;
    if (!endsWith(test.stupidunitError(1), "is true")) return false;
    return true;
}

// Returns true if ASSERT_EQ works. We'll assume the others work the same way.
bool testAssert() {
    class FailTest : public TestTemplate {
    public:
        virtual void run() {
            ASSERT_EQ(3, 5);
            fail(__FILE__, __LINE__, "do not reach this");
        }
    };

    FailTest test;
    test.run();

    if (test.testSuccess()) return false;
    if (test.stupidunitNumErrors() != 1) return false;
    if (!endsWith(test.stupidunitError(0), "3 == 5")) return false;

    return true;
}

// Returns true if ASSERT_TRUE works.
bool testAssertTrue() {
    class AssertTest : public TestTemplate {
    public:
        virtual void run() {
            ASSERT_TRUE(0);
            fail(__FILE__, __LINE__, "not reached");
        }
    };
    AssertTest test;
    test.run();

    if (test.testSuccess()) return false;
    if (test.stupidunitNumErrors() != 1) return false;
    if (!endsWith(test.stupidunitError(0), "is false")) return false;
    return true;
}


// Test the EXPECT_DEATH macro. Note: This should *not* produce any output.
TEST(ExpectDeath, FailsToDie) {
    // Skip expectDeath in non-debug builds because overwriting memory doesn't
    // always cause release builds to crash.
#ifndef DEBUG
    printf("SKIPPED: testing expectDeath due to non-debug build.\n");
    return;
#endif

    static const char MSG[] = "THIS TEXT SHOULD BE CAPTURED AND NOT VISIBLE";
    class DeathTest : public TestTemplate {
    public:
        virtual void run() {
            EXPECT_DEATH(printf("%s\n", MSG); fprintf(stderr, "%s\n", MSG));
        }
    };
    DeathTest test;
    test.run();

    EXPECT_FALSE(test.testSuccess());
    ASSERT_EQ(1, test.stupidunitNumErrors());
    EXPECT_TRUE(endsWith(test.stupidunitError(0), "did not die"));
}

// Test the EXPECT_DEATH macro. Note: This should *not* produce any output.
TEST(ExpectDeath, Dies) {
    class DeathTest : public TestTemplate {
    public:
        virtual void run() {
            EXPECT_DEATH(assert(false));
        }
    };
    DeathTest test;
    test.run();

    EXPECT_TRUE(test.testSuccess());
    EXPECT_EQ(0, test.stupidunitNumErrors());
}

TEST(ChTempDir, Simple) {
    string tempdir_name;
    {
        ChTempDir tempdir;
        tempdir_name = tempdir.name();

        // Create a file in the temporary directory
        string name = tempdir.name() + "/..foo";
        int fd = open(name.c_str(), O_CREAT, 666);
        EXPECT_NE(0, fd);
        int status = close(fd);
        EXPECT_EQ(0, status);

        // Create a directory in the temporary directory
        name = tempdir.name() + "/..bar";
        status = mkdir(name.c_str(), 0777);
        EXPECT_EQ(0, status);
    }

    // Verify that the directory no longer exists
    struct stat buf;
    int status = stat(tempdir_name.c_str(), &buf);
    EXPECT_EQ(-1, status);
    EXPECT_EQ(ENOENT, errno);
}

class StupidUnitOutputTest : public Test {
public:
    StupidUnitOutputTest() {
        const char* current = getenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE);
        if (current != NULL) {
            current_value_ = current;
        }
        out_name_ = temp_dir_.name() + "/out";
    }

    ~StupidUnitOutputTest() {
        if (!current_value_.empty()) {
            int success = setenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE,
                    current_value_.c_str(), 1);
            assert(success == 0);
        } else {
            int success = unsetenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE);
            assert(success == 0);
        }
    }

protected:
    TestSuite suite_;
    string current_value_;
    ChTempDir temp_dir_;
    string out_name_;
};

TEST(JSONEscape, Simple) {
    extern void jsonEscape(string* s);

    static const char TEST[] = "\"\\\b\f\n\r\t\x1f\x0f";
    // include the NULL byte
    string test(TEST, sizeof(TEST));
    jsonEscape(&test);
    EXPECT_EQ("\\\"\\\\\\b\\f\\n\\r\\t\\u001f\\u000f\\u0000", test);
}

TEST_F(StupidUnitOutputTest, EmptyOutputFileName) {
    suite_.registerTest(&RegisterTest<SuccessTest>::create);
    EXPECT_EQ(0, setenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE, "", 1));
    EXPECT_EQ(0, suite_.runAll());
}

TEST_F(StupidUnitOutputTest, FileExists) {
    suite_.registerTest(&RegisterTest<SuccessTest>::create);
    EXPECT_EQ(0, setenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE, out_name_.c_str(), 1));

    // When the output file exists, the tests refuse to run
    int fh = open(out_name_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    ASSERT_GE(fh, 0);
    close(fh);
    EXPECT_DEATH(suite_.runAll());
}

static string readFile(const char* path) {
    int handle = open(path, O_RDONLY);
    assert(handle >= 0);

    struct stat info;
    size_t error = fstat(handle, &info);
    assert(error == 0);

    string data(info.st_size, '\0');
    error = read(handle, const_cast<char*>(data.data()), data.size());

    assert(error == data.size());
    error = close(handle);
    assert(error == 0);

    return data;
}

TEST_F(StupidUnitOutputTest, OutputSuccess) {
    suite_.registerTest(&RegisterTest<SuccessTest>::create);
    suite_.registerTest(&RegisterTest<SuccessTest>::create);
    EXPECT_EQ(0, setenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE, out_name_.c_str(), 1));
    EXPECT_EQ(0, suite_.runAll());

    string expected_single = "{\"class_name\": \"Test\", \"name\": \"Test\"}";
    string expected_real = "[" + expected_single + ",\n" + expected_single + "]\n";
    string out = readFile(out_name_.c_str());
    EXPECT_EQ(expected_real, out);
}

TEST_F(StupidUnitOutputTest, OutputFailure) {
    suite_.registerTest(&RegisterTest<FailTest>::create);
    EXPECT_EQ(0, setenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE, out_name_.c_str(), 1));
    EXPECT_EQ(1, suite_.runAll());

    string out = readFile(out_name_.c_str());
    const char PREFIX[] = "[{\"class_name\": \"Test\", \"name\": \"Test\", \"failure\": \"";
    EXPECT_TRUE(out.compare(0, sizeof(PREFIX)-1, PREFIX) == 0);
    const char MSG[] = "fail called once\\\"\\";
    EXPECT_TRUE(out.find(MSG) != string::npos);
    EXPECT_TRUE(endsWith(out, "\"}]\n"));
}

struct BasicTest {
    bool (*test_function)();
    const char* name;
};

// Abuse macros to make life easier
#define ADD_TEST(x) { x, #x }
const struct BasicTest tests[] = {
    ADD_TEST(testSuccess),
    ADD_TEST(testFail),
    ADD_TEST(testExpectEQ),
    ADD_TEST(testExpectNE),
    ADD_TEST(testExpectLT),
    ADD_TEST(testExpectLE),
    ADD_TEST(testExpectGT),
    ADD_TEST(testExpectGE),
    ADD_TEST(testExpectTrue),
    ADD_TEST(testAssert),
    ADD_TEST(testAssertTrue),
};
#undef ADD_TEST
static const size_t TESTS_SIZE = sizeof(tests)/sizeof(*tests);

int main() {
    int failure_count = 0;
    {
        // This saves and restores the StupidUnit output environment variable
        class SaveRestoreEnvironment : public StupidUnitOutputTest {
        public:
            virtual void run() {}
            virtual const char* suiteName() const { return NULL; }
            virtual const char* testName() const { return NULL; }
        };
        SaveRestoreEnvironment save_environment;
        int success = unsetenv(stupidunit::OUT_FILE_ENVIRONMENT_VARIABLE);
        assert(success == 0);

        for (int i = 0; i < TESTS_SIZE; ++i) {
            const char* result = "PASSED.";
            if (!tests[i].test_function()) {
                result = "FAILED.";
                failure_count += 1;
            }

            printf("%s: %s\n", tests[i].name, result);
        }
        printf("\n");
    }

    return failure_count + TestSuite::globalInstance()->runAll();
}
