/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * A subclass of TestSuite that multiplexes test methods across
 * all volt configurations given to it. For example, if there are
 * 7 test methods and 2 volt configurations, this TestSuite will
 * contain 14 tests and will munge names to descibe each test
 * individually. This class is typically used as a helper in a
 * TestCase's suite() method.
 *
 */
public class MultiConfigSuiteBuilder extends TestSuite {

    /** The class that contains the JUnit test methods to run */
    final Class<? extends TestCase> m_testClass;
    // Collection of test method names to be ignored and not executed
    final Collection<String> m_ignoredTests;

    /**
     * Get the JUnit test methods for a given class. These methods have no
     * parameters, return void and start with "test".
     *
     * @return A list of the names of each JUnit test method.
     */
    List<String> getTestMethodNames() {
        ArrayList<String> retval = new ArrayList<>();

        for (Method m : m_testClass.getMethods()) {
            if (m.getReturnType() != void.class) {
                continue;
            }
            if (m.getParameterCount() > 0) {
                continue;
            }
            String name = m.getName();
            if (!name.startsWith("test") || m_ignoredTests.contains(name)) {
                continue;
            }
            retval.add(name);
        }

        return retval;
    }

    /**
     * Initialize by passing in a class that contains JUnit test methods to run.
     *
     * @param testClass The class that contains the JUnit test methods to run.
     */
    public MultiConfigSuiteBuilder(Class<? extends TestCase> testClass) {
        this(testClass, Collections.emptySet());
    }

    /**
     * Initialize by passing in a class that contains JUnit test methods to run.
     *
     * @param testClass The class that contains the JUnit test methods to run.
     * @param ignoredTests {@link Collection} of test names to be skipped. A test will be skipped if
     *                  {@code skipTest.contains(methodName)} returns {@code true}
     */
    public MultiConfigSuiteBuilder(Class<? extends TestCase> testClass, Collection<String> ignoredTests) {
        m_testClass = testClass;
        m_ignoredTests = ignoredTests;
    }

    /**
     * Add a sever configuration to the set of configurations we want these
     * tests to run on.
     *
     * @param config A Server Configuration to run this set of tests on.
     */
    public boolean addServerConfig(VoltServerConfig config) {
        return addServerConfig(config, true);
    }

    public boolean addServerConfig(VoltServerConfig config, boolean reuseServer) {

        if (config.isValgrind()) {
            reuseServer = false;
        }

        final String enabled_configs = System.getenv().get("VOLT_REGRESSIONS");
        System.out.println("VOLT REGRESSIONS ENABLED: " + enabled_configs);

        if (!(enabled_configs == null || enabled_configs.contentEquals("all")))
        {
            if (config instanceof LocalCluster) {
                if (config.isHSQL() && !enabled_configs.contains("hsql")) {
                    return true;
                }
                if ((config.getNodeCount() == 1) && !enabled_configs.contains("local")) {
                    return true;
                }
                if ((config.getNodeCount() > 1) && !enabled_configs.contains("cluster")) {
                    return true;
                }
            }
        }

        if (LocalCluster.isMemcheckDefined()) {
            if (config instanceof LocalCluster) {
                LocalCluster lc = (LocalCluster) config;
                // don't run valgrind on multi-node clusters without embedded processes
                if ((lc.getNodeCount() > 1) || (lc.m_hasLocalServer == false)) {
                    return true;
                }
            }
            if (config.isHSQL()) {
                return true;
            }
        }

        // get the constructor of the test class
        Constructor<?> cons = null;
        try {
            cons = m_testClass.getConstructor(String.class);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        // get the set of test methods
        List<String> methods = getTestMethodNames();

        // add a test case instance for each method for the specified
        // server config
        for (int i = 0; i < methods.size(); i++) {
            String mname = methods.get(i);
            RegressionSuite rs = null;
            try {
                rs = (RegressionSuite) cons.newInstance(mname);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            rs.setConfig(config);
            // The last test method for the current cluster configuration will need to
            // shutdown the cluster completely after finishing the test.
            rs.m_completeShutdown = ! reuseServer || (i == methods.size() - 1);
            super.addTest(rs);
        }

        return true;
    }

    @Override
    public void addTest(Test test) {
        // don't let users do this
        throw new RuntimeException("Unsupported Usage");
    }

    @Override
    public void addTestSuite(Class<? extends TestCase> testClass) {
        // don't let users do this
        throw new RuntimeException("Unsupported Usage");
    }
}
