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

package org.voltdb.regressionsuites;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.voltdb.utils.MiscUtils;

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
    Class<? extends TestCase> m_testClass = null;

    /**
     * Get the JUnit test methods for a given class. These methods have no
     * parameters, return void and start with "test".
     *
     * @param testCls The class that contains the JUnit test methods to run.
     * @return A list of the names of each JUnit test method.
     */
    static List<String> getTestMethodNames(Class<? extends TestCase> testCls) {
        ArrayList<String> retval = new ArrayList<String>();

        for (Method m : testCls.getMethods()) {
            if (m.getReturnType() != void.class)
                continue;
            if (m.getParameterTypes().length > 0)
                continue;
            if (m.getName().startsWith("test") == false)
                continue;
            retval.add(m.getName());
        }

        return retval;
    }

    /**
     * Initialize by passing in a class that contains JUnit test methods to run.
     *
     * @param testClass The class that contains the JUnit test methods to run.
     */
    public MultiConfigSuiteBuilder(Class<? extends TestCase> testClass) {
        m_testClass = testClass;
    }

    /**
     * Add a sever configuration to the set of configurations we want these
     * tests to run on.
     *
     * @param config A Server Configuration to run this set of tests on.
     */
    public boolean addServerConfig(VoltServerConfig config) {

        // near silent skip on k>0 and community edition
        if (!MiscUtils.isPro()) {
            int k = 0;
            if (config instanceof LocalCluster) {
                k = ((LocalCluster) config).m_kfactor;
            }
            if (k > 0) {
                System.out.println("Skipping ClusterConfig instance with k > 0.");
                return false;
            }
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
        List<String> methods = getTestMethodNames(m_testClass);

        // add a test case instance for each method for the specified
        // server config
        for (String mname : methods) {
            RegressionSuite rs = null;
            try {
                rs = (RegressionSuite) cons.newInstance(mname);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            rs.setConfig(config);
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
