/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package vmcTest.tests

import java.text.SimpleDateFormat;
import java.util.Date;

import geb.spock.GebReportingSpec

import org.junit.Rule;
import org.junit.rules.TestName;
import org.openqa.selenium.Dimension

import spock.lang.Shared

/**
 * This class is the base class for all of the test classes; it provides
 * initialization and convenience method(s).
 */
class TestBase extends GebReportingSpec {
    @Rule public TestName tName = new TestName()

    // Set this to true, if you want to see debug print
    static final boolean DEFAULT_DEBUG_PRINT = false;
    static final int DEFAULT_WINDOW_WIDTH  = 1500;
    static final int DEFAULT_WINDOW_HEIGHT = 1000;
    static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    @Shared boolean firstDebugMessage = true;

    /**
     * Returns the specified System Property as an int value; or the default
     * value, if the System Property is not set, or cannot be parsed as an int.
     * @param propertyName - name of the System Property whose value you want.
     * @param defaultValue - a default value to be used, if the System Property
     * cannot be retrieved.
     * @return the specified System Property (as int); or the default value.
     */
    static int getIntSystemProperty(String propertyName, int defaultValue) {
        String sysPropValue = System.getProperty(propertyName, "_NOT_SET_")
        if (sysPropValue == "_NOT_SET_") {
            return defaultValue
        } else {
            try {
                return Integer.parseInt(sysPropValue)
            } catch (Throwable e) {
                println "Property '" + propertyName + "' should be int, not '" + sysPropValue + "'."
                return defaultValue
            }
        }
    }

    /**
     * Returns the specified System Property as a boolean value; or the default
     * value, if the System Property is not set, or cannot be parsed as a boolean.
     * @param propertyName - name of the System Property whose value you want.
     * @param defaultValue - a default value to be used, if the System Property
     * cannot be retrieved.
     * @return the specified System Property (as boolean); or the default value.
     */
    static boolean getBooleanSystemProperty(String propertyName, boolean defaultValue) {
        String sysPropValue = System.getProperty(propertyName, "_NOT_SET_")
        if (sysPropValue == "_NOT_SET_") {
            return defaultValue
        } else {
            try {
                return Boolean.parseBoolean(sysPropValue)
            } catch (Throwable e) {
                println "Property '" + propertyName + "' should be int, not '" + sysPropValue + "'."
                return defaultValue
            }
        }
    }

    def setupSpec() { // called once (per test class), before any tests
        // If the window is not the right size, resize it
        def winSize = driver.manage().window().size
        int desiredWidth  = getIntSystemProperty("windowWidth", DEFAULT_WINDOW_WIDTH)
        int desiredHeight = getIntSystemProperty("windowHeight", DEFAULT_WINDOW_HEIGHT)
        if (winSize.width != desiredWidth || winSize.height != desiredHeight) {
            driver.manage().window().setSize(new Dimension(desiredWidth, desiredHeight))
            debugPrint "Window resized, from (" + winSize.width + ", " + winSize.height +
                       ") to (" + desiredWidth + ", " + desiredHeight + ")"
        }
    }

    def setup() { // called before each test
        debugPrint "\nTest: " + tName.getMethodName()
    }

    /**
     * Optionally (if the <i>debug</i> argument is true), prints the specified
     * text message (to stdout).
     * @param text - the text to be printed.
     * @param debug - whether or not to print the text.
     */
    void debugPrint(Object text, boolean debug) {
        if (debug) {
            if (firstDebugMessage) {
                firstDebugMessage = false
                println sdf.format(new Date())
            }
            println text.toString()
        }
    }

    /**
     * Optionally (if the <i>DEBUG</i> constant is true), prints the specified
     * text message (to stdout).
     * @param text - the text to be printed.
     */
    void debugPrint(Object text) {
        debugPrint(text, getBooleanSystemProperty("debugPrint", DEFAULT_DEBUG_PRINT))
    }

}
