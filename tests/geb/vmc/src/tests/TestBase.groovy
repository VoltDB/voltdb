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

package vmcTest.tests

import geb.spock.GebReportingSpec

/**
 * This class is the base class for all of the test classes; it merely
 * provides (static) convenience method(s).
 */
class TestBase extends GebReportingSpec {

    // Set this to true, if you want to see debug print
    static final boolean DEBUG = true;

    /**
     * Optionally (if the <i>debug</i> argument is true), prints the specified
     * text message (to stdout).
     * @param text - the text to be printed.
     * @param debug - whether or not to print the text.
     */
    static void debugPrint(String text, boolean debug) {
        if (debug) {
            println text
        }
    }
    
    /**
     * Optionally (if the <i>DEBUG</i> constant is true), prints the specified
     * text message (to stdout).
     * @param text - the text to be printed.
     */
    static void debugPrint(String text) {
        debugPrint(text, DEBUG)
    }

}
