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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.utils;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.voltdb.regressionsuites.ValgrindXMLParser;

/**
 * Test the valgrind xml output parser.
 *
 * The valgrind tool can write output in XML format.  Using this is
 * more reliable than parsing output on stderr meant for human consumption.
 *
 * This test runs on valgrind xml files which have been generated on all
 * the different versions of valgrind which we support.  The xml files are
 * all found in tests/frontend/org/voltdb/utils/valgrind_test_files.  The
 * file names have the form V_T.xml, where V is the valgrind version and
 * T is the test name.  So, 3.11.0_no_losses.xml is the XML output from valgrind
 * 3.11.0 on the program which has no memory losses or errors, or 3.8.1_definite_losses.xml
 * is the valgrind XML output from a program which has definite losses.  These
 * programs are all found in tests/ee/memleaks.
 */
public class TestValgrindParser {

    @Test
    public void testMemoryLeaks() {
        List<String> valgrindErrors = new ArrayList<>();
        File urlFile = new File("tests/frontend/org/voltdb/utils/valgrind_test_files");
        File [] files = urlFile.listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".xml");
            }

        });
        Arrays.sort(files);
        for (File file : files) {
            String baseName = file.getName();
            // Remove the valgrind version and extension.
            String valgrindVersion = baseName.substring(0, baseName.indexOf('_'));
            String programName = baseName.substring(baseName.indexOf('_') + 1, baseName.length()-4);
            System.out.printf("Testing valgrind version %s with program %s\n", valgrindVersion, programName);
            valgrindErrors.clear();
            //
            // Don't delete the XML file.  It's checked into valgrind.
            //
            ValgrindXMLParser.processValgrindOutput(file, valgrindErrors);
            switch (programName) {
            case "definite_losses":
                assertEquals(1, valgrindErrors.size());
                assertTrue(valgrindErrors.get(0).startsWith("Leak_DefinitelyLost"));
                break;
            case "indirect_losses":
                assertEquals(2, valgrindErrors.size());
                assertTrue(valgrindErrors.get(0).startsWith("Leak_IndirectlyLost"));
                assertTrue(valgrindErrors.get(1).startsWith("Leak_DefinitelyLost"));
                break;
            case "possible_losses":
                assertEquals(1, valgrindErrors.size());
                assertTrue(valgrindErrors.get(0).startsWith("Leak_PossiblyLost"));
                break;
            case "rw_deleted":
                assertEquals(3, valgrindErrors.size());
                assertTrue(valgrindErrors.get(0).startsWith("MismatchedFree"));
                assertTrue(valgrindErrors.get(1).startsWith("InvalidRead"));
                assertTrue(valgrindErrors.get(2).startsWith("InvalidWrite"));
                break;
            case "still_reachable_losses":
                assertEquals(1, valgrindErrors.size());
                assertTrue(valgrindErrors.get(0).startsWith("Leak_StillReachable"));
                break;
            case "no_losses":
                assertEquals(0, valgrindErrors.size());
            default:
            }
        }
    }
}
