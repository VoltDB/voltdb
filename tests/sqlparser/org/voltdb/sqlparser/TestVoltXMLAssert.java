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
package org.voltdb.sqlparser;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.junit.Before;
import org.junit.Test;

import static org.voltdb.sqlparser.semantics.VoltXMLElementAssert.*;

public class TestVoltXMLAssert {
    VoltXMLElement testElem;

    @Before
    public void setup() {
        VoltXMLElement elem = new VoltXMLElement("top");
        addAttributes(elem, 3, "topAttr%d", "topValue%d");
        addChildren(elem, 3, "topChild%d");
        for (VoltXMLElement chelem : elem.children) {
            addAttributes(chelem, 3, "chAttr%d", "chValue%d");
        }
        testElem = elem;
    }
    private void addChildren(VoltXMLElement aElem, int aNumChildren, String aChildNameFormat) {
        for (int idx = 0; idx < aNumChildren; idx += 1) {
            aElem.children.add(new VoltXMLElement(String.format(aChildNameFormat, idx)));
        }
    }
    private void addAttributes(VoltXMLElement aElem, int aNumAttrs,
                               String aAttrFmt,
                               String aValueFmt) {
        for (int idx = 0; idx < aNumAttrs; idx += 1) {
            aElem.attributes.put(String.format(aAttrFmt, idx), String.format(aValueFmt, idx));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testVoltXMLElement() throws Exception {
        assertThat(testElem).hasName("top")
            .hasAllOf(withAttribute("topAttr0", "topValue0"),
                      withAttribute("topAttr2", "topValue2"),
                      withAttribute("topAttr1", "topValue1"),
                      withChildNamed("topChild1",
                            withAttribute("chAttr1", "chValue1"),
                            withAttribute("chAttr2", "chValue2"),
                            withAttribute("chAttr0", "chValue0")),
                      withChildNamed("topChild2",
                            withAttribute("chAttr1", "chValue1"),
                            withAttribute("chAttr2", "chValue2"),
                            withAttribute("chAttr0", "chValue0")),
                      withChildNamed("topChild0",
                           withAttribute("chAttr1", "chValue1"),
                           withAttribute("chAttr2", "chValue2"),
                           withAttribute("chAttr0", "chValue0")));
    }
}
