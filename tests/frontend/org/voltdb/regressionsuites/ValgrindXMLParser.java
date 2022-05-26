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
package org.voltdb.regressionsuites;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class manages valgrind XML output.  The main entry point is the static
 * function processValgrindOutput.
 */
public class ValgrindXMLParser {
    static class StackTrace {
        public List<String> m_stackFrames = new ArrayList<>();
    }

    //
    // Valgrind will have written its output to an xml file.  We
    // open that file here and process it.  If there are memory
    // errors we set the member variable m_allHeapBlocksFreed to
    // false.  If there are other errors we put the error messages
    // in the list m_valgrindErrors.
    //
    static class ValgrindError {
        public String           m_kind;
        public String           m_what;
        public int              m_leakedBytes = -1;
        public int              m_leakedBlocks = -1;
        public List<StackTrace> m_stacks = new ArrayList<>();
        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(m_kind).append(" error:\n")
              .append("  ").append(m_what).append("\n");
            if (0 < m_leakedBytes) {
                sb.append("  leakedBytes: ").append(m_leakedBytes).append("\n");
            }
            if (0 < m_leakedBlocks) {
                sb.append("  leakedBlocks: ").append(m_leakedBlocks).append("\n");
            }
            sb.append("  Stacks:\n");
            if (m_stacks.size() == 0) {
                sb.append("    None!!\n");
            } else {
                for (int stackno = 0; stackno < m_stacks.size(); stackno += 1) {
                    StackTrace st = m_stacks.get(stackno);
                    int frameNo = 0;
                    for (String frame : st.m_stackFrames) {
                        sb.append(String.format("    %2d.) %s\n",
                                                frameNo,
                                                frame));
                        frameNo += 1;
                    }
                }
            }
            return sb.toString();
        }
    }

    private static StackTrace readStackTrace(Node stackTrace) {
        StackTrace answer = new StackTrace();
        for (Node frame = stackTrace.getFirstChild(); frame != null; frame = frame.getNextSibling()) {
            String nodeName = frame.getNodeName();
            if ( ! "frame".equals(nodeName)) {
                continue;
            }
            String  ip     = "<undefined ip>";
            String  fn     = "<unknown function>";
            String  dir    = "<unknown directory>";
            String  file   = "<unknown file>";
            String  lineNo = "<unknown line number>";
            for (Node info = frame.getFirstChild(); info != null; info = info.getNextSibling()) {
                String name = info.getNodeName();
                String value = info.getTextContent().trim();
                switch (name) {
                case "ip":
                    ip = value;
                    break;
                case "fn":
                    fn = value;
                    break;
                case "dir":
                    dir = value;
                    break;
                case "file":
                    file = value;
                    break;
                case "line":
                    lineNo = value;
                    break;
                default:
                    break;
                }
            }
            answer.m_stackFrames.add(String.format("%s: %s@%s/%s: line %s",
                                                   ip,
                                                   fn,
                                                   dir,
                                                   file,
                                                   lineNo));
        }
        return answer;
    };

    private static String readExtendedWhat(Node xwhat) {
        String text = "<unknown cause>";
        String lbytes = null;
        String lblks = null;
        for (Node sib = xwhat.getFirstChild(); sib != null; sib = sib.getNextSibling()) {
            String name = sib.getNodeName();
            String value = sib.getTextContent().trim();
            switch (name) {
            case "leakedbytes":
                lbytes = "Leaked bytes: " + Integer.valueOf(value);
                break;
            case "leakedblocks":
                lblks = "Leaked blocks: " + Integer.valueOf(value);
                break;
            case "text":
                text = value;
                break;
            default:
                ;
            }
        }
        return "Error: " + text + (lbytes != null ? (", " + lbytes) : "") + ((lblks != null) ? (", " + lblks) : "");
    }

    /**
     * Given an output file from valgrind, and a list to which we want
     * to add errors, parse the file and add any errors found in the file
     * to the end of the list.  We do this this way, rather than just
     * returning a list, because this list is shared with another thread.
     * So, it has to be synchronized.
     *
     * Note that this does not delete the valgrindOutputFile.  The caller
     * is responsible for this.
     *
     * @param valgrindOutputFile
     * @param valgrindErrors
     */
    public static void processValgrindOutput(File         valgrindOutputFile,
                                             List<String> valgrindErrors) {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse (valgrindOutputFile);
            NodeList errors = doc.getElementsByTagName("error");
            for (int idx = 0; idx < errors.getLength(); idx += 1) {
                Node error = errors.item(idx);
                ValgrindError vgerr = new ValgrindError();
                for (Node sib = error.getFirstChild(); sib != null; sib = sib.getNextSibling()) {
                    String name = sib.getNodeName();
                    String value = sib.getTextContent().trim();
                    switch (name) {
                    case "kind":
                        vgerr.m_kind = value;
                        break;
                    case "what":
                        vgerr.m_what = value;
                        break;
                    case "xwhat":
                        vgerr.m_what = readExtendedWhat(sib);
                        break;
                    case "leakedbytes":
                        vgerr.m_leakedBytes = Integer.valueOf(value);
                        break;
                    case "leakedblocks":
                        vgerr.m_leakedBlocks = Integer.valueOf(value);
                        break;
                    case "stack":
                        vgerr.m_stacks.add(readStackTrace(sib));
                        break;
                    default:
                        break;
                    }
                }
                valgrindErrors.add(vgerr.toString());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return;
        }
    }
}
