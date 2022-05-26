/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package genqa;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

public class RoughCSVTokenizer {

        private RoughCSVTokenizer() {
        }

        private static void moveToBuffer(List<String> resultBuffer, StringBuilder buf) {
            resultBuffer.add(buf.toString());
            buf.delete(0, buf.length());
        }

        public static String[] tokenize(String csv) {
            List<String> resultBuffer = new java.util.ArrayList<String>();

            if (csv != null) {
                int z = csv.length();
                Character openingQuote = null;
                boolean trimSpace = false;
                StringBuilder buf = new StringBuilder();

                for (int i = 0; i < z; ++i) {
                    char c = csv.charAt(i);
                    trimSpace = trimSpace && Character.isWhitespace(c);
                    if (c == '"' || c == '\'') {
                        if (openingQuote == null) {
                            openingQuote = c;
                            int bi = 0;
                            while (bi < buf.length()) {
                                if (Character.isWhitespace(buf.charAt(bi))) {
                                    buf.deleteCharAt(bi);
                                } else {
                                    bi++;
                                }
                            }
                        }
                        else if (openingQuote == c ) {
                            openingQuote = null;
                            trimSpace = true;
                        }
                    }
                    else if (c == '\\') {
                        if ((z > i + 1)
                            && ((csv.charAt(i + 1) == '"')
                                || (csv.charAt(i + 1) == '\\'))) {
                            buf.append(csv.charAt(i + 1));
                            ++i;
                        } else {
                            buf.append("\\");
                        }
                    } else {
                        if (openingQuote != null) {
                            buf.append(c);
                        } else {
                            if (c == ',') {
                                moveToBuffer(resultBuffer, buf);
                            } else {
                                if (!trimSpace) buf.append(c);
                            }
                        }
                    }
                }
                moveToBuffer(resultBuffer, buf);
            }

            String[] result = new String[resultBuffer.size()];
            return resultBuffer.toArray(result);
        }
}
